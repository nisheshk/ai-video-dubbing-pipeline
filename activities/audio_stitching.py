"""Audio stitching and video muxing activities for final dubbed video assembly."""

import os
import json
import logging
import asyncio
import tempfile
import subprocess
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from pathlib import Path

from temporalio import activity

from shared.models import ProcessingStatus
from shared.database import get_database_client
from shared.gcs_client import AsyncGCSClient
from config import DubbingConfig

logger = logging.getLogger(__name__)


@activity.defn
async def stitch_aligned_audio_activity(
    video_id: str,
    output_gcs_folder: str
) -> Dict[str, Any]:
    """
    Stitch aligned audio segments and silence gaps into continuous dubbed audio track.
    
    Args:
        video_id: Video identifier
        output_gcs_folder: GCS folder for final audio output
        
    Returns:
        Stitching result with final audio path and timing validation
    """
    logger.info(f"Stitching aligned audio segments for video {video_id}")
    
    config = DubbingConfig.from_env()
    start_time = datetime.now()
    
    try:
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "stitch_aligned_audio", 
                "processing",
                "Concatenating aligned audio segments"
            )
            
            # Load aligned audio segments
            alignment_query = """
                SELECT aa.segment_id, s.segment_index, aa.original_start_time, aa.original_end_time,
                       aa.aligned_audio_gcs_path, aa.aligned_duration, aa.timing_accuracy_ms,
                       s.start_time, s.end_time
                FROM audio_alignments aa
                JOIN segments s ON aa.segment_id = s.id
                WHERE aa.video_id = $1 AND aa.status = 'completed'
                ORDER BY aa.original_start_time
            """
            
            async with db_client.pool.acquire() as conn:
                alignment_rows = await conn.fetch(alignment_query, video_id)
                alignments = [dict(row) for row in alignment_rows]
            
            if not alignments:
                raise ValueError(f"No aligned audio segments found for video {video_id}")
            
            # Initialize GCS client
            gcs_client = AsyncGCSClient(config)
            await gcs_client.connect()
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Download all aligned audio segments
                audio_files = []
                total_audio_duration = 0.0
                
                for i, alignment in enumerate(alignments):
                    # Download aligned segment
                    segment_filename = f"segment_{alignment['segment_index']:03d}.wav"
                    segment_path = temp_path / segment_filename
                    
                    gcs_path = alignment['aligned_audio_gcs_path'].replace(f"gs://{config.gcs_bucket_name}/", "")
                    await gcs_client.download_file(gcs_path, str(segment_path))
                    
                    audio_files.append({
                        'type': 'segment',
                        'path': str(segment_path),
                        'start_time': alignment['original_start_time'],
                        'end_time': alignment['original_end_time'],
                        'duration': alignment['aligned_duration']
                    })
                    
                    total_audio_duration += alignment['aligned_duration']
                    
                    # Generate silence gap if not the last segment
                    if i < len(alignments) - 1:
                        current_end = alignment['original_end_time']
                        next_start = alignments[i + 1]['original_start_time']
                        gap_duration = next_start - current_end
                        
                        if gap_duration > 0.01:  # Only add silence for gaps > 10ms
                            silence_filename = f"silence_{i:03d}.wav"
                            silence_path = temp_path / silence_filename
                            
                            # Generate silence with FFmpeg
                            silence_cmd = [
                                "ffmpeg", "-y",
                                "-f", "lavfi",
                                "-i", f"anullsrc=duration={gap_duration}:sample_rate=44100:channel_layout=mono",
                                "-acodec", "pcm_s16le",
                                str(silence_path)
                            ]
                            
                            silence_process = await asyncio.create_subprocess_exec(
                                *silence_cmd,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            
                            await silence_process.communicate()
                            
                            if silence_process.returncode == 0:
                                audio_files.append({
                                    'type': 'silence',
                                    'path': str(silence_path),
                                    'start_time': current_end,
                                    'end_time': next_start,
                                    'duration': gap_duration
                                })
                                total_audio_duration += gap_duration
                
                # Create FFmpeg concat file
                concat_file_path = temp_path / "concat_list.txt"
                with open(concat_file_path, 'w') as f:
                    for audio_file in audio_files:
                        f.write(f"file '{audio_file['path']}'\n")
                
                # Concatenate all audio files
                final_audio_path = temp_path / "final_dubbed_audio.wav"
                concat_cmd = [
                    "ffmpeg", "-y",
                    "-f", "concat", "-safe", "0",
                    "-i", str(concat_file_path),
                    "-acodec", "pcm_s16le",
                    "-ar", "44100",
                    str(final_audio_path)
                ]
                
                concat_process = await asyncio.create_subprocess_exec(
                    *concat_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await asyncio.wait_for(concat_process.communicate(), timeout=600)  # 10 min timeout
                
                if concat_process.returncode != 0:
                    raise RuntimeError(f"FFmpeg concatenation failed: {stderr.decode()}")
                
                # Validate final audio duration
                duration_cmd = [
                    "ffprobe", "-v", "quiet",
                    "-show_entries", "format=duration",
                    "-of", "csv=p=0",
                    str(final_audio_path)
                ]
                
                duration_process = await asyncio.create_subprocess_exec(
                    *duration_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                duration_stdout, _ = await duration_process.communicate()
                final_audio_duration = float(duration_stdout.decode().strip())
                
                # Upload final audio to GCS
                output_filename = f"final_dubbed_audio_{video_id}.wav"
                output_gcs_path = f"{output_gcs_folder.strip('/')}/{output_filename}"
                await gcs_client.upload_file(str(final_audio_path), output_gcs_path)
                
                processing_time = (datetime.now() - start_time).total_seconds()
                
                result = {
                    'video_id': video_id,
                    'final_audio_gcs_path': f"gs://{config.gcs_bucket_name}/{output_gcs_path}",
                    'segments_processed': len(alignments),
                    'total_audio_duration': final_audio_duration,
                    'calculated_duration': total_audio_duration,
                    'duration_accuracy_ms': abs((final_audio_duration or 0.0) - (total_audio_duration or 0.0)) * 1000,
                    'processing_time_seconds': processing_time,
                    'success': True,
                    'error_message': ''
                }
                
                await db_client.log_processing_step(
                    video_id, 
                    "stitch_aligned_audio", 
                    "completed",
                    f"Stitched {len(alignments)} segments into {final_audio_duration:.2f}s audio track"
                )
                
                return result
                
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds()
        error_msg = str(e)
        logger.error(f"Failed to stitch aligned audio: {error_msg}")
        
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "stitch_aligned_audio", 
                "failed",
                f"Error stitching audio: {error_msg}"
            )
        
        return {
            'video_id': video_id,
            'processing_time_seconds': processing_time,
            'success': False,
            'error_message': error_msg
        }


@activity.defn
async def mux_video_with_dubbed_audio_activity(
    video_id: str,
    final_audio_gcs_path: str,
    output_gcs_folder: str,
    create_multitrack: bool = True
) -> Dict[str, Any]:
    """
    Mux original video with dubbed audio to create final dubbed video.
    
    Args:
        video_id: Video identifier
        final_audio_gcs_path: GCS path to final dubbed audio
        output_gcs_folder: GCS folder for final video output
        create_multitrack: Whether to create version with both audio tracks
        
    Returns:
        Muxing result with final video paths and validation
    """
    logger.info(f"Muxing video {video_id} with dubbed audio")
    
    config = DubbingConfig.from_env()
    start_time = datetime.now()
    
    try:
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "mux_video_audio", 
                "processing",
                "Combining original video with dubbed audio"
            )
            
            # Get original video info
            video_info = await db_client.get_video_info(video_id)
            if not video_info:
                raise ValueError(f"Video info not found for {video_id}")
            
            original_video_gcs_path = video_info['gcs_input_path']
            original_duration = video_info.get('duration_seconds') or 0.0
            
            # Initialize GCS client
            gcs_client = AsyncGCSClient(config)
            await gcs_client.connect()
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Download original video
                original_video_path = temp_path / "original_video.mp4"
                video_gcs_path = original_video_gcs_path.replace(f"gs://{config.gcs_bucket_name}/", "")
                await gcs_client.download_file(video_gcs_path, str(original_video_path))
                
                # Download dubbed audio
                dubbed_audio_path = temp_path / "dubbed_audio.wav"
                audio_gcs_path = final_audio_gcs_path.replace(f"gs://{config.gcs_bucket_name}/", "")
                await gcs_client.download_file(audio_gcs_path, str(dubbed_audio_path))
                
                # Create final dubbed video (replace original audio)
                final_video_path = temp_path / f"final_dubbed_video_{video_id}.mp4"
                mux_cmd = [
                    "ffmpeg", "-y",
                    "-i", str(original_video_path),
                    "-i", str(dubbed_audio_path),
                    "-c:v", "copy",  # Copy video stream without re-encoding
                    "-c:a", "aac", "-b:a", "192k",  # Encode audio to AAC
                    "-map", "0:v:0",  # Use video from first input
                    "-map", "1:a:0",  # Use audio from second input
                    "-shortest",  # Match shortest stream
                    str(final_video_path)
                ]
                
                mux_process = await asyncio.create_subprocess_exec(
                    *mux_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await asyncio.wait_for(mux_process.communicate(), timeout=1800)  # 30 min timeout
                
                if mux_process.returncode != 0:
                    raise RuntimeError(f"FFmpeg video muxing failed: {stderr.decode()}")
                
                # Validate final video duration
                duration_cmd = [
                    "ffprobe", "-v", "quiet",
                    "-show_entries", "format=duration",
                    "-of", "csv=p=0",
                    str(final_video_path)
                ]
                
                duration_process = await asyncio.create_subprocess_exec(
                    *duration_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                duration_stdout, _ = await duration_process.communicate()
                final_video_duration = float(duration_stdout.decode().strip())
                
                # Upload final video
                final_output_filename = f"final_dubbed_video_{video_id}.mp4"
                final_output_gcs_path = f"{output_gcs_folder.strip('/')}/{final_output_filename}"
                await gcs_client.upload_file(str(final_video_path), final_output_gcs_path)
                
                result = {
                    'video_id': video_id,
                    'final_video_gcs_path': f"gs://{config.gcs_bucket_name}/{final_output_gcs_path}",
                    'original_video_duration': original_duration,
                    'dubbed_video_duration': final_video_duration,
                    'duration_accuracy_ms': abs((final_video_duration or 0.0) - (original_duration or 0.0)) * 1000,
                    'multitrack_video_gcs_path': None
                }
                
                # Create multitrack version if requested
                if create_multitrack:
                    multitrack_video_path = temp_path / f"multitrack_video_{video_id}.mp4"
                    multitrack_cmd = [
                        "ffmpeg", "-y",
                        "-i", str(original_video_path),
                        "-i", str(dubbed_audio_path),
                        "-c:v", "copy",
                        "-c:a", "aac", "-b:a", "192k",
                        "-map", "0:v:0",  # Video from original
                        "-map", "0:a:0",  # Original audio
                        "-map", "1:a:0",  # Dubbed audio
                        "-metadata:s:a:0", "title=Original",
                        "-metadata:s:a:1", "title=Dubbed",
                        "-disposition:a:0", "default",  # Original audio as default
                        str(multitrack_video_path)
                    ]
                    
                    multitrack_process = await asyncio.create_subprocess_exec(
                        *multitrack_cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    
                    multi_stdout, multi_stderr = await asyncio.wait_for(multitrack_process.communicate(), timeout=1800)
                    
                    if multitrack_process.returncode == 0:
                        # Upload multitrack version
                        multitrack_filename = f"multitrack_video_{video_id}.mp4"
                        multitrack_gcs_path = f"{output_gcs_folder.strip('/')}/{multitrack_filename}"
                        await gcs_client.upload_file(str(multitrack_video_path), multitrack_gcs_path)
                        
                        result['multitrack_video_gcs_path'] = f"gs://{config.gcs_bucket_name}/{multitrack_gcs_path}"
                        logger.info(f"Created multitrack video for {video_id}")
                    else:
                        logger.warning(f"Multitrack video creation failed: {multi_stderr.decode()}")
                
                processing_time = (datetime.now() - start_time).total_seconds()
                result.update({
                    'processing_time_seconds': processing_time,
                    'success': True,
                    'error_message': ''
                })
                
                # Store result in database
                await _store_dubbed_video_result(config, result)
                
                await db_client.log_processing_step(
                    video_id, 
                    "mux_video_audio", 
                    "completed",
                    f"Created final dubbed video with {result['duration_accuracy_ms']:.1f}ms timing accuracy"
                )
                
                return result
                
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds()
        error_msg = str(e)
        logger.error(f"Failed to mux video with dubbed audio: {error_msg}")
        
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "mux_video_audio", 
                "failed",
                f"Error muxing video: {error_msg}"
            )
        
        return {
            'video_id': video_id,
            'processing_time_seconds': processing_time,
            'success': False,
            'error_message': error_msg
        }


async def _store_dubbed_video_result(config: DubbingConfig, result: Dict[str, Any]) -> None:
    """Store dubbed video result in database."""
    try:
        async with get_database_client(config) as db_client:
            insert_query = """
                INSERT INTO dubbed_videos (
                    video_id, final_audio_gcs_path, final_video_gcs_path, multitrack_video_gcs_path,
                    original_video_duration, dubbed_video_duration, duration_accuracy_ms,
                    processing_time_seconds, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """
            
            async with db_client.pool.acquire() as conn:
                await conn.execute(
                    insert_query,
                    result['video_id'],
                    result.get('final_audio_gcs_path', ''),
                    result['final_video_gcs_path'],
                    result.get('multitrack_video_gcs_path'),
                    result['original_video_duration'],
                    result['dubbed_video_duration'],
                    result['duration_accuracy_ms'],
                    result['processing_time_seconds'],
                    'completed'
                )
                
    except Exception as e:
        logger.error(f"Failed to store dubbed video result: {e}")


@activity.defn
async def validate_final_video_activity(
    video_id: str,
    final_video_gcs_path: str
) -> Dict[str, Any]:
    """
    Validate final dubbed video for quality and integrity.
    
    Args:
        video_id: Video identifier
        final_video_gcs_path: GCS path to final dubbed video
        
    Returns:
        Validation result with quality metrics
    """
    logger.info(f"Validating final dubbed video {video_id}")
    
    config = DubbingConfig.from_env()
    
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket_name = config.gcs_bucket
        bucket = storage_client.bucket(bucket_name)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Download final video for validation
            final_video_path = temp_path / "final_video.mp4"
            gcs_path = final_video_gcs_path.replace(f"gs://{bucket_name}/", "")
            video_blob = bucket.blob(gcs_path)
            video_blob.download_to_filename(str(final_video_path))
            
            # Get video metadata with ffprobe
            metadata_cmd = [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format", "-show_streams",
                str(final_video_path)
            ]
            
            metadata_process = await asyncio.create_subprocess_exec(
                *metadata_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            metadata_stdout, _ = await metadata_process.communicate()
            metadata = json.loads(metadata_stdout.decode())
            
            # Extract validation metrics
            format_info = metadata.get('format', {})
            streams = metadata.get('streams', [])
            
            video_stream = next((s for s in streams if s['codec_type'] == 'video'), None)
            audio_stream = next((s for s in streams if s['codec_type'] == 'audio'), None)
            
            validation_result = {
                'video_id': video_id,
                'file_size_bytes': int(format_info.get('size', 0)),
                'duration': float(format_info.get('duration', 0.0)),
                'has_video': video_stream is not None,
                'has_audio': audio_stream is not None,
                'video_codec': video_stream['codec_name'] if video_stream else None,
                'audio_codec': audio_stream['codec_name'] if audio_stream else None,
                'video_resolution': f"{video_stream['width']}x{video_stream['height']}" if video_stream else None,
                'audio_sample_rate': int(audio_stream['sample_rate']) if audio_stream else None,
                'is_playable': True,  # If ffprobe succeeded, file is likely playable
                'validation_passed': True,
                'issues': []
            }
            
            # Check for potential issues
            issues = []
            if not validation_result['has_video']:
                issues.append("No video stream found")
            if not validation_result['has_audio']:
                issues.append("No audio stream found")
            if validation_result['duration'] < 1.0:
                issues.append("Video duration too short")
            if validation_result['file_size_bytes'] < 1000:
                issues.append("File size suspiciously small")
            
            validation_result['issues'] = issues
            validation_result['validation_passed'] = len(issues) == 0
            
            return validation_result
            
    except Exception as e:
        logger.error(f"Video validation failed: {e}")
        return {
            'video_id': video_id,
            'validation_passed': False,
            'error_message': str(e),
            'issues': [f"Validation error: {str(e)}"]
        }