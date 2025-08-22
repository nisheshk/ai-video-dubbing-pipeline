"""Audio alignment activities for precise timing synchronization with original video segments."""

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
async def load_alignment_data_activity(video_id: str) -> Dict[str, Any]:
    """
    Load segment timing and voice synthesis data required for alignment.
    
    Args:
        video_id: Video identifier
        
    Returns:
        Dict containing segments, voice synthesis data, and original video info
    """
    logger.info(f"Loading alignment data for video {video_id}")
    
    config = DubbingConfig.from_env()
    
    try:
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "load_alignment_data", 
                "processing",
                "Loading segment timing and voice synthesis data"
            )
            
            # Load segments ordered by start_time
            segments_query = """
                SELECT id, segment_index, start_time, end_time, duration, status
                FROM segments 
                WHERE video_id = $1 
                ORDER BY start_time
            """
            async with db_client.pool.acquire() as conn:
                segment_rows = await conn.fetch(segments_query, video_id)
                segments = [dict(row) for row in segment_rows]
            
            # Load voice synthesis data (latest record for each segment)
            voice_query = """
                SELECT DISTINCT ON (segment_id) 
                       id, segment_id, segment_index, gcs_audio_path, 
                       audio_duration_seconds, status
                FROM voice_synthesis 
                WHERE video_id = $1 AND status = 'completed'
                ORDER BY segment_id, created_at DESC
            """
            async with db_client.pool.acquire() as conn:
                voice_rows = await conn.fetch(voice_query, video_id)
                voice_data = [dict(row) for row in voice_rows]
            
            # Load original video info
            video_info = await db_client.get_video_info(video_id)
            
            # Validate data completeness
            if not segments:
                raise ValueError(f"No segments found for video {video_id}")
            if not voice_data:
                raise ValueError(f"No voice synthesis data found for video {video_id}")
            if len(segments) != len(voice_data):
                raise ValueError(f"Segment count mismatch: {len(segments)} segments vs {len(voice_data)} voice files")
            
            # Create alignment mapping
            alignment_data = []
            for segment in segments:
                # Find matching voice synthesis data
                voice_match = next((v for v in voice_data if str(v['segment_id']) == str(segment['id'])), None)
                if not voice_match:
                    raise ValueError(f"No voice synthesis found for segment {segment['id']}")
                
                original_duration = segment['end_time'] - segment['start_time']
                synthesized_duration = voice_match['audio_duration_seconds'] or 0.0
                
                # Handle missing or invalid audio duration by calculating it from the GCS audio file
                if synthesized_duration <= 0:
                    logger.warning(f"Invalid synthesized audio duration ({synthesized_duration}) for segment {segment['id']}, calculating from audio file")
                    try:
                        synthesized_duration = await _calculate_audio_duration_from_gcs(
                            voice_match['gcs_audio_path'], config
                        )
                        if synthesized_duration <= 0:
                            raise ValueError(f"Could not determine valid audio duration for segment {segment['id']}: calculated {synthesized_duration}")
                        logger.info(f"Calculated duration {synthesized_duration:.3f}s for segment {segment['id']} from audio file")
                    except Exception as calc_error:
                        raise ValueError(f"Invalid synthesized audio duration for segment {segment['id']}: {synthesized_duration}. Failed to calculate from file: {calc_error}")
                
                stretch_ratio = original_duration / synthesized_duration
                
                # Validate stretch ratio is within safe bounds
                if not 0.5 <= stretch_ratio <= 2.0:
                    logger.warning(f"Stretch ratio {stretch_ratio:.2f} outside recommended range for segment {segment['id']}")
                
                alignment_data.append({
                    'segment_id': str(segment['id']),
                    'voice_synthesis_id': str(voice_match['id']),
                    'segment_index': segment['segment_index'],
                    'start_time': segment['start_time'],
                    'end_time': segment['end_time'],
                    'original_duration': original_duration,
                    'synthesized_duration': synthesized_duration,
                    'stretch_ratio': stretch_ratio,
                    'gcs_audio_path': voice_match['gcs_audio_path']
                })
            
            result = {
                'video_id': video_id,
                'segments_count': len(segments),
                'alignment_data': alignment_data,
                'original_video_path': video_info['gcs_input_path'] if video_info else None,
                'original_video_duration': video_info['duration_seconds'] if video_info else None
            }
            
            await db_client.log_processing_step(
                video_id, 
                "load_alignment_data", 
                "completed",
                f"Loaded {len(alignment_data)} segments for alignment"
            )
            
            return result
            
    except Exception as e:
        logger.error(f"Failed to load alignment data: {e}")
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "load_alignment_data", 
                "failed",
                f"Error loading alignment data: {str(e)}"
            )
        raise


@activity.defn
async def align_audio_segment_activity(
    video_id: str,
    segment_data: Dict[str, Any],
    output_gcs_folder: str
) -> Dict[str, Any]:
    """
    Align a single audio segment to match original timing using FFmpeg time-stretching.
    
    Args:
        video_id: Video identifier
        segment_data: Segment alignment data
        output_gcs_folder: GCS folder for aligned audio output
        
    Returns:
        Alignment result with timing validation
    """
    logger.info(f"Aligning audio segment {segment_data['segment_id']} for video {video_id}")
    
    config = DubbingConfig.from_env()
    segment_id = segment_data['segment_id']
    stretch_ratio = segment_data['stretch_ratio']
    
    start_time = datetime.now()
    
    try:
        # Check if stretch ratio is within FFmpeg's supported range [0.5, 100.0]
        if stretch_ratio < 0.5:
            logger.warning(f"Stretch ratio {stretch_ratio:.3f} is below FFmpeg minimum (0.5). Clamping to 0.5")
            stretch_ratio = 0.5
        elif stretch_ratio > 100.0:
            logger.warning(f"Stretch ratio {stretch_ratio:.3f} is above FFmpeg maximum (100.0). Clamping to 100.0")
            stretch_ratio = 100.0
            
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                f"align_segment_{segment_data['segment_index']}", 
                "processing",
                f"Time-stretching segment {segment_id} by ratio {stretch_ratio:.3f}"
            )
            
            # Initialize GCS client
            gcs_client = AsyncGCSClient(config)
            await gcs_client.connect()
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Download synthesized audio
                input_audio_path = temp_path / f"synthesized_{segment_id}.wav"
                gcs_audio_path = segment_data['gcs_audio_path'].replace(f"gs://{config.gcs_bucket_name}/", "")
                await gcs_client.download_file(gcs_audio_path, str(input_audio_path))
                
                # Output aligned audio path
                aligned_audio_path = temp_path / f"aligned_{segment_id}.wav"
                
                # FFmpeg time-stretching command
                ffmpeg_cmd = [
                    "ffmpeg", "-y",
                    "-i", str(input_audio_path),
                    "-filter:a", f"atempo={stretch_ratio}",
                    "-acodec", "pcm_s16le",
                    "-ar", "44100",
                    str(aligned_audio_path)
                ]
                
                # Execute FFmpeg with timeout
                process = await asyncio.create_subprocess_exec(
                    *ffmpeg_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)  # 5 min timeout
                
                if process.returncode != 0:
                    raise RuntimeError(f"FFmpeg time-stretching failed: {stderr.decode()}")
                
                # Validate output audio duration
                duration_cmd = [
                    "ffprobe", "-v", "quiet",
                    "-show_entries", "format=duration",
                    "-of", "csv=p=0",
                    str(aligned_audio_path)
                ]
                
                duration_process = await asyncio.create_subprocess_exec(
                    *duration_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                duration_stdout, _ = await duration_process.communicate()
                aligned_duration = float(duration_stdout.decode().strip())
                
                # Calculate timing accuracy
                target_duration = segment_data['original_duration']
                timing_accuracy_ms = abs(aligned_duration - target_duration) * 1000
                
                logger.info(f"Segment {segment_id}: target={target_duration:.3f}s, aligned={aligned_duration:.3f}s, accuracy={timing_accuracy_ms:.1f}ms")
                
                # Upload aligned audio to GCS
                output_filename = f"aligned_segment_{segment_data['segment_index']:03d}.wav"
                output_gcs_path = f"{output_gcs_folder}/{output_filename}"
                await gcs_client.upload_file(str(aligned_audio_path), output_gcs_path)
                
                processing_time = (datetime.now() - start_time).total_seconds()
                
                # Calculate quality scores
                alignment_quality_score = max(0.0, 1.0 - (timing_accuracy_ms / 100.0))  # Degrade linearly after 100ms
                audio_distortion_score = max(0.0, 1.0 - abs(1.0 - stretch_ratio))  # Penalize extreme stretching
                
                result = {
                    'video_id': video_id,
                    'segment_id': segment_id,
                    'voice_synthesis_id': segment_data['voice_synthesis_id'],
                    'segment_index': segment_data['segment_index'],
                    'original_start_time': segment_data['start_time'],
                    'original_end_time': segment_data['end_time'],
                    'original_duration': target_duration,
                    'synthesized_duration': segment_data['synthesized_duration'],
                    'stretch_ratio': stretch_ratio,
                    'aligned_audio_gcs_path': f"gs://{config.gcs_bucket_name}/{output_gcs_path}",
                    'aligned_duration': aligned_duration,
                    'timing_accuracy_ms': timing_accuracy_ms,
                    'alignment_quality_score': alignment_quality_score,
                    'audio_distortion_score': audio_distortion_score,
                    'processing_time_seconds': processing_time,
                    'success': True,
                    'error_message': ''
                }
                
                # Store alignment result in database
                await _store_alignment_result(config, result)
                
                await db_client.log_processing_step(
                    video_id, 
                    f"align_segment_{segment_data['segment_index']}", 
                    "completed",
                    f"Aligned segment {segment_id} with {timing_accuracy_ms:.1f}ms accuracy"
                )
                
                return result
                
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds()
        error_msg = str(e)
        logger.error(f"Failed to align segment {segment_id}: {error_msg}")
        
        result = {
            'video_id': video_id,
            'segment_id': segment_id,
            'voice_synthesis_id': segment_data.get('voice_synthesis_id'),
            'segment_index': segment_data['segment_index'],
            'processing_time_seconds': processing_time,
            'success': False,
            'error_message': error_msg
        }
        
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                f"align_segment_{segment_data['segment_index']}", 
                "failed",
                f"Error aligning segment {segment_id}: {error_msg}"
            )
        
        return result


async def _store_alignment_result(config: DubbingConfig, result: Dict[str, Any]) -> None:
    """Store alignment result in database."""
    try:
        async with get_database_client(config) as db_client:
            insert_query = """
                INSERT INTO audio_alignments (
                    video_id, segment_id, voice_synthesis_id,
                    original_start_time, original_end_time, original_duration,
                    synthesized_duration, stretch_ratio,
                    aligned_audio_gcs_path, aligned_duration, timing_accuracy_ms,
                    alignment_quality_score, audio_distortion_score,
                    status, processing_time_seconds
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """
            
            async with db_client.pool.acquire() as conn:
                await conn.execute(
                    insert_query,
                    result['video_id'],
                    result['segment_id'], 
                    result['voice_synthesis_id'],
                    result['original_start_time'],
                    result['original_end_time'],
                    result['original_duration'],
                    result['synthesized_duration'],
                    result['stretch_ratio'],
                    result['aligned_audio_gcs_path'],
                    result['aligned_duration'],
                    result['timing_accuracy_ms'],
                    result['alignment_quality_score'],
                    result['audio_distortion_score'],
                    'completed',
                    result['processing_time_seconds']
                )
                
    except Exception as e:
        logger.error(f"Failed to store alignment result: {e}")
        # Don't raise - this is a logging operation


@activity.defn
async def generate_silence_activity(
    duration_seconds: float,
    output_gcs_path: str
) -> Dict[str, Any]:
    """
    Generate silence audio file for gaps between segments.
    
    Args:
        duration_seconds: Duration of silence to generate
        output_gcs_path: GCS path for output silence file
        
    Returns:
        Generation result
    """
    logger.info(f"Generating {duration_seconds:.3f}s of silence")
    
    config = DubbingConfig.from_env()
    
    try:
        if duration_seconds <= 0.01:  # Skip very short gaps
            return {
                'success': True,
                'gcs_path': None,
                'duration': 0.0,
                'message': 'Skipped short silence gap'
            }
        
        # Initialize GCS client
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            silence_path = temp_path / "silence.wav"
            
            # FFmpeg command to generate silence
            ffmpeg_cmd = [
                "ffmpeg", "-y",
                "-f", "lavfi",
                "-i", f"anullsrc=duration={duration_seconds}:sample_rate=44100:channel_layout=mono",
                "-acodec", "pcm_s16le",
                str(silence_path)
            ]
            
            process = await asyncio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
            
            if process.returncode != 0:
                raise RuntimeError(f"FFmpeg silence generation failed: {stderr.decode()}")
            
            # Upload to GCS
            gcs_path = output_gcs_path.replace(f"gs://{config.gcs_bucket_name}/", "")
            await gcs_client.upload_file(str(silence_path), gcs_path)
            
            return {
                'success': True,
                'gcs_path': output_gcs_path,
                'duration': duration_seconds,
                'message': f'Generated {duration_seconds:.3f}s silence'
            }
            
    except Exception as e:
        logger.error(f"Failed to generate silence: {e}")
        return {
            'success': False,
            'gcs_path': None,
            'duration': 0.0,
            'error_message': str(e)
        }