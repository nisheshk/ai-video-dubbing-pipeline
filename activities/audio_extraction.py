"""Audio extraction activities for AI dubbing pipeline."""

import asyncio
import os
import logging
from typing import Dict, Any, Optional
from pathlib import Path
import time

from temporalio import activity
from google.cloud import storage
from google.cloud.exceptions import NotFound
import ffmpeg
from tenacity import retry, stop_after_attempt, wait_exponential

from shared.models import AudioExtractionRequest, AudioExtractionResult
from shared.database import get_database_client
from shared.gcs_client import AsyncGCSClient
from config import DubbingConfig

logger = logging.getLogger(__name__)


class AudioExtractionActivities:
    """Audio extraction activities for Temporal workflows."""
    
    def __init__(self) -> None:
        self.config = DubbingConfig.from_env()
        
    @activity.defn
    async def download_video_activity(self, request: AudioExtractionRequest) -> Dict[str, Any]:
        """Download video from GCS to local storage."""
        start_time = time.time()
        
        try:
            activity.logger.info(f"Downloading video: {request.gcs_input_path}")
            
            # Create temp directory
            temp_dir = Path(self.config.temp_storage_path) / request.video_id
            temp_dir.mkdir(parents=True, exist_ok=True)
            video_path = str(temp_dir / "video.mp4")
            
            # Download from GCS
            gcs_client = AsyncGCSClient(self.config)
            await gcs_client.connect()
            await gcs_client.download_file(request.gcs_input_path, video_path)
            
            # Validate download
            if not os.path.exists(video_path):
                raise RuntimeError("Video download failed - file not found")
                
            file_size = os.path.getsize(video_path)
            duration = time.time() - start_time
            
            # Log to database
            async with get_database_client(self.config) as db:
                await db.log_processing_step(
                    request.video_id, "video_download", "completed",
                    f"Downloaded {file_size / (1024*1024):.1f}MB in {duration:.2f}s",
                    execution_time_ms=int(duration * 1000)
                )
            
            return {
                "success": True,
                "local_video_path": video_path,
                "file_size_bytes": file_size,
                "download_time_seconds": duration
            }
            
        except Exception as e:
            duration = time.time() - start_time
            activity.logger.error(f"Video download failed: {e}")
            
            # Log error to database
            async with get_database_client(self.config) as db:
                await db.log_processing_step(
                    request.video_id, "video_download", "failed",
                    f"Download failed: {str(e)}",
                    error_details={"error_type": type(e).__name__, "error_message": str(e)},
                    execution_time_ms=int(duration * 1000)
                )
            
            raise
    
    @activity.defn 
    async def validate_video_activity(self, video_path: str, video_id: str, gcs_input_path: str) -> Dict[str, Any]:
        """Validate video file and extract metadata."""
        start_time = time.time()
        
        try:
            activity.logger.info(f"Validating video file: {video_path}")
            
            if not os.path.exists(video_path):
                raise FileNotFoundError(f"Video file not found: {video_path}")
            
            # Use ffprobe to get video information
            probe = await asyncio.to_thread(ffmpeg.probe, video_path)
            
            # Extract metadata
            format_info = probe.get('format', {})
            video_streams = [s for s in probe.get('streams', []) if s['codec_type'] == 'video']
            audio_streams = [s for s in probe.get('streams', []) if s['codec_type'] == 'audio']
            
            if not video_streams:
                raise ValueError("No video streams found")
            if not audio_streams:
                raise ValueError("No audio streams found")
                
            video_stream = video_streams[0]
            audio_stream = audio_streams[0]
            
            metadata = {
                'duration': float(format_info.get('duration', 0)),
                'size_bytes': int(format_info.get('size', 0)),
                'format_name': format_info.get('format_name'),
                'video_codec': video_stream.get('codec_name'),
                'video_width': int(video_stream.get('width', 0)),
                'video_height': int(video_stream.get('height', 0)),
                'audio_codec': audio_stream.get('codec_name'),
                'audio_sample_rate': int(audio_stream.get('sample_rate', 0)),
                'audio_channels': int(audio_stream.get('channels', 0)),
            }
            
            duration = time.time() - start_time
            
            # Create video record in database (if it doesn't exist)
            async with get_database_client(self.config) as db:
                try:
                    # Check if video record already exists
                    existing_video = await db.get_video_info(video_id)
                    if not existing_video:
                        # Extract original filename from gcs_input_path
                        # Example: "gs://bucket/video_id/test_video1.mp4" -> "test_video1.mp4"
                        original_filename = gcs_input_path.split("/")[-1]
                        
                        # Create video record with metadata
                        await db.create_video_record(
                            video_id=video_id,
                            original_filename=original_filename,
                            gcs_input_path=gcs_input_path,
                            file_size_bytes=metadata['size_bytes'],
                            source_language="en",  # Default, can be updated later
                            target_language=None
                        )
                        
                        # Update with duration
                        await db.update_video_status(
                            video_id=video_id,
                            status="processing",
                            duration_seconds=metadata['duration']
                        )
                        activity.logger.info(f"Created video record for {video_id}")
                    else:
                        activity.logger.info(f"Video record already exists for {video_id}")
                except Exception as db_error:
                    activity.logger.warning(f"Failed to create/update video record: {db_error}")
                    # Continue processing even if video record creation fails
                
                # Log processing step
                await db.log_processing_step(
                    video_id, "video_validation", "completed",
                    f"Video validated: {metadata['duration']:.1f}s duration",
                    execution_time_ms=int(duration * 1000)
                )
            
            activity.logger.info(f"Video validation successful: {metadata}")
            return {"success": True, "metadata": metadata, "validation_time": duration}
            
        except Exception as e:
            duration = time.time() - start_time
            activity.logger.error(f"Video validation failed: {e}")
            
            # Log error to database
            async with get_database_client(self.config) as db:
                await db.log_processing_step(
                    video_id, "video_validation", "failed",
                    f"Validation failed: {str(e)}",
                    error_details={"error_type": type(e).__name__, "error_message": str(e)},
                    execution_time_ms=int(duration * 1000)
                )
            
            raise
    
    @activity.defn
    async def extract_audio_activity(self, video_path: str, video_id: str) -> Dict[str, Any]:
        """Extract audio from video file using FFmpeg."""
        start_time = time.time()
        
        try:
            activity.logger.info(f"Extracting audio from: {video_path}")
            
            # Create audio output path
            temp_dir = Path(self.config.temp_storage_path) / video_id
            temp_dir.mkdir(parents=True, exist_ok=True)
            audio_path = str(temp_dir / "audio.wav")
            
            # Remove existing audio file if it exists
            if os.path.exists(audio_path):
                os.remove(audio_path)
            
            # Configure FFmpeg for audio extraction
            stream = ffmpeg.input(video_path)
            stream = ffmpeg.output(
                stream,
                audio_path,
                acodec='pcm_s16le',  # 16-bit PCM
                ac=self.config.audio_channels,  # Mono
                ar=self.config.audio_sample_rate,  # 16kHz sample rate
                loglevel='error'  # Reduce log verbosity
            )
            
            # Run FFmpeg asynchronously
            ffmpeg_args = ffmpeg.compile(stream, overwrite_output=True)
            process = await asyncio.create_subprocess_exec(
                *ffmpeg_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), 
                timeout=self.config.timeout_seconds
            )
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown FFmpeg error"
                raise RuntimeError(f"Audio extraction failed: {error_msg}")
            
            # Verify extraction
            if not os.path.exists(audio_path):
                raise RuntimeError("Audio extraction failed - output file not created")
            
            # Validate extracted audio
            audio_metadata = await self._validate_extracted_audio(audio_path)
            
            duration = time.time() - start_time
            
            # Log to database
            async with get_database_client(self.config) as db:
                await db.log_processing_step(
                    video_id, "audio_extraction", "completed",
                    f"Audio extracted in {duration:.2f}s",
                    execution_time_ms=int(duration * 1000)
                )
            
            result = {
                'success': True,
                'audio_path': audio_path,
                'extraction_time': duration,
                'audio_metadata': audio_metadata
            }
            
            activity.logger.info(f"Audio extraction completed in {duration:.2f}s")
            return result
            
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            activity.logger.error(f"Audio extraction timed out after {duration:.2f}s")
            
            # Clean up failed extraction
            if os.path.exists(audio_path):
                os.remove(audio_path)
            
            raise RuntimeError(f"Audio extraction timed out after {self.config.timeout_seconds}s")
            
        except Exception as e:
            duration = time.time() - start_time
            activity.logger.error(f"Audio extraction failed: {e}")
            
            # Clean up failed extraction
            audio_path = str(Path(self.config.temp_storage_path) / video_id / "audio.wav")
            if os.path.exists(audio_path):
                os.remove(audio_path)
            
            # Log error to database
            async with get_database_client(self.config) as db:
                await db.log_processing_step(
                    video_id, "audio_extraction", "failed",
                    f"Extraction failed: {str(e)}",
                    error_details={"error_type": type(e).__name__, "error_message": str(e)},
                    execution_time_ms=int(duration * 1000)
                )
            
            raise
    
    @activity.defn
    async def upload_audio_activity(self, audio_path: str, video_id: str) -> Dict[str, Any]:
        """Upload extracted audio to GCS."""
        start_time = time.time()
        
        try:
            activity.logger.info(f"Uploading audio: {audio_path}")
            
            # Upload to GCS
            gcs_client = AsyncGCSClient(self.config)
            await gcs_client.connect()
            
            gcs_audio_path = f"{video_id}/audio/extracted.wav"
            uploaded_url = await gcs_client.upload_file(audio_path, gcs_audio_path)
            
            duration = time.time() - start_time
            file_size = os.path.getsize(audio_path)
            
            # Log to database
            async with get_database_client(self.config) as db:
                await db.log_processing_step(
                    video_id, "audio_upload", "completed",
                    f"Uploaded {file_size / (1024*1024):.1f}MB in {duration:.2f}s",
                    execution_time_ms=int(duration * 1000)
                )
            
            return {
                "success": True,
                "gcs_audio_url": uploaded_url,
                "upload_time": duration,
                "file_size_bytes": file_size
            }
            
        except Exception as e:
            duration = time.time() - start_time
            activity.logger.error(f"Audio upload failed: {e}")
            
            # Log error to database
            async with get_database_client(self.config) as db:
                await db.log_processing_step(
                    video_id, "audio_upload", "failed",
                    f"Upload failed: {str(e)}",
                    error_details={"error_type": type(e).__name__, "error_message": str(e)},
                    execution_time_ms=int(duration * 1000)
                )
            
            raise
    
    @activity.defn
    async def cleanup_temp_files_activity(self, video_id: str) -> Dict[str, Any]:
        """Clean up temporary files for a video processing session."""
        try:
            temp_dir = Path(self.config.temp_storage_path) / video_id
            
            if temp_dir.exists():
                # Remove all files in the directory
                for file_path in temp_dir.rglob("*"):
                    if file_path.is_file():
                        file_path.unlink()
                        activity.logger.debug(f"Cleaned up: {file_path}")
                
                # Remove the directory
                temp_dir.rmdir()
                activity.logger.info(f"Cleaned up temp directory: {temp_dir}")
            
            return {"success": True, "message": f"Cleanup completed for {video_id}"}
            
        except Exception as e:
            activity.logger.warning(f"Cleanup failed for {video_id}: {e}")
            return {"success": False, "error": str(e)}
    
    async def _validate_extracted_audio(self, audio_path: str) -> Dict[str, Any]:
        """Validate extracted audio file meets requirements."""
        try:
            probe = await asyncio.to_thread(ffmpeg.probe, audio_path)
            
            format_info = probe.get('format', {})
            audio_streams = [s for s in probe.get('streams', []) if s['codec_type'] == 'audio']
            
            if not audio_streams:
                raise ValueError("No audio stream found in extracted file")
            
            audio_stream = audio_streams[0]
            
            metadata = {
                'duration': float(format_info.get('duration', 0)),
                'size_bytes': int(format_info.get('size', 0)),
                'codec': audio_stream.get('codec_name'),
                'sample_rate': int(audio_stream.get('sample_rate', 0)),
                'channels': int(audio_stream.get('channels', 0)),
                'bitrate': int(audio_stream.get('bit_rate', 0)),
            }
            
            # Validate audio parameters
            if metadata['sample_rate'] != self.config.audio_sample_rate:
                activity.logger.warning(f"Sample rate mismatch: expected {self.config.audio_sample_rate}, got {metadata['sample_rate']}")
            
            if metadata['channels'] != self.config.audio_channels:
                activity.logger.warning(f"Channel count mismatch: expected {self.config.audio_channels}, got {metadata['channels']}")
            
            if metadata['duration'] < 1.0:
                raise ValueError("Extracted audio too short (< 1 second)")
            
            return metadata
            
        except Exception as e:
            activity.logger.error(f"Error validating extracted audio: {e}")
            raise


# Activity instances for worker registration
audio_extraction_activities = AudioExtractionActivities()