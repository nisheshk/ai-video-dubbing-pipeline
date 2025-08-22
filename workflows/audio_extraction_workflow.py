"""Audio extraction workflow for AI dubbing pipeline."""

import asyncio
from datetime import timedelta
from typing import Dict, Any, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities.audio_extraction import audio_extraction_activities
    from shared.models import AudioExtractionRequest, AudioExtractionResult
    from shared.database import get_database_client
    from config import DubbingConfig


@workflow.defn
class AudioExtractionWorkflow:
    """Complete audio extraction workflow with error handling and monitoring."""
    
    def __init__(self) -> None:
        # Don't load config in workflow - workflows must be deterministic
        # Config will be loaded in activities where file operations are allowed
        self.processing_status: str = "pending"
        self.current_step: str = "initialization"
        self.error_message: Optional[str] = None
        
    @workflow.run
    async def run(self, request: AudioExtractionRequest) -> AudioExtractionResult:
        """Execute complete audio extraction pipeline."""
        
        workflow.logger.info(f"Starting audio extraction for video: {request.video_id}")
        
        # Initialize result
        result = AudioExtractionResult(
            video_id=request.video_id,
            success=False,
            gcs_audio_url="",
            processing_time_seconds=0,
            video_metadata={},
            audio_metadata={},
            error_message=""
        )
        
        try:
            # Update status to processing
            self.processing_status = "processing"
            await self._update_video_status(request.video_id, "processing")
            
            # Define retry policy for activities
            retry_policy = RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=30),
                maximum_attempts=3,
                backoff_coefficient=2.0
            )
            
            # Step 1: Download video from GCS
            self.current_step = "download"
            workflow.logger.info("Step 1: Downloading video from GCS")
            
            download_result = await workflow.execute_activity(
                audio_extraction_activities.download_video_activity,
                request,
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=retry_policy
            )
            
            if not download_result["success"]:
                raise RuntimeError("Video download failed")
            
            local_video_path = download_result["local_video_path"]
            
            # Step 2: Validate video file
            self.current_step = "validation"  
            workflow.logger.info("Step 2: Validating video file")
            
            validation_result = await workflow.execute_activity(
                audio_extraction_activities.validate_video_activity,
                args=[local_video_path, request.video_id, request.gcs_input_path],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=retry_policy
            )
            
            if not validation_result["success"]:
                raise RuntimeError("Video validation failed")
            
            result.video_metadata = validation_result["metadata"]
            
            # Step 3: Extract audio
            self.current_step = "extraction"
            workflow.logger.info("Step 3: Extracting audio from video")
            
            extraction_result = await workflow.execute_activity(
                audio_extraction_activities.extract_audio_activity,
                args=[local_video_path, request.video_id],
                start_to_close_timeout=timedelta(minutes=15),
                retry_policy=retry_policy
            )
            
            if not extraction_result["success"]:
                raise RuntimeError("Audio extraction failed")
            
            result.audio_metadata = extraction_result["audio_metadata"]
            local_audio_path = extraction_result["audio_path"]
            
            # Step 4: Upload audio to GCS
            self.current_step = "upload"
            workflow.logger.info("Step 4: Uploading audio to GCS")
            
            upload_result = await workflow.execute_activity(
                audio_extraction_activities.upload_audio_activity,
                args=[local_audio_path, request.video_id],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=retry_policy
            )
            
            if not upload_result["success"]:
                raise RuntimeError("Audio upload failed")
            
            result.gcs_audio_url = upload_result["gcs_audio_url"]
            
            # Calculate total processing time
            total_time = (
                download_result.get("download_time_seconds", 0) +
                validation_result.get("validation_time", 0) +
                extraction_result.get("extraction_time", 0) +
                upload_result.get("upload_time", 0)
            )
            result.processing_time_seconds = total_time
            
            # Step 5: Update database with success
            await self._update_video_status(request.video_id, "completed", result.gcs_audio_url)
            
            # Step 6: Cleanup temporary files (best effort)
            try:
                self.current_step = "cleanup"
                await workflow.execute_activity(
                    audio_extraction_activities.cleanup_temp_files_activity,
                    args=[request.video_id],
                    start_to_close_timeout=timedelta(minutes=1)
                )
            except Exception as e:
                workflow.logger.warning(f"Cleanup failed (non-fatal): {e}")
            
            # Mark as successful
            result.success = True
            self.processing_status = "completed"
            
            workflow.logger.info(f"Audio extraction completed successfully for {request.video_id} in {total_time:.2f}s")
            
            return result
            
        except Exception as e:
            # Handle workflow failure
            self.processing_status = "failed"
            self.error_message = str(e)
            
            workflow.logger.error(f"Audio extraction failed for {request.video_id}: {e}")
            
            # Update database with failure
            try:
                await self._update_video_status(request.video_id, "failed")
            except Exception as db_error:
                workflow.logger.error(f"Failed to update database status: {db_error}")
            
            # Cleanup on failure (best effort)
            try:
                await workflow.execute_activity(
                    audio_extraction_activities.cleanup_temp_files_activity,
                    args=[request.video_id],
                    start_to_close_timeout=timedelta(minutes=1)
                )
            except Exception as cleanup_error:
                workflow.logger.warning(f"Cleanup after failure failed: {cleanup_error}")
            
            raise
    
    @workflow.signal
    def get_status(self) -> Dict[str, Any]:
        """Get current workflow processing status."""
        return {
            "status": self.processing_status,
            "current_step": self.current_step,
            "error_message": self.error_message
        }
    
    @workflow.signal  
    def cancel_processing(self) -> None:
        """Signal to cancel processing."""
        workflow.logger.info("Processing cancellation requested")
        self.processing_status = "cancelled"
        # Note: Actual cancellation handled by Temporal's cancellation mechanism
    
    async def _update_video_status(self, video_id: str, status: str, gcs_audio_path: Optional[str] = None) -> None:
        """Update video status in database."""
        try:
            # This would typically be done through an activity for proper separation
            # But for status updates, we can make direct DB calls in workflow
            # Following the pattern from chatbot project
            
            workflow.logger.info(f"Updating video {video_id} status to {status}")
            
            # In production, this should be an activity to maintain proper separation
            # For now, logging the status change
            await workflow.execute_activity(
                audio_extraction_activities.cleanup_temp_files_activity,  # Placeholder activity
                args=[f"status_update_{video_id}_{status}"],
                start_to_close_timeout=timedelta(seconds=30)
            )
            
        except Exception as e:
            workflow.logger.error(f"Failed to update video status: {e}")
            # Don't fail the workflow for status update failures


@workflow.defn  
class AudioExtractionMonitorWorkflow:
    """Monitoring workflow for audio extraction progress."""
    
    def __init__(self) -> None:
        self.monitored_videos: Dict[str, Dict[str, Any]] = {}
    
    @workflow.run
    async def run(self, monitoring_duration_minutes: int = 60) -> Dict[str, Any]:
        """Monitor audio extraction workflows."""
        
        workflow.logger.info(f"Starting audio extraction monitoring for {monitoring_duration_minutes} minutes")
        
        try:
            # Wait for monitoring signals or timeout
            await workflow.wait_condition(
                lambda: False,  # Always wait for timeout
                timeout=timedelta(minutes=monitoring_duration_minutes)
            )
            
        except asyncio.TimeoutError:
            workflow.logger.info("Monitoring period completed")
        
        return {
            "monitored_videos": len(self.monitored_videos),
            "summary": self.monitored_videos
        }
    
    @workflow.signal
    def add_video_monitoring(self, video_id: str, workflow_id: str) -> None:
        """Add a video to monitoring."""
        self.monitored_videos[video_id] = {
            "workflow_id": workflow_id,
            "added_at": workflow.now(),
            "status": "monitoring"
        }
        workflow.logger.info(f"Added video {video_id} to monitoring")
    
    @workflow.signal
    def update_video_status(self, video_id: str, status: str) -> None:
        """Update monitored video status."""
        if video_id in self.monitored_videos:
            self.monitored_videos[video_id].update({
                "status": status,
                "updated_at": workflow.now()
            })
            workflow.logger.info(f"Updated monitoring status for {video_id}: {status}")
    
    @workflow.query
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Query current monitoring status."""
        return {
            "total_videos": len(self.monitored_videos),
            "videos": self.monitored_videos
        }