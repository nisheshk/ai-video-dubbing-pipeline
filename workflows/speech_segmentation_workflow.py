"""Speech segmentation workflow for AI dubbing pipeline."""

import logging
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from shared.models import SpeechSegmentationRequest, SpeechSegmentationResult
    from activities import speech_segmentation

logger = logging.getLogger(__name__)


@workflow.defn
class SpeechSegmentationWorkflow:
    """Workflow for speech segmentation using Voice Activity Detection (VAD)."""

    @workflow.run
    async def run(self, request: SpeechSegmentationRequest) -> SpeechSegmentationResult:
        """
        Execute speech segmentation workflow.
        
        Args:
            request: Speech segmentation request
            
        Returns:
            SpeechSegmentationResult with segment data
        """
        logger.info(f"Starting speech segmentation workflow for video {request.video_id}")
        
        # Define retry policy for activities
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=2),
            maximum_attempts=3,
            non_retryable_error_types=["ValueError", "FileNotFoundError"]
        )
        
        try:
            # Execute speech segmentation activity
            result = await workflow.execute_activity(
                speech_segmentation.speech_segmentation_activity,
                args=[request],
                start_to_close_timeout=timedelta(minutes=30),  # Allow longer for VAD processing
                retry_policy=retry_policy
            )
            
            if result.success:
                logger.info(f"Speech segmentation completed successfully for video {request.video_id}: "
                           f"{result.total_segments} segments created")
            else:
                logger.error(f"Speech segmentation failed for video {request.video_id}: "
                           f"{result.error_message}")
            
            return result
            
        except Exception as e:
            logger.error(f"Speech segmentation workflow failed for video {request.video_id}: {e}")
            
            # Return failed result
            return SpeechSegmentationResult(
                video_id=request.video_id,
                success=False,
                total_segments=0,
                total_speech_duration=0.0,
                processing_time_seconds=0.0,
                speech_to_silence_ratio=0.0,
                avg_segment_duration=0.0,
                error_message=str(e)
            )


# Workflow helper functions for integration with main dubbing pipeline

async def start_speech_segmentation_workflow(
    client,
    video_id: str,
    gcs_audio_path: str,
    vad_config: Optional[dict] = None
) -> str:
    """
    Start speech segmentation workflow.
    
    Args:
        client: Temporal client
        video_id: Video identifier
        gcs_audio_path: GCS path to extracted audio file
        vad_config: Optional VAD configuration parameters
        
    Returns:
        Workflow ID
    """
    from config import DubbingConfig
    config = DubbingConfig.from_env()
    
    # Create segmentation request
    request = SpeechSegmentationRequest(
        video_id=video_id,
        gcs_audio_path=gcs_audio_path,
        **(vad_config or {})
    )
    
    workflow_id = f"speech-segmentation-{video_id}"
    
    # Start workflow
    handle = await client.start_workflow(
        SpeechSegmentationWorkflow.run,
        request,
        id=workflow_id,
        task_queue=config.task_queue or "dubbing-prod-queue"
    )
    
    logger.info(f"Started speech segmentation workflow: {workflow_id}")
    return workflow_id


async def get_speech_segmentation_result(client, workflow_id: str) -> SpeechSegmentationResult:
    """
    Get result from speech segmentation workflow.
    
    Args:
        client: Temporal client
        workflow_id: Workflow identifier
        
    Returns:
        SpeechSegmentationResult
    """
    handle = client.get_workflow_handle(workflow_id)
    result = await handle.result()
    
    # Ensure result is converted to SpeechSegmentationResult if it's a dict
    if isinstance(result, dict):
        return SpeechSegmentationResult(**result)
    
    return result