"""Complete AI dubbing pipeline workflow orchestrating all processing steps."""

import logging
from datetime import timedelta
from typing import Dict, Any, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from workflows.audio_extraction_workflow import AudioExtractionWorkflow
    from workflows.speech_segmentation_workflow import SpeechSegmentationWorkflow
    from shared.models import (
        AudioExtractionRequest, AudioExtractionResult,
        SpeechSegmentationRequest, SpeechSegmentationResult,
        DubbingPipelineRequest, DubbingPipelineResult
    )

logger = logging.getLogger(__name__)


@workflow.defn
class DubbingPipelineWorkflow:
    """Complete AI dubbing pipeline workflow coordinating all processing stages."""

    def __init__(self) -> None:
        self.processing_status: str = "pending"
        self.current_step: str = "initialization"
        self.error_message: Optional[str] = None
        self.completed_steps: Dict[str, bool] = {}

    @workflow.run
    async def run(self, request: DubbingPipelineRequest) -> DubbingPipelineResult:
        """
        Execute complete AI dubbing pipeline.
        
        Args:
            request: Complete dubbing pipeline request
            
        Returns:
            DubbingPipelineResult with all processing results
        """
        start_time = workflow.now()
        workflow.logger.info(f"Starting complete dubbing pipeline for video {request.video_id}")
        
        # Initialize result
        result = DubbingPipelineResult(
            video_id=request.video_id,
            success=False,
            final_video_url="",
            processing_time_seconds=0.0,
            error_message=""
        )
        
        try:
            self.processing_status = "processing"
            
            # Define retry policy
            retry_policy = RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(minutes=2),
                maximum_attempts=3,
                backoff_coefficient=2.0
            )
            
            # Step 1: Audio Extraction
            self.current_step = "audio_extraction"
            workflow.logger.info("Pipeline Step 1: Audio Extraction")
            
            audio_extraction_request = AudioExtractionRequest(
                video_id=request.video_id,
                gcs_input_path=request.gcs_input_path,
                original_filename=request.original_filename,
                source_language=request.source_language,
                target_language=request.target_language
            )
            
            audio_result = await workflow.execute_child_workflow(
                AudioExtractionWorkflow.run,
                audio_extraction_request,
                id=f"audio-extraction-{request.video_id}",
                task_queue="dubbing-pipeline",
                execution_timeout=timedelta(minutes=30),
                retry_policy=retry_policy
            )
            
            if not audio_result.success:
                raise RuntimeError(f"Audio extraction failed: {audio_result.error_message}")
            
            self.completed_steps["audio_extraction"] = True
            result.audio_extraction = audio_result
            workflow.logger.info(f"Audio extraction completed: {audio_result.gcs_audio_url}")
            
            # Step 2: Speech Segmentation
            self.current_step = "speech_segmentation" 
            workflow.logger.info("Pipeline Step 2: Speech Segmentation (VAD)")
            
            segmentation_request = SpeechSegmentationRequest(
                video_id=request.video_id,
                gcs_audio_path=audio_result.gcs_audio_url,
                # Use default VAD configuration, could be customized based on request
                vad_threshold=0.5,
                min_speech_duration_ms=1000,
                max_segment_duration_s=30,
                min_silence_gap_ms=500,
                speech_padding_ms=50
            )
            
            segmentation_result = await workflow.execute_child_workflow(
                SpeechSegmentationWorkflow.run,
                segmentation_request,
                id=f"speech-segmentation-{request.video_id}",
                task_queue="dubbing-pipeline",
                execution_timeout=timedelta(minutes=45),
                retry_policy=retry_policy
            )
            
            if not segmentation_result.success:
                raise RuntimeError(f"Speech segmentation failed: {segmentation_result.error_message}")
                
            self.completed_steps["speech_segmentation"] = True
            workflow.logger.info(f"Speech segmentation completed: {segmentation_result.total_segments} segments")
            
            # TODO: Add remaining pipeline steps
            # Step 3: Transcription (ASR)
            # Step 4: Translation (MT)  
            # Step 5: Voice Cloning
            # Step 6: Text-to-Speech (TTS)
            # Step 7: Alignment & Pacing
            # Step 8: Stitch & Mix
            # Step 9: Mux (Multiplexing)
            # Step 10: Subtitles
            
            # For now, mark as completed with current steps
            processing_time = (workflow.now() - start_time).total_seconds()
            result.processing_time_seconds = processing_time
            result.success = True
            result.final_video_url = request.gcs_input_path  # Placeholder until full pipeline
            
            self.processing_status = "completed"
            
            workflow.logger.info(f"Dubbing pipeline completed successfully for video {request.video_id} "
                               f"in {processing_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            self.processing_status = "failed"
            self.error_message = str(e)
            
            processing_time = (workflow.now() - start_time).total_seconds()
            result.processing_time_seconds = processing_time
            result.error_message = str(e)
            
            workflow.logger.error(f"Dubbing pipeline failed for video {request.video_id}: {e}")
            
            return result

    @workflow.query
    def get_status(self) -> Dict[str, Any]:
        """Get current workflow processing status."""
        return {
            "status": self.processing_status,
            "current_step": self.current_step,
            "completed_steps": self.completed_steps,
            "error_message": self.error_message
        }

    @workflow.signal
    def cancel_processing(self) -> None:
        """Signal to cancel processing."""
        workflow.logger.info("Pipeline processing cancellation requested")
        self.processing_status = "cancelled"


# Helper functions for starting workflows

async def start_dubbing_pipeline(
    client,
    video_id: str,
    gcs_input_path: str,
    source_language: str,
    target_language: str,
    original_filename: Optional[str] = None,
    **kwargs
) -> str:
    """
    Start complete dubbing pipeline workflow.
    
    Args:
        client: Temporal client
        video_id: Video identifier
        gcs_input_path: GCS path to input video
        source_language: Source language code
        target_language: Target language code
        original_filename: Original video filename
        **kwargs: Additional pipeline options
        
    Returns:
        Workflow ID
    """
    request = DubbingPipelineRequest(
        video_id=video_id,
        gcs_input_path=gcs_input_path,
        source_language=source_language,
        target_language=target_language,
        original_filename=original_filename,
        **kwargs
    )
    
    workflow_id = f"dubbing-pipeline-{video_id}"
    
    handle = await client.start_workflow(
        DubbingPipelineWorkflow.run,
        request,
        id=workflow_id,
        task_queue="dubbing-pipeline"
    )
    
    logger.info(f"Started dubbing pipeline workflow: {workflow_id}")
    return workflow_id


async def get_dubbing_pipeline_status(client, workflow_id: str) -> Dict[str, Any]:
    """
    Query dubbing pipeline workflow status.
    
    Args:
        client: Temporal client  
        workflow_id: Workflow identifier
        
    Returns:
        Status dictionary
    """
    handle = client.get_workflow_handle(workflow_id)
    return await handle.query("get_status")


async def get_dubbing_pipeline_result(client, workflow_id: str) -> DubbingPipelineResult:
    """
    Get result from dubbing pipeline workflow.
    
    Args:
        client: Temporal client
        workflow_id: Workflow identifier
        
    Returns:
        DubbingPipelineResult
    """
    handle = client.get_workflow_handle(workflow_id)
    return await handle.result()