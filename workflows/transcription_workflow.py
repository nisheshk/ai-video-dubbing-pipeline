"""Transcription workflow for AI dubbing pipeline using OpenAI Whisper API."""

import asyncio
import logging
from datetime import timedelta
from typing import List, Optional, Dict, Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from shared.models import TranscriptionRequest, ConsolidatedTranscriptionResult
    from activities.transcription import (
        load_segments_activity,
        transcribe_segment_activity,
        consolidate_transcriptions_activity
    )

logger = logging.getLogger(__name__)


@workflow.defn
class TranscriptionWorkflow:
    """Workflow for transcribing audio segments using OpenAI Whisper API."""

    @workflow.run
    async def run(self, request: TranscriptionRequest) -> ConsolidatedTranscriptionResult:
        """
        Execute transcription workflow with parallel segment processing.
        
        Args:
            request: Transcription request with video ID and configuration
            
        Returns:
            ConsolidatedTranscriptionResult with all transcriptions
        """
        logger.info(f"Starting transcription workflow for video {request.video_id}")
        
        # Define retry policy for activities  
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=5),
            maximum_attempts=3
        )
        
        try:
            # Step 1: Load segments from speech segmentation results
            logger.info(f"Loading audio segments for video {request.video_id}")
            
            segments = await workflow.execute_activity(
                load_segments_activity,
                args=[request.video_id],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
                # Uses current queue (consolidation queue) for metadata loading
            )
            
            if not segments:
                logger.error(f"No segments found for video {request.video_id}")
                return ConsolidatedTranscriptionResult(
                    video_id=request.video_id,
                    success=False,
                    total_segments=0,
                    successful_transcriptions=0,
                    primary_language="unknown",
                    full_transcript="",
                    total_words=0,
                    language_consistency_score=0.0,
                    processing_time_seconds=0.0,
                    api_requests_count=0,
                    success_rate=0.0,
                    error_message="No audio segments found for transcription"
                )
            
            logger.info(f"Found {len(segments)} segments to transcribe")
            
            # Step 2: Create parallel transcription tasks for each segment using dedicated queue
            transcription_config = {
                "model": request.model,
                "response_format": request.response_format,
                "language": request.language,
                "prompt": request.prompt,
                "temperature": request.temperature
            }
            
            # Create parallel transcription tasks using dedicated transcription queue
            transcription_tasks = []
            for i, segment in enumerate(segments):
                segment_id = segment.get('id', f'seg_{i}')
                segment_index = segment.get('segment_index', i)
                duration = segment.get('duration', 0)
                
                logger.info(f"Creating transcription task {i+1}/{len(segments)}: segment {segment_index} ({duration:.2f}s)")
                
                # Execute each transcription task on dedicated transcription queue
                task = workflow.execute_activity(
                    transcribe_segment_activity,
                    args=[request.video_id, segment, transcription_config],
                    start_to_close_timeout=timedelta(minutes=10),  # Allow time for API calls
                    retry_policy=retry_policy,
                    task_queue=request.transcription_queue  # Use transcription queue from request
                )
                transcription_tasks.append(task)
            
            logger.info(f"Created {len(transcription_tasks)} parallel transcription tasks on queue: {request.transcription_queue}")
            
            # Step 3: Wait for all transcription tasks to complete with detailed tracking
            logger.info("🔄 Starting parallel transcription processing...")
            start_time = workflow.now()
            
            # Track individual task completion
            total_tasks = len(transcription_tasks)
            logger.info(f"📊 Tracking {total_tasks} parallel transcription tasks")
            
            try:
                # Wait for all tasks with detailed logging
                logger.info("⏳ Waiting for all transcription activities to complete...")
                transcription_results = await asyncio.gather(*transcription_tasks)
                processing_time = (workflow.now() - start_time).total_seconds()
                logger.info(f"✅ All {total_tasks} transcription tasks completed in {processing_time:.2f}s")
                
                # Log individual results
                for i, result in enumerate(transcription_results):
                    status = "✅ SUCCESS" if result.success else "❌ FAILED"
                    logger.info(f"  Task {i+1}/{total_tasks}: Segment {result.segment_index} - {status}")
                    if not result.success:
                        logger.error(f"    Error: {result.error_message}")
                        
            except Exception as e:
                processing_time = (workflow.now() - start_time).total_seconds()
                logger.error(f"❌ Transcription tasks failed after {processing_time:.2f}s: {e}")
                logger.error(f"💥 Exception details: {str(e)}")
                raise
            
            successful_count = sum(1 for r in transcription_results if r.success)
            failed_count = total_tasks - successful_count
            logger.info(f"📋 Final results: {successful_count}/{total_tasks} successful, {failed_count} failed")
            
            # Step 4: Consolidate results into final output using consolidation queue
            logger.info("Consolidating transcription results...")
            
            final_result = await workflow.execute_activity(
                consolidate_transcriptions_activity,
                args=[request.video_id, transcription_results],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy,
                task_queue=request.transcription_queue  # Use same transcription queue for consolidation
            )
            
            if final_result.success:
                logger.info(f"Transcription workflow completed successfully for video {request.video_id}: "
                           f"{final_result.successful_transcriptions}/{final_result.total_segments} segments, "
                           f"{final_result.total_words} words, "
                           f"primary language: {final_result.primary_language}")
            else:
                logger.error(f"Transcription workflow failed for video {request.video_id}: "
                           f"{final_result.error_message}")
            
            return final_result
            
        except Exception as e:
            logger.error(f"Transcription workflow failed for video {request.video_id}: {e}")
            raise


# Workflow helper functions for integration with main dubbing pipeline

async def start_transcription_workflow(
    client,
    video_id: str,
    transcription_config: Optional[Dict[str, Any]] = None
) -> str:
    """
    Start transcription workflow.
    
    Args:
        client: Temporal client
        video_id: Video identifier
        transcription_config: Optional transcription configuration
        
    Returns:
        Workflow ID
    """
    from config import DubbingConfig
    config = DubbingConfig.from_env()
    
    # Default configuration
    config_params = {
        "model": "whisper-1",
        "response_format": "json",
        "language": None,
        "prompt": None,
        "temperature": 0
    }
    
    if transcription_config:
        config_params.update(transcription_config)
    
    # Create transcription request with queue information
    request = TranscriptionRequest(
        video_id=video_id,
        segments=[],  # Segments will be loaded from database
        transcription_queue=config.transcription_queue,  # Pass queue name to avoid config loading in workflow
        **config_params
    )
    
    workflow_id = f"transcription-test6-{video_id}"
    
    # Start workflow
    handle = await client.start_workflow(
        TranscriptionWorkflow.run,
        request,
        id=workflow_id,
        task_queue=config.transcription_queue  # Main workflow uses transcription queue
    )
    
    logger.info(f"Started transcription workflow: {workflow_id}")
    return workflow_id


async def get_transcription_result(client, workflow_id: str) -> ConsolidatedTranscriptionResult:
    """
    Get result from transcription workflow.
    
    Args:
        client: Temporal client
        workflow_id: Workflow identifier
        
    Returns:
        ConsolidatedTranscriptionResult
    """
    handle = client.get_workflow_handle(workflow_id)
    result = await handle.result()
    
    # Ensure result is converted to ConsolidatedTranscriptionResult if it's a dict
    if isinstance(result, dict):
        return ConsolidatedTranscriptionResult(**result)
    
    return result