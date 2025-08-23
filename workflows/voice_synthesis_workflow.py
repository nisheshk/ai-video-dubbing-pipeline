"""Voice synthesis workflow for AI dubbing pipeline using Replicate Speech-02-HD."""

import logging
import asyncio
from datetime import timedelta
from typing import List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from shared.models import VoiceSynthesisRequest, ConsolidatedVoiceSynthesisResult, SegmentVoiceSynthesisResult
    from activities import voice_synthesis

logger = logging.getLogger(__name__)


@workflow.defn
class VoiceSynthesisWorkflow:
    """Workflow for synthesizing voice from translated text using Replicate Speech-02-HD."""

    @workflow.run
    async def run(self, request: VoiceSynthesisRequest) -> ConsolidatedVoiceSynthesisResult:
        """
        Execute voice synthesis workflow.
        
        Args:
            request: Voice synthesis request with video ID and voice configuration
            
        Returns:
            ConsolidatedVoiceSynthesisResult with audio generation results
        """
        logger.info(f"Starting voice synthesis workflow for video {request.video_id}")
        
        # Define retry policy for activities
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=3),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=10),
            maximum_attempts=3,
            non_retryable_error_types=["ValueError", "KeyError"]
        )
        
        try:
            # Step 1: Load OpenAI refined translations from database
            logger.info(f"Loading translations for TTS generation: video {request.video_id}, language {request.target_language}")
            
            translations = await workflow.execute_activity(
                voice_synthesis.load_translations_for_tts_activity,
                args=[request.video_id, request.target_language],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
            )
            
            if not translations:
                logger.warning(f"No translations found for voice synthesis: video {request.video_id}, language {request.target_language}")
                return ConsolidatedVoiceSynthesisResult(
                    video_id=request.video_id,
                    target_language=request.target_language,
                    voice_id=request.voice_id,
                    emotion=request.emotion,
                    total_segments=0,
                    successful_synthesis=0,
                    success_rate=0.0,
                    voice_segments=[],
                    total_audio_duration=0.0,
                    avg_audio_quality=0.0,
                    voice_consistency_score=0.0,
                    total_processing_time_seconds=0.0,
                    avg_processing_time_per_segment=0.0,
                    gcs_audio_folder="",
                    audio_manifest_path="",
                    estimated_cost=0.0,
                    character_count=0,
                    success=False,
                    error_message="No translations found for voice synthesis"
                )
            
            logger.info(f"Loaded {len(translations)} translations for voice synthesis")
            
            # Step 2: Parallel voice synthesis for each segment
            logger.info(f"Starting parallel voice synthesis for {len(translations)} segments")
            
            synthesis_tasks = []
            for translation in translations:
                task = workflow.execute_activity(
                    voice_synthesis.synthesize_voice_segment_activity,
                    args=[translation, request],
                    start_to_close_timeout=timedelta(minutes=15),  # Allow longer for TTS generation
                    retry_policy=retry_policy,
                    task_queue=request.voice_synthesis_queue
                )
                synthesis_tasks.append(task)
            
            # Wait for all synthesis tasks to complete
            synthesis_results = await asyncio.gather(*synthesis_tasks, return_exceptions=False)
            
            # Filter out failed results and log them
            successful_synthesis = []
            failed_synthesis = []
            
            for result in synthesis_results:
                if isinstance(result, SegmentVoiceSynthesisResult):
                    if result.success:
                        successful_synthesis.append(result)
                    else:
                        failed_synthesis.append(result)
                        logger.warning(f"Voice synthesis failed for segment {result.segment_index}: {result.error_message}")
                else:
                    logger.error(f"Unexpected synthesis result type: {type(result)}")
                    failed_synthesis.append(result)
            
            logger.info(f"Voice synthesis completed: {len(successful_synthesis)} successful, {len(failed_synthesis)} failed")
            
            # Step 3: Download and store audio files in parallel (only for successful synthesis)
            if successful_synthesis:
                logger.info(f"Starting parallel audio download and storage for {len(successful_synthesis)} segments")
                
                storage_tasks = []
                for result in successful_synthesis:
                    if result.replicate_audio_url:
                        task = workflow.execute_activity(
                            voice_synthesis.download_and_store_audio_activity,
                            args=[result, request.video_id],
                            start_to_close_timeout=timedelta(minutes=10),
                            retry_policy=retry_policy,
                            task_queue=request.voice_synthesis_queue
                        )
                        storage_tasks.append((result, task))
                
                if storage_tasks:
                    # Wait for all storage tasks to complete
                    storage_results = await asyncio.gather(
                        *[task for _, task in storage_tasks], 
                        return_exceptions=False
                    )
                    
                    # Update synthesis results with GCS paths and audio durations
                    for (synthesis_result, _), storage_result in zip(storage_tasks, storage_results):
                        if isinstance(storage_result, dict) and storage_result.get('success'):
                            synthesis_result.gcs_audio_path = storage_result['gcs_audio_path']
                            synthesis_result.audio_duration_seconds = storage_result.get('audio_duration_seconds', 0.0)
                        else:
                            logger.warning(f"Failed to store audio for segment {synthesis_result.segment_index}")
                            synthesis_result.success = False
                            synthesis_result.error_message = storage_result.get('error_message', 'Audio storage failed')
                
                logger.info("Audio download and storage completed")
            
            # Step 4: Consolidate results and create audio manifest
            logger.info("Consolidating voice synthesis results and creating audio manifest")
            
            all_results = synthesis_results  # Include both successful and failed results
            
            final_result = await workflow.execute_activity(
                voice_synthesis.consolidate_voice_synthesis_activity,
                args=[request.video_id, all_results],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy,
                task_queue=request.voice_synthesis_queue
            )
            
            if final_result.success:
                logger.info(f"Voice synthesis workflow completed successfully for video {request.video_id}: "
                           f"{final_result.successful_synthesis}/{final_result.total_segments} segments synthesized")
            else:
                logger.error(f"Voice synthesis workflow completed with issues for video {request.video_id}: "
                           f"{final_result.error_message}")
            
            return final_result
            
        except Exception as e:
            logger.error(f"Voice synthesis workflow failed for video {request.video_id}: {e}")
            raise


# Workflow helper functions for integration with main dubbing pipeline

async def start_voice_synthesis_workflow(
    client,
    video_id: str,
    target_language: str,
    voice_config: Optional[dict] = None
) -> str:
    """
    Start voice synthesis workflow.
    
    Args:
        client: Temporal client
        video_id: Video identifier
        target_language: Target language code
        voice_config: Optional voice synthesis configuration parameters
        
    Returns:
        Workflow ID
    """
    from config import DubbingConfig
    config = DubbingConfig.from_env()
    
    # Create voice synthesis request
    request = VoiceSynthesisRequest(
        video_id=video_id,
        target_language=target_language,
        voice_id=voice_config.get('voice_id', 'Friendly_Person') if voice_config else 'Friendly_Person',
        emotion=voice_config.get('emotion', 'neutral') if voice_config else 'neutral',
        language_boost=voice_config.get('language_boost', 'Automatic') if voice_config else 'Automatic',
        english_normalization=voice_config.get('english_normalization', True) if voice_config else True,
        voice_synthesis_queue=config.voice_synthesis_queue
    )
    
    workflow_id = f"voice-synthesis7-{video_id}-{target_language}"
    
    # Start workflow
    handle = await client.start_workflow(
        VoiceSynthesisWorkflow.run,
        request,
        id=workflow_id,
        task_queue=config.consolidation_queue  # Use consolidation queue for main workflow orchestration
    )
    
    logger.info(f"Started voice synthesis workflow: {workflow_id}")
    return workflow_id


async def get_voice_synthesis_result(client, workflow_id: str) -> ConsolidatedVoiceSynthesisResult:
    """
    Get result from voice synthesis workflow.
    
    Args:
        client: Temporal client
        workflow_id: Workflow identifier
        
    Returns:
        ConsolidatedVoiceSynthesisResult
    """
    handle = client.get_workflow_handle(workflow_id)
    result = await handle.result()
    
    # Ensure result is converted to ConsolidatedVoiceSynthesisResult if it's a dict
    if isinstance(result, dict):
        return ConsolidatedVoiceSynthesisResult(**result)
    
    return result