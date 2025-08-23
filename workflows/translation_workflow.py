"""Translation workflow for AI dubbing pipeline using Google Translate v3 + OpenAI refinement."""

import asyncio
import logging
from datetime import timedelta
from typing import List, Optional, Dict, Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from shared.models import TranslationRequest, ConsolidatedTranslationResult
from activities import translation

logger = logging.getLogger(__name__)


@workflow.defn
class TranslationWorkflow:
    """Workflow for translating transcribed segments with cultural refinement."""

    @workflow.run
    async def run(self, request: TranslationRequest) -> ConsolidatedTranslationResult:
        """
        Execute translation workflow with Google Translate + OpenAI refinement.
        
        Args:
            request: Translation request with video ID, target language, and configuration
            
        Returns:
            ConsolidatedTranslationResult with both Google and OpenAI translations
        """
        logger.info(f"Starting translation workflow for video {request.video_id} to {request.target_language}")
        
        # Define retry policy for activities  
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=5),
            maximum_attempts=3
        )
        
        try:
            # Step 1: Load transcriptions from database by video_id
            logger.info(f"Loading transcriptions for video {request.video_id}")
            
            transcriptions = await workflow.execute_activity(
                translation.load_transcriptions_activity,
                args=[request.video_id],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
                # Uses current queue (consolidation queue) for metadata loading
            )
            
            if not transcriptions:
                logger.error(f"No transcriptions found for video {request.video_id}")
                return ConsolidatedTranslationResult(
                    video_id=request.video_id,
                    source_language="unknown",
                    target_language=request.target_language,
                    cultural_context=request.cultural_context,
                    total_segments=0,
                    successful_translations=0,
                    success_rate=0.0,
                    translations=[],
                    avg_google_confidence=0.0,
                    avg_translation_quality=0.0,
                    total_processing_time_seconds=0.0,
                    google_processing_time_seconds=0.0,
                    openai_processing_time_seconds=0.0,
                    gcs_translation_path="",
                    success=False,
                    error_message="No transcriptions found for translation"
                )
            
            logger.info(f"Found {len(transcriptions)} transcriptions to translate")
            
            # Step 2: Create parallel Google Translate tasks for each transcription
            logger.info("Creating parallel Google Translate tasks...")
            
            google_tasks = []
            for transcription in transcriptions:
                # Execute each Google translation task on dedicated translation queue
                task = workflow.execute_activity(
                    translation.translate_segment_google_activity,
                    args=[
                        request.video_id, 
                        transcription, 
                        request.target_language, 
                        request.source_language
                    ],
                    start_to_close_timeout=timedelta(minutes=10),  # Increased timeout for API rate limits
                    retry_policy=retry_policy,
                    task_queue=request.translation_queue  # Use translation queue from request
                )
                google_tasks.append(task)
            
            logger.info(f"Created {len(google_tasks)} parallel Google Translate tasks on queue: {request.translation_queue}")
            
            # Step 3: Wait for all Google translation tasks to complete
            logger.info("Waiting for all Google Translate tasks to complete...")
            google_results = await asyncio.gather(*google_tasks)
            
            successful_google_count = sum(1 for r in google_results if r.get('success', False))
            logger.info(f"Google translation tasks completed: {successful_google_count}/{len(google_results)} successful")
            
            # Step 4: Create parallel OpenAI refinement tasks (only if refinement is enabled)
            openai_results = []
            if request.enable_refinement:
                logger.info("Creating parallel OpenAI refinement tasks...")
                
                openai_tasks = []
                for google_result in google_results:
                    if google_result.get('success', False):
                        # Execute each OpenAI refinement task on dedicated translation queue
                        task = workflow.execute_activity(
                            translation.refine_translation_openai_activity,
                            args=[
                                request.video_id, 
                                google_result, 
                                request.cultural_context
                            ],
                            start_to_close_timeout=timedelta(minutes=10),  # Longer timeout for OpenAI
                            retry_policy=retry_policy,
                            task_queue=request.translation_queue  # Use translation queue from request
                        )
                        openai_tasks.append(task)
                    else:
                        # For failed Google translations, create a placeholder result
                        openai_result = google_result.copy()
                        openai_result.update({
                            "openai_refined_translation": google_result.get('google_translation', ''),
                            "openai_processing_time_seconds": 0.0,
                            "cultural_context": request.cultural_context,
                            "processing_time_total_seconds": google_result.get('google_processing_time_seconds', 0)
                        })
                        openai_results.append(openai_result)
                
                if openai_tasks:
                    logger.info(f"Created {len(openai_tasks)} parallel OpenAI refinement tasks")
                    
                    # Wait for all OpenAI refinement tasks to complete
                    logger.info("Waiting for all OpenAI refinement tasks to complete...")
                    refined_results = await asyncio.gather(*openai_tasks)
                    openai_results.extend(refined_results)
                
                successful_openai_count = sum(1 for r in openai_results if not r.get('error_message', '').startswith('OpenAI refinement failed'))
                logger.info(f"OpenAI refinement tasks completed: {successful_openai_count}/{len(google_results)} successful")
            else:
                # If refinement is disabled, use Google translations as final results
                logger.info("OpenAI refinement disabled, using Google translations as final results")
                for google_result in google_results:
                    openai_result = google_result.copy()
                    openai_result.update({
                        "openai_refined_translation": google_result.get('google_translation', ''),
                        "openai_processing_time_seconds": 0.0,
                        "cultural_context": request.cultural_context,
                        "processing_time_total_seconds": google_result.get('google_processing_time_seconds', 0)
                    })
                    openai_results.append(openai_result)
            
            # Step 5: Store translation results in database
            logger.info("Storing translation results in database...")
            
            await workflow.execute_activity(
                translation.store_translation_results_activity,
                args=[request.video_id, openai_results],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
                # Uses current queue (consolidation queue) for database storage
            )
            
            # Step 6: Consolidate results into final output
            logger.info("Consolidating translation results...")
            
            final_result = await workflow.execute_activity(
                translation.consolidate_translations_activity,
                args=[request.video_id, openai_results, request.target_language],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
                # Uses current queue (consolidation queue) for consolidation
            )
            
            if final_result.success:
                logger.info(f"Translation workflow completed successfully for video {request.video_id}: "
                           f"{final_result.successful_translations}/{final_result.total_segments} segments, "
                           f"target language: {final_result.target_language}, "
                           f"cultural context: {final_result.cultural_context}")
            else:
                logger.error(f"Translation workflow failed for video {request.video_id}: "
                           f"{final_result.error_message}")
            
            return final_result
            
        except Exception as e:
            logger.error(f"Translation workflow failed for video {request.video_id}: {e}")
            raise


# Workflow helper functions for integration with main dubbing pipeline

async def start_translation_workflow(
    client,
    video_id: str,
    target_language: str,
    source_language: Optional[str] = None,
    cultural_context: str = "general",
    enable_refinement: bool = True
) -> str:
    """
    Start translation workflow.
    
    Args:
        client: Temporal client
        video_id: Video identifier
        target_language: Target language code (e.g., 'es', 'fr', 'de')
        source_language: Source language code (optional, auto-detect if None)
        cultural_context: Cultural context for refinement
        enable_refinement: Whether to enable OpenAI cultural refinement
        
    Returns:
        Workflow ID
    """
    from config import DubbingConfig
    config = DubbingConfig.from_env()
    
    # Create translation request with queue information
    request = TranslationRequest(
        video_id=video_id,
        target_language=target_language,
        source_language=source_language,
        cultural_context=cultural_context,
        enable_refinement=enable_refinement,
        translation_queue=config.translation_queue  # Pass queue name to avoid config loading in workflow
    )
    
    workflow_id = f"translation5-{video_id}-{target_language}"
    
    # Start workflow
    handle = await client.start_workflow(
        TranslationWorkflow.run,
        request,
        id=workflow_id,
        task_queue=config.consolidation_queue  # Main workflow uses consolidation queue
    )
    
    logger.info(f"Started translation workflow: {workflow_id}")
    return workflow_id


async def get_translation_result(client, workflow_id: str) -> ConsolidatedTranslationResult:
    """
    Get result from translation workflow.
    
    Args:
        client: Temporal client
        workflow_id: Workflow identifier
        
    Returns:
        ConsolidatedTranslationResult
    """
    handle = client.get_workflow_handle(workflow_id)
    result = await handle.result()
    
    # Ensure result is converted to ConsolidatedTranslationResult if it's a dict
    if isinstance(result, dict):
        return ConsolidatedTranslationResult(**result)
    
    return result