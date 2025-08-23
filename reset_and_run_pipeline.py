#!/usr/bin/env python3
"""
Complete AI Dubbing Pipeline Runner
Drops all tables, recreates schema, and runs entire pipeline from start to finish.
"""

import os
import sys
import asyncio
import logging
import subprocess
import signal
from datetime import datetime
from typing import Dict, Any, Optional

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.database import get_database_client, AsyncDatabaseClient
from config import DubbingConfig
from workflows.audio_extraction_workflow import AudioExtractionWorkflow
from workflows.speech_segmentation_workflow import SpeechSegmentationWorkflow
from workflows.transcription_workflow import TranscriptionWorkflow
from workflows.translation_workflow import TranslationWorkflow
from workflows.voice_synthesis_workflow import VoiceSynthesisWorkflow
from workflows.alignment_stitching_workflow import AlignmentStitchingWorkflow

# Configure logging to file and console
log_file_path = os.path.join(os.path.dirname(__file__), 'logs', 'worker_test.log')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='w'),  # Overwrite file each run
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)


async def drop_all_tables(config: DubbingConfig) -> None:
    """Drop all existing tables to start fresh."""
    logger.info("🗑️  Dropping all existing tables...")
    
    drop_tables_sql = """
    -- Drop all tables in dependency order
    DROP TABLE IF EXISTS dubbed_videos CASCADE;
    DROP TABLE IF EXISTS audio_alignments CASCADE;
    DROP TABLE IF EXISTS voice_synthesis CASCADE;
    DROP TABLE IF EXISTS translations CASCADE;
    DROP TABLE IF EXISTS transcriptions CASCADE;
    DROP TABLE IF EXISTS workflow_executions CASCADE;
    DROP TABLE IF EXISTS processing_logs CASCADE;
    DROP TABLE IF EXISTS segments CASCADE;
    DROP TABLE IF EXISTS videos CASCADE;
    
    -- Drop functions if they exist
    DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;
    
    -- Drop UUID extension (will be recreated)
    DROP EXTENSION IF EXISTS "uuid-ossp" CASCADE;
    """
    
    try:
        async with get_database_client(config) as db_client:
            async with db_client.pool.acquire() as conn:
                await conn.execute(drop_tables_sql)
        logger.info("✅ All tables dropped successfully")
    except Exception as e:
        logger.error(f"❌ Failed to drop tables: {e}")
        raise


async def recreate_schema(config: DubbingConfig) -> None:
    """Recreate the complete database schema."""
    logger.info("🏗️  Recreating database schema...")
    
    try:
        async with get_database_client(config) as db_client:
            await db_client.create_schema()
        logger.info("✅ Database schema recreated successfully")
    except Exception as e:
        logger.error(f"❌ Failed to recreate schema: {e}")
        raise


async def wait_for_temporal_workflow(client, workflow_id: str, timeout_minutes: int = 30) -> Dict[str, Any]:
    """Wait for a Temporal workflow to complete."""
    from temporalio.client import WorkflowFailureError
    
    logger.info(f"⏳ Waiting for workflow {workflow_id} to complete (timeout: {timeout_minutes}min)...")
    
    try:
        handle = client.get_workflow_handle(workflow_id)
        result = await asyncio.wait_for(handle.result(), timeout=timeout_minutes * 60)
        
        if result.get('success', False):
            logger.info(f"✅ Workflow {workflow_id} completed successfully")
        else:
            logger.error(f"❌ Workflow {workflow_id} failed: {result.get('error_message', 'Unknown error')}")
        
        return result
        
    except asyncio.TimeoutError:
        logger.error(f"⏰ Workflow {workflow_id} timed out after {timeout_minutes} minutes")
        raise
    except WorkflowFailureError as e:
        logger.error(f"❌ Workflow {workflow_id} failed with error: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error waiting for workflow {workflow_id}: {e}")
        raise



async def validate_video_file(config: DubbingConfig, gcs_input_path: str) -> bool:
    """Validate that the input video file exists in GCS before starting pipeline."""
    try:
        from shared.gcs_client import AsyncGCSClient
        
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        
        # Extract blob path from full GCS URL
        blob_path = gcs_input_path.replace(f"gs://{config.gcs_bucket_name}/", "")
        blob = gcs_client.bucket.blob(blob_path)
        
        exists = await asyncio.to_thread(blob.exists)
        if exists:
            await asyncio.to_thread(blob.reload)
            size_mb = blob.size / (1024 * 1024)
            logger.info(f"✅ Input video validated: {gcs_input_path} ({size_mb:.1f} MB)")
            return True
        else:
            logger.error(f"❌ Input video not found: {gcs_input_path}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error validating video file: {e}")
        return False


async def run_complete_pipeline(
    video_id: str = "de5b5f5a-f7d6-43a1-af7b-2fb0db0f58fc",  # Test video
    source_language: str = "en",
    target_language: str = "ne",  # Nepali
    gcs_input_path: str = "gs://dubbing-pipeline/de5b5f5a-f7d6-43a1-af7b-2fb0db0f58fc/test_video1.mp4"
) -> Dict[str, Any]:
    """Run the complete AI dubbing pipeline from start to finish.
    
    Configured for:
    - Target language: Nepali (ne)
    - OpenAI refinement: Disabled (uses Google Translate only)
    - Filename extraction: Automatic from gcs_input_path
    """
    
    logger.info(f"🎬 Starting complete AI dubbing pipeline for video {video_id}")
    logger.info(f"📍 Source: {source_language} → Target: {target_language}")
    logger.info(f"📁 Input: {gcs_input_path}")
    
    # Pre-flight validation
    config = DubbingConfig.from_env()
    if not await validate_video_file(config, gcs_input_path):
        raise RuntimeError(f"Input video validation failed: {gcs_input_path}")
    
    pipeline_start_time = datetime.now()
    
    try:
        # Connect to Temporal Cloud
        from temporalio.client import Client, TLSConfig
        import os
        
        # Set environment variable for Temporal Cloud API key
        os.environ["TEMPORAL_CLOUD_API_KEY"] = config.temporal_cloud_api_key
        
        client = await Client.connect(
            config.temporal_cloud_address,
            namespace=config.temporal_cloud_namespace,
            tls=TLSConfig(server_root_ca_cert=None),
            rpc_metadata={"temporal-namespace": config.temporal_cloud_namespace, 
                         "authorization": f"Bearer {config.temporal_cloud_api_key}"}
        )
        
        pipeline_results = {
            'video_id': video_id,
            'source_language': source_language,
            'target_language': target_language,
            'pipeline_start_time': pipeline_start_time.isoformat(),
            'stages': {}
        }
        
        # Stage 1: Audio Extraction
        logger.info("\n🎵 STAGE 1: Audio Extraction")
        logger.info("=" * 50)
        
        audio_extraction_id = f"audio-extraction2-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Extract filename from path for proper database storage
        original_filename = gcs_input_path.split("/")[-1]
        
        await client.start_workflow(
            AudioExtractionWorkflow.run,
            {
                'video_id': video_id,
                'gcs_input_path': gcs_input_path,
                'original_filename': original_filename,
                'source_language': source_language,
                'target_language': target_language
            },
            id=audio_extraction_id,
            task_queue=config.task_queue
        )
        
        audio_result = await wait_for_temporal_workflow(client, audio_extraction_id, timeout_minutes=10)
        pipeline_results['stages']['audio_extraction'] = audio_result
        
        if not audio_result.get('success', False):
            raise RuntimeError(f"Audio extraction failed: {audio_result.get('error_message')}")
        
        gcs_audio_path = audio_result['gcs_audio_url']
        logger.info(f"📄 Audio extracted: {gcs_audio_path}")
        
        # Stage 2: Speech Segmentation
        logger.info("\n🗣️  STAGE 2: Speech Segmentation (VAD)")
        logger.info("=" * 50)
        
        segmentation_id = f"speech-segmentation-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        await client.start_workflow(
            SpeechSegmentationWorkflow.run,
            {
                'video_id': video_id,
                'gcs_audio_path': gcs_audio_path,
                'vad_threshold': 0.5,
                'min_speech_duration_ms': 1000,
                'max_segment_duration_s': 30
            },
            id=segmentation_id,
            task_queue=config.task_queue
        )
        
        segmentation_result = await wait_for_temporal_workflow(client, segmentation_id, timeout_minutes=15)
        pipeline_results['stages']['speech_segmentation'] = segmentation_result
        
        if not segmentation_result.get('success', False):
            raise RuntimeError(f"Speech segmentation failed: {segmentation_result.get('error_message')}")
        
        segments_count = segmentation_result['total_segments']
        logger.info(f"📊 Created {segments_count} speech segments")
        
        # Stage 3: Transcription
        logger.info("\n📝 STAGE 3: Transcription (Whisper)")
        logger.info("=" * 50)
        
        transcription_id = f"transcription-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        await client.start_workflow(
            TranscriptionWorkflow.run,
            {
                'video_id': video_id,
                'segments': segmentation_result['segments'],
                'model': 'whisper-1',
                'language': source_language,
                'transcription_queue': config.transcription_queue
            },
            id=transcription_id,
            task_queue=config.transcription_queue
        )
        
        transcription_result = await wait_for_temporal_workflow(client, transcription_id, timeout_minutes=20)
        pipeline_results['stages']['transcription'] = transcription_result
        
        if not transcription_result.get('success', False):
            raise RuntimeError(f"Transcription failed: {transcription_result.get('error_message')}")
        
        transcriptions_count = transcription_result['successful_transcriptions']
        logger.info(f"📄 Transcribed {transcriptions_count} segments")
        
        # Stage 4: Translation
        logger.info("\n🌍 STAGE 4: Translation (Google + OpenAI)")
        logger.info("=" * 50)
        
        translation_id = f"translation-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        await client.start_workflow(
            TranslationWorkflow.run,
            {
                'video_id': video_id,
                'target_language': target_language,
                'source_language': source_language,
                'cultural_context': 'general',
                'enable_refinement': False,  # Disable OpenAI refinement
                'translation_queue': config.translation_queue
            },
            id=translation_id,
            task_queue=config.translation_queue
        )
        
        translation_result = await wait_for_temporal_workflow(client, translation_id, timeout_minutes=25)
        pipeline_results['stages']['translation'] = translation_result
        
        if not translation_result.get('success', False):
            raise RuntimeError(f"Translation failed: {translation_result.get('error_message')}")
        
        translations_count = translation_result['successful_translations']
        logger.info(f"🔄 Translated {translations_count} segments to {target_language}")
        
        # Stage 5: Voice Synthesis
        logger.info("\n🎤 STAGE 5: Voice Synthesis (TTS)")
        logger.info("=" * 50)
        
        voice_synthesis_id = f"voice-synthesis-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        await client.start_workflow(
            VoiceSynthesisWorkflow.run,
            {
                'video_id': video_id,
                'target_language': target_language,
                'voice_id': 'Friendly_Person',
                'emotion': 'neutral',
                'voice_synthesis_queue': config.voice_synthesis_queue
            },
            id=voice_synthesis_id,
            task_queue=config.voice_synthesis_queue
        )
        
        voice_result = await wait_for_temporal_workflow(client, voice_synthesis_id, timeout_minutes=30)
        pipeline_results['stages']['voice_synthesis'] = voice_result
        
        if not voice_result.get('success', False):
            raise RuntimeError(f"Voice synthesis failed: {voice_result.get('error_message')}")
        
        voice_segments_count = voice_result['successful_synthesis']
        logger.info(f"🔊 Synthesized {voice_segments_count} voice segments")
        
        # Stage 6: Alignment & Stitching
        logger.info("\n🎯 STAGE 6: Audio Alignment & Video Stitching")
        logger.info("=" * 50)
        
        alignment_id = f"alignment-stitching-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        await client.start_workflow(
            AlignmentStitchingWorkflow.run,
            args=[video_id, target_language, True, 2.0, 50.0],
            id=alignment_id,
            task_queue=config.task_queue
        )
        
        alignment_result = await wait_for_temporal_workflow(client, alignment_id, timeout_minutes=25)
        pipeline_results['stages']['alignment_stitching'] = alignment_result
        
        if not alignment_result.get('success', False):
            error_msg = alignment_result.get('error_message', 'Unknown error')
            logger.error(f"❌ Alignment and stitching failed: {error_msg}")
            
            # Common fixes for mux issues
            if 'NotFound' in str(error_msg) or 'File not found' in str(error_msg):
                logger.info("💡 Tip: Check that video file path matches actual GCS filename")
                logger.info(f"   Expected: {gcs_input_path}")
                logger.info("   Common fix: Ensure database gcs_input_path matches actual file")
                
            raise RuntimeError(f"Alignment and stitching failed: {error_msg}")
        
        final_video_path = alignment_result['final_video_gcs_path']
        multitrack_path = alignment_result.get('multitrack_video_gcs_path')
        
        logger.info(f"🎉 Final dubbed video created: {final_video_path}")
        if multitrack_path:
            logger.info(f"🎵 Multitrack video created: {multitrack_path}")
        
        # Calculate overall pipeline metrics
        pipeline_end_time = datetime.now()
        total_processing_time = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        pipeline_results.update({
            'pipeline_end_time': pipeline_end_time.isoformat(),
            'total_processing_time_seconds': total_processing_time,
            'total_processing_time_minutes': total_processing_time / 60,
            'success': True,
            'final_outputs': {
                'dubbed_video': final_video_path,
                'multitrack_video': multitrack_path,
                'segments_processed': alignment_result['segments_processed'],
                'avg_timing_accuracy_ms': alignment_result.get('avg_timing_accuracy_ms', 0),
                'overall_sync_score': alignment_result.get('overall_sync_score', 0),
                'duration_accuracy_ms': alignment_result.get('duration_accuracy_ms', 0)
            }
        })
        
        # Final success summary
        logger.info("\n" + "=" * 60)
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY! 🎉")
        logger.info("=" * 60)
        logger.info(f"📁 Final dubbed video: {final_video_path}")
        if multitrack_path:
            logger.info(f"🎵 Multitrack version: {multitrack_path}")
        logger.info(f"⏱️  Total processing time: {total_processing_time/60:.1f} minutes")
        logger.info(f"📊 Segments processed: {alignment_result['segments_processed']}")
        logger.info(f"🎯 Timing accuracy: {alignment_result.get('avg_timing_accuracy_ms', 0):.1f}ms")
        logger.info(f"⭐ Overall sync score: {alignment_result.get('overall_sync_score', 0):.2f}/1.0")
        logger.info("=" * 60)
        
        return pipeline_results
        
    except Exception as e:
        pipeline_end_time = datetime.now()
        total_processing_time = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        logger.error(f"\n❌ PIPELINE FAILED after {total_processing_time/60:.1f} minutes")
        logger.error(f"💥 Error: {str(e)}")
        
        pipeline_results.update({
            'pipeline_end_time': pipeline_end_time.isoformat(),
            'total_processing_time_seconds': total_processing_time,
            'success': False,
            'error_message': str(e)
        })
        
        return pipeline_results


async def start_workers() -> list:
    """Start all required workers for the pipeline."""
    logger.info("🏃 Starting Temporal workers...")
    
    import subprocess
    import asyncio
    
    # List of worker scripts to start with multiple instances for high concurrency
    workers = [
        ("Audio Extraction & Speech Segmentation #1", "python worker_cloud.py"),
        ("Audio Extraction & Speech Segmentation #2", "python worker_cloud.py"),
        ("Audio Extraction & Speech Segmentation #3", "python worker_cloud.py"),
        ("Transcription #1", "python worker_transcription.py"),
        ("Transcription #2", "python worker_transcription.py"),
        ("Transcription #3", "python worker_transcription.py"),
        ("Translation #1", "python worker_translation.py"), 
        ("Translation #2", "python worker_translation.py"),
        ("Translation #3", "python worker_translation.py"),
        ("Voice Synthesis #1", "python worker_voice_synthesis.py"),
        ("Voice Synthesis #2", "python worker_voice_synthesis.py"),
        ("Voice Synthesis #3", "python worker_voice_synthesis.py"),
        ("Consolidation #1", "python worker_consolidation.py"),
        ("Consolidation #2", "python worker_consolidation.py")
    ]
    
    started_processes = []
    
    for worker_name, command in workers:
        try:
            logger.info(f"   ▶️  Starting {worker_name} worker...")
            
            # Start worker process in background
            process = subprocess.Popen(
                command.split(),
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid if hasattr(os, 'setsid') else None
            )
            
            started_processes.append({
                'name': worker_name,
                'process': process,
                'command': command
            })
            
            # Give each worker a moment to start
            await asyncio.sleep(2)
            
            logger.info(f"   ✅ {worker_name} worker started (PID: {process.pid})")
            
        except Exception as e:
            logger.error(f"   ❌ Failed to start {worker_name} worker: {e}")
    
    if started_processes:
        logger.info(f"🎉 Started {len(started_processes)} workers successfully")
        logger.info("⏳ Waiting 10 seconds for workers to initialize...")
        await asyncio.sleep(10)
    else:
        logger.error("❌ No workers started successfully")
        
    return started_processes


async def stop_workers(worker_processes: list):
    """Stop all worker processes."""
    logger.info("🛑 Stopping all workers...")
    
    import signal
    
    for worker in worker_processes:
        try:
            process = worker['process']
            if process.poll() is None:  # Process is still running
                logger.info(f"   🛑 Stopping {worker['name']} worker (PID: {process.pid})")
                
                # Try graceful shutdown first
                if hasattr(os, 'killpg'):
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                else:
                    process.terminate()
                
                # Wait a bit for graceful shutdown
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if still running
                    if hasattr(os, 'killpg'):
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    else:
                        process.kill()
                
                logger.info(f"   ✅ {worker['name']} worker stopped")
            
        except Exception as e:
            logger.warning(f"   ⚠️  Error stopping {worker['name']} worker: {e}")


async def check_prerequisites() -> bool:
    """Check if all prerequisites are met."""
    logger.info("🔍 Checking prerequisites...")
    
    # Load configuration
    config = DubbingConfig.from_env()
    
    # Check Temporal Cloud connection
    try:
        from temporalio.client import Client, TLSConfig
        import os
        
        # Set environment variable for Temporal Cloud API key
        os.environ["TEMPORAL_CLOUD_API_KEY"] = config.temporal_cloud_api_key
        
        # Connect to Temporal Cloud
        client = await Client.connect(
            config.temporal_cloud_address,
            namespace=config.temporal_cloud_namespace,
            tls=TLSConfig(server_root_ca_cert=None),
            rpc_metadata={"temporal-namespace": config.temporal_cloud_namespace, 
                         "authorization": f"Bearer {config.temporal_cloud_api_key}"}
        )
        logger.info("✅ Temporal Cloud connection verified")
    except Exception as e:
        logger.error(f"❌ Cannot connect to Temporal Cloud: {e}")
        return False
    
    # Check database connection
    try:
        async with get_database_client(config) as db_client:
            async with db_client.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
        logger.info("✅ Database connection verified")
    except Exception as e:
        logger.error(f"❌ Cannot connect to database: {e}")
        return False
    
    logger.info("✅ All prerequisites met")
    return True


async def main():
    """Main pipeline execution function."""
    print("""
    ╔══════════════════════════════════════════════════════════════════╗
    ║                    AI DUBBING PIPELINE RUNNER                   ║
    ║                     Complete Reset & Run                        ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)
    
    worker_processes = []
    
    try:
        # Check prerequisites
        if not await check_prerequisites():
            logger.error("❌ Prerequisites not met. Exiting.")
            return 1
        
        # Load configuration
        config = DubbingConfig.from_env()
        
        # Drop and recreate tables
        await drop_all_tables(config)
        await recreate_schema(config)
        
        # Start all workers
        worker_processes = await start_workers()
        if not worker_processes:
            logger.error("❌ Failed to start workers. Cannot run pipeline.")
            return 1
        
        # Run complete pipeline
        result = await run_complete_pipeline(
            video_id="de5b5f5a-f7d6-43a1-af7b-2fb0db0f58fc",
            source_language="en",
            target_language="ne",  # Nepali
            gcs_input_path="gs://dubbing-pipeline/de5b5f5a-f7d6-43a1-af7b-2fb0db0f58fc/test_video1.mp4"
        )
        
        if result['success']:
            logger.info("🎊 PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
            return 0
        else:
            logger.error(f"💥 PIPELINE FAILED: {result.get('error_message')}")
            return 1
            
    except KeyboardInterrupt:
        logger.info("\n⏹️  Pipeline execution interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        return 1
    finally:
        # Always stop workers on exit
        if worker_processes:
            await stop_workers(worker_processes)


if __name__ == "__main__":
    import sys
    
    # Run the pipeline
    exit_code = asyncio.run(main())
    sys.exit(exit_code)