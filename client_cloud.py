#!/usr/bin/env python3
"""Client script to trigger audio extraction workflows on Temporal Cloud."""

import asyncio
import logging
import sys
import uuid
from datetime import timedelta
from temporalio.client import Client, TLSConfig

from workflows.audio_extraction_workflow import AudioExtractionWorkflow
from shared.models import AudioExtractionRequest
from config import DubbingConfig
from shared.database import get_database_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def create_temporal_client(config: DubbingConfig) -> Client:
    """Create Temporal Cloud client."""
    if not all([
        config.temporal_cloud_namespace,
        config.temporal_cloud_address,
        config.temporal_cloud_api_key
    ]):
        raise ValueError("Missing Temporal Cloud configuration")
    
    logger.info(f"Connecting to Temporal Cloud: {config.temporal_cloud_address}")
    
    client = await Client.connect(
        config.temporal_cloud_address,
        namespace=config.temporal_cloud_namespace,
        tls=TLSConfig(),
        rpc_metadata={"temporal-namespace": config.temporal_cloud_namespace, 
                     "authorization": f"Bearer {config.temporal_cloud_api_key}"}
    )
    
    logger.info("✅ Connected to Temporal Cloud")
    return client


async def trigger_audio_extraction(
    video_id: str,
    gcs_input_path: str,
    original_filename: str,
    source_language: str = "en",
    target_language: str = "es"
):
    """Trigger audio extraction workflow for a specific video."""
    
    logger.info("🚀 Starting Audio Extraction Client")
    logger.info(f"Video ID: {video_id}")
    logger.info(f"GCS Path: {gcs_input_path}")
    
    # Load configuration from environment variables
    config = DubbingConfig.from_env()
    
    try:
        # Skip database record creation for now
        logger.info("⚠️  Skipping database record creation (will be created by workflow)")
        # async with get_database_client(config) as db:
        #     await db.create_video_record(
        #         video_id=video_id,
        #         original_filename=original_filename,
        #         gcs_input_path=gcs_input_path,
        #         source_language=source_language,
        #         target_language=target_language
        #     )
        # logger.info("✅ Video record created")
        
        # Connect to Temporal Cloud
        client = await create_temporal_client(config)
        
        # Create workflow request
        request = AudioExtractionRequest(
            video_id=video_id,
            gcs_input_path=gcs_input_path,
            original_filename=original_filename,
            source_language=source_language,
            target_language=target_language
        )
        
        # Generate workflow ID
        workflow_id = f"audio-extraction-{video_id}"
        
        logger.info(f"Starting workflow: {workflow_id}")
        logger.info(f"Task Queue: {config.task_queue}")
        
        # Start workflow execution
        workflow_handle = await client.start_workflow(
            AudioExtractionWorkflow.run,
            request,
            id=workflow_id,
            task_queue=config.task_queue or "dubbing-prod-queue",
            execution_timeout=timedelta(hours=1),
            task_timeout=timedelta(minutes=10)
        )
        
        logger.info("=" * 80)
        logger.info("🎬 WORKFLOW STARTED SUCCESSFULLY")
        logger.info(f"   Workflow ID: {workflow_id}")
        logger.info(f"   Run ID: {workflow_handle.id}")
        logger.info(f"   Video ID: {video_id}")
        logger.info("=" * 80)
        
        # Wait for workflow to complete (optional - you can exit here)
        print("\nChoose an option:")
        print("1. Wait for workflow completion")
        print("2. Exit now (workflow continues in background)")
        choice = input("Enter choice (1 or 2): ").strip()
        
        if choice == "1":
            logger.info("⏳ Waiting for workflow completion...")
            logger.info("   (This may take several minutes)")
            
            result = await workflow_handle.result()
            
            logger.info("=" * 80)
            if result.success:
                logger.info("🎉 WORKFLOW COMPLETED SUCCESSFULLY!")
                logger.info(f"   Audio URL: {result.gcs_audio_url}")
                logger.info(f"   Processing Time: {result.processing_time_seconds:.2f}s")
                logger.info(f"   Video Duration: {result.video_metadata.get('duration', 'unknown')}s")
            else:
                logger.error("❌ WORKFLOW FAILED")
                logger.error(f"   Error: {result.error_message}")
            logger.info("=" * 80)
        else:
            logger.info("✅ Workflow started in background")
            logger.info(f"   You can monitor it in Temporal Cloud UI")
            logger.info(f"   Workflow ID: {workflow_id}")
        
        return workflow_handle
        
    except Exception as e:
        logger.error(f"❌ Failed to start workflow: {e}", exc_info=True)
        return None


async def main():
    """Main function - trigger workflow for test video."""
    
    # Test video configuration
    TEST_VIDEO_ID = "d82c5c2a-3099-476d-937b-caf03bcc4043"
    TEST_GCS_PATH = "d82c5c2a-3099-476d-937b-caf03bcc4043/test_video1.mp4"
    TEST_FILENAME = "test_video1.mp4"
    
    logger.info("🎯 Triggering Audio Extraction for Test Video")
    
    workflow_handle = await trigger_audio_extraction(
        video_id=TEST_VIDEO_ID,
        gcs_input_path=TEST_GCS_PATH,
        original_filename=TEST_FILENAME,
        source_language="en",
        target_language="es"
    )
    
    if workflow_handle:
        logger.info("✅ Successfully triggered workflow")
        return 0
    else:
        logger.error("❌ Failed to trigger workflow")
        return 1


async def trigger_custom_video():
    """Interactive function to trigger workflow for custom video."""
    
    print("\n🎬 Custom Video Processing")
    print("=" * 50)
    
    video_id = input("Enter Video ID (UUID): ").strip()
    if not video_id:
        video_id = str(uuid.uuid4())
        print(f"Generated Video ID: {video_id}")
    
    gcs_path = input("Enter GCS path (e.g., folder/video.mp4): ").strip()
    if not gcs_path:
        print("❌ GCS path is required")
        return
    
    filename = input("Enter original filename: ").strip()
    if not filename:
        filename = gcs_path.split('/')[-1]
    
    source_lang = input("Source language [en]: ").strip() or "en"
    target_lang = input("Target language [es]: ").strip() or "es"
    
    workflow_handle = await trigger_audio_extraction(
        video_id=video_id,
        gcs_input_path=gcs_path,
        original_filename=filename,
        source_language=source_lang,
        target_language=target_lang
    )
    
    return workflow_handle


if __name__ == "__main__":
    print("🎵 AI Dubbing Pipeline - Temporal Cloud Client")
    print("=" * 60)
    print("1. Process test video (d82c5c2a-3099-476d-937b-caf03bcc4043)")
    print("2. Process custom video")
    print("3. Exit")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        sys.exit(asyncio.run(main()))
    elif choice == "2":
        result = asyncio.run(trigger_custom_video())
        sys.exit(0 if result else 1)
    else:
        print("👋 Goodbye!")
        sys.exit(0)