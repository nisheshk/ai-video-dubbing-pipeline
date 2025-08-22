#!/usr/bin/env python3
"""Test just the video muxing step."""

import asyncio
import logging
from datetime import timedelta
from temporalio.client import Client, TLSConfig
from temporalio import workflow
from config import DubbingConfig
from activities.audio_stitching import mux_video_with_dubbed_audio_activity

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@workflow.defn
class TestMuxWorkflow:
    @workflow.run
    async def run(self, video_id: str, audio_path: str, output_folder: str) -> dict:
        return await workflow.execute_activity(
            mux_video_with_dubbed_audio_activity,
            args=[video_id, audio_path, output_folder, True],
            start_to_close_timeout=timedelta(minutes=15)
        )

async def test_mux_step():
    """Test just the muxing step with existing data."""
    
    # Configuration - use your video ID and paths
    video_id = "fa2883f4-1040-42f6-bdcc-ffc45e9fef69"
    final_audio_gcs_path = f"gs://dubbing-pipeline/{video_id}/alignment_stitching/final_dubbed_audio.wav"
    output_gcs_folder = f"{video_id}/final_output"
    
    logger.info(f"🎬 Testing video muxing for video {video_id}")
    
    try:
        # Load configuration and connect to Temporal Cloud
        config = DubbingConfig.from_env('.env.cloud')
        
        client = await Client.connect(
            config.temporal_cloud_address,
            namespace=config.temporal_cloud_namespace,
            tls=TLSConfig(),
            rpc_metadata={
                "temporal-namespace": config.temporal_cloud_namespace,
                "authorization": f"Bearer {config.temporal_cloud_api_key}"
            }
        )
        
        logger.info("✅ Connected to Temporal Cloud")
        
        # Execute the workflow
        logger.info("🎬 Starting video muxing workflow...")
        
        workflow_id = f"test-mux-{video_id}"
        handle = await client.start_workflow(
            TestMuxWorkflow.run,
            args=[video_id, final_audio_gcs_path, output_gcs_folder],
            id=workflow_id,
            task_queue=config.task_queue
        )
        
        result = await handle.result()
        
        if result.get('success'):
            logger.info("🎉 Video muxing completed successfully!")
            logger.info(f"📁 Final video: {result.get('final_video_gcs_path')}")
            logger.info(f"🎵 Multitrack video: {result.get('multitrack_video_gcs_path')}")
            logger.info(f"⏱️ Processing time: {result.get('processing_time_seconds', 0):.2f}s")
        else:
            logger.error(f"❌ Video muxing failed: {result.get('error_message')}")
            
        return result
        
    except Exception as e:
        logger.error(f"💥 Muxing test failed: {e}")
        raise

if __name__ == "__main__":
    result = asyncio.run(test_mux_step())
    if result and result.get('success'):
        print("🎊 SUCCESS! Video muxing completed.")
    else:
        print(f"❌ FAILED: {result.get('error_message', 'Unknown error') if result else 'Test crashed'}")