#!/usr/bin/env python3
"""Run just Stage 6: Audio Alignment & Video Stitching"""

import asyncio
import logging
from datetime import datetime
from temporalio.client import Client, TLSConfig
from config import DubbingConfig
from workflows.alignment_stitching_workflow import AlignmentStitchingWorkflow

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_alignment_stage():
    """Run Stage 6: Audio Alignment & Video Stitching"""
    
    # Configuration
    video_id = "fa2883f4-1040-42f6-bdcc-fbc45e9fef69"  # Your video with successful voice synthesis
    target_language = "es"
    
    logger.info("🎯 Starting Stage 6: Audio Alignment & Video Stitching")
    logger.info(f"Video ID: {video_id}")
    logger.info(f"Target Language: {target_language}")
    
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
        
        # Start alignment workflow
        alignment_id = f"alignment-stitching-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        logger.info(f"🚀 Starting alignment workflow: {alignment_id}")
        
        await client.start_workflow(
            AlignmentStitchingWorkflow.run,
            args=[video_id, target_language, True, 2.0, 50.0],
            id=alignment_id,
            task_queue=config.task_queue
        )
        
        logger.info("⏳ Waiting for alignment workflow to complete...")
        
        # Wait for workflow completion
        handle = client.get_workflow_handle(alignment_id)
        result = await handle.result()
        
        if result.get('success'):
            logger.info("🎉 Stage 6 completed successfully!")
            logger.info(f"📁 Final video: {result.get('final_video_gcs_path')}")
            logger.info(f"🎵 Multitrack video: {result.get('multitrack_video_gcs_path')}")
            logger.info(f"📊 Segments processed: {result.get('segments_processed')}")
            logger.info(f"🎯 Timing accuracy: {result.get('avg_timing_accuracy_ms', 0):.1f}ms")
            logger.info(f"⭐ Overall sync score: {result.get('overall_sync_score', 0):.2f}/1.0")
        else:
            logger.error(f"❌ Stage 6 failed: {result.get('error_message')}")
            
        return result
        
    except Exception as e:
        logger.error(f"💥 Stage 6 failed with error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(run_alignment_stage())