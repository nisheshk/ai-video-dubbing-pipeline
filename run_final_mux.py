#!/usr/bin/env python3
"""Run the final muxing step manually after fixing the database path"""

import asyncio
import logging
from activities.audio_stitching import mux_video_with_dubbed_audio_activity
from config import DubbingConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_final_mux():
    """Run the final muxing step with the corrected database paths"""
    
    video_id = "aa64131d-8d3b-4649-8033-d146c834e867"
    config = DubbingConfig.from_env('.env.cloud')
    
    # Parameters for muxing - corrected audio path
    final_audio_gcs_path = f"gs://{config.gcs_bucket_name}/{video_id}/final_output/final_dubbed_audio_{video_id}.wav"
    output_gcs_folder = f"{video_id}/final_output"
    create_multitrack = True
    
    logger.info(f"🎬 Starting final muxing for video {video_id}")
    logger.info(f"📹 Expected audio path: {final_audio_gcs_path}")
    logger.info(f"📁 Output folder: {output_gcs_folder}")
    
    try:
        # Run the muxing activity directly
        result = await mux_video_with_dubbed_audio_activity(
            video_id=video_id,
            final_audio_gcs_path=final_audio_gcs_path,
            output_gcs_folder=output_gcs_folder,
            create_multitrack=create_multitrack
        )
        
        if result['success']:
            logger.info("✅ Muxing completed successfully!")
            logger.info(f"📹 Final video: {result['final_video_gcs_path']}")
            if result.get('multitrack_video_gcs_path'):
                logger.info(f"🎵 Multitrack video: {result['multitrack_video_gcs_path']}")
            logger.info(f"⏱️  Processing time: {result['processing_time_seconds']:.1f}s")
            logger.info(f"🎯 Duration accuracy: {result['duration_accuracy_ms']:.1f}ms")
        else:
            logger.error(f"❌ Muxing failed: {result['error_message']}")
            
    except Exception as e:
        logger.error(f"❌ Error running muxing: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(run_final_mux())