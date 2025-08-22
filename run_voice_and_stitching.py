#!/usr/bin/env python3
"""Run just Voice Synthesis and Alignment/Stitching steps for a specific video"""

import asyncio
import logging
from datetime import datetime
from temporalio.client import Client, TLSConfig
from config import DubbingConfig
from workflows.voice_synthesis_workflow import VoiceSynthesisWorkflow
from workflows.alignment_stitching_workflow import AlignmentStitchingWorkflow

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_voice_and_stitching():
    """Run Voice Synthesis and Alignment/Stitching for specific video"""
    
    # Configuration
    video_id = "fa2883f4-1040-42f6-bdcc-fbc45e9fef69"  # Your video with existing translations
    target_language = "es"
    source_language = "en"
    
    logger.info(f"🎤 Starting Voice Synthesis + Alignment/Stitching for video {video_id}")
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
        
        # Step 1: Voice Synthesis
        logger.info("🎤 STEP 1: Voice Synthesis (TTS)")
        logger.info("=" * 50)
        
        voice_synthesis_id = f"voice-synthesis-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        logger.info(f"🚀 Starting voice synthesis workflow: {voice_synthesis_id}")
        
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
        
        logger.info("⏳ Waiting for voice synthesis to complete...")
        
        # Wait for voice synthesis completion
        handle = client.get_workflow_handle(voice_synthesis_id)
        voice_result = await handle.result()
        
        if not voice_result.get('success'):
            raise RuntimeError(f"Voice synthesis failed: {voice_result.get('error_message')}")
        
        voice_segments_count = voice_result['successful_synthesis']
        logger.info(f"🔊 Voice synthesis completed: {voice_segments_count} segments synthesized")
        
        # Step 2: Alignment & Stitching
        logger.info("\n🎯 STEP 2: Audio Alignment & Video Stitching")
        logger.info("=" * 50)
        
        alignment_id = f"alignment-stitching-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        logger.info(f"🚀 Starting alignment workflow: {alignment_id}")
        
        await client.start_workflow(
            AlignmentStitchingWorkflow.run,
            args=[video_id, target_language, True, 2.0, 50.0],
            id=alignment_id,
            task_queue=config.task_queue
        )
        
        logger.info("⏳ Waiting for alignment and stitching to complete...")
        
        # Wait for alignment completion
        alignment_handle = client.get_workflow_handle(alignment_id)
        alignment_result = await alignment_handle.result()
        
        if not alignment_result.get('success'):
            raise RuntimeError(f"Alignment and stitching failed: {alignment_result.get('error_message')}")
        
        # Final results
        logger.info("🎉 BOTH STEPS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"📁 Final video: {alignment_result.get('final_video_gcs_path')}")
        logger.info(f"🎵 Multitrack video: {alignment_result.get('multitrack_video_gcs_path')}")
        logger.info(f"📊 Voice segments: {voice_segments_count}")
        logger.info(f"📊 Alignment segments: {alignment_result.get('segments_processed')}")
        logger.info(f"🎯 Timing accuracy: {alignment_result.get('avg_timing_accuracy_ms', 0):.1f}ms")
        logger.info(f"⭐ Overall sync score: {alignment_result.get('overall_sync_score', 0):.2f}/1.0")
        logger.info("=" * 60)
        
        return {
            'success': True,
            'voice_synthesis': voice_result,
            'alignment_stitching': alignment_result
        }
        
    except Exception as e:
        logger.error(f"💥 Voice Synthesis + Stitching failed: {e}")
        return {
            'success': False,
            'error_message': str(e)
        }

if __name__ == "__main__":
    result = asyncio.run(run_voice_and_stitching())
    if result['success']:
        print("🎊 SUCCESS! Both voice synthesis and stitching completed.")
    else:
        print(f"❌ FAILED: {result['error_message']}")