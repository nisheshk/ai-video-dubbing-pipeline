"""Client for testing speech segmentation workflow with Temporal Cloud."""

import asyncio
import logging
from temporalio.client import Client

from workflows.speech_segmentation_workflow import start_speech_segmentation_workflow, get_speech_segmentation_result
from config import DubbingConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Test speech segmentation workflow."""
    try:
        # Load configuration
        config = DubbingConfig.from_env()
        
        # Connect to Temporal Cloud with API key authentication
        from temporalio.client import TLSConfig
        
        client = await Client.connect(
            config.temporal_cloud_address,
            namespace=config.temporal_cloud_namespace,
            tls=TLSConfig(),
            rpc_metadata={"temporal-namespace": config.temporal_cloud_namespace, 
                         "authorization": f"Bearer {config.temporal_cloud_api_key}"}
        )
        
        logger.info("Connected to Temporal Cloud")
        
        # Test speech segmentation with the extracted audio from previous workflow  
        video_id = "e82c5c2a-3099-476d-937b-caf03bcc4043"
        gcs_audio_path = f"gs://{config.gcs_bucket_name}/{video_id}/audio/extracted.wav"
        
        # Start speech segmentation workflow
        logger.info(f"Starting speech segmentation for video {video_id}")
        logger.info(f"Audio path: {gcs_audio_path}")
        
        workflow_id = await start_speech_segmentation_workflow(
            client=client,
            video_id=video_id,
            gcs_audio_path=gcs_audio_path,
            vad_config={
                "vad_threshold": 0.5,
                "min_speech_duration_ms": 1000,
                "max_segment_duration_s": 30,
                "min_silence_gap_ms": 500,
                "speech_padding_ms": 50
            }
        )
        
        logger.info(f"Started workflow: {workflow_id}")
        
        # Wait for result
        logger.info("Waiting for speech segmentation to complete...")
        result = await get_speech_segmentation_result(client, workflow_id)
        
        # Print results
        if result.success:
            logger.info("✅ Speech segmentation completed successfully!")
            logger.info(f"📊 Results:")
            logger.info(f"   - Total segments: {result.total_segments}")
            logger.info(f"   - Total speech duration: {result.total_speech_duration:.2f}s")
            logger.info(f"   - Processing time: {result.processing_time_seconds:.2f}s")
            logger.info(f"   - Speech/silence ratio: {result.speech_to_silence_ratio:.3f}")
            logger.info(f"   - Average segment duration: {result.avg_segment_duration:.2f}s")
            logger.info(f"   - Manifest path: {result.gcs_manifest_path}")
            logger.info(f"   - Segments folder: {result.gcs_segments_folder}")
            
            if result.segments:
                logger.info(f"📝 First few segments:")
                for i, segment in enumerate(result.segments[:3]):
                    logger.info(f"   Segment {i}: {segment['start_time']:.2f}s - {segment['end_time']:.2f}s "
                               f"({segment['duration']:.2f}s, confidence: {segment['confidence_score']:.3f})")
                    
        else:
            logger.error("❌ Speech segmentation failed!")
            logger.error(f"Error: {result.error_message}")
            
    except Exception as e:
        logger.error(f"Failed to test speech segmentation: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())