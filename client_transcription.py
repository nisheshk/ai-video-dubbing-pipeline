"""Client for testing transcription workflow with Temporal Cloud."""

import asyncio
import logging
from temporalio.client import Client

from workflows.transcription_workflow import start_transcription_workflow, get_transcription_result
from config import DubbingConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Test transcription workflow."""
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
        logger.info(f"Queue Configuration:")
        logger.info(f"   - Consolidation Queue: {config.consolidation_queue}")
        logger.info(f"   - Transcription Queue: {config.transcription_queue}")
        logger.info(f"   - Rate Limit: {config.openai_rate_limit_rpm} RPM")
        logger.info(f"   - Max Concurrent: {config.max_concurrent_transcriptions}")
        
        # Test transcription with segments from previous speech segmentation workflow  
        video_id = "e82c5c2a-3099-476d-937b-caf03bcc4043"
        
        # Start transcription workflow
        logger.info(f"Starting transcription for video {video_id}")
        
        workflow_id = await start_transcription_workflow(
            client=client,
            video_id=video_id,
            transcription_config={
                "model": "whisper-1",
                "response_format": "json",
                "language": None,  # Auto-detect
                "prompt": None,
                "temperature": 0
            }
        )
        
        logger.info(f"Started workflow: {workflow_id}")
        
        # Wait for result
        logger.info("Waiting for transcription to complete...")
        result = await get_transcription_result(client, workflow_id)
        
        # Print results
        if result.success:
            logger.info("✅ Transcription completed successfully!")
            logger.info(f"📊 Results:")
            logger.info(f"   - Total segments: {result.total_segments}")
            logger.info(f"   - Successful transcriptions: {result.successful_transcriptions}")
            logger.info(f"   - Success rate: {result.success_rate:.1%}")
            logger.info(f"   - Primary language: {result.primary_language}")
            logger.info(f"   - Total words: {result.total_words}")
            logger.info(f"   - Language consistency: {result.language_consistency_score:.3f}")
            logger.info(f"   - Processing time: {result.processing_time_seconds:.2f}s")
            logger.info(f"   - API requests: {result.api_requests_count}")
            logger.info(f"   - Cost estimate: ${result.api_requests_count * 0.006:.3f}")
            logger.info(f"   - Transcript path: {result.gcs_transcript_path}")
            logger.info(f"📋 Queue Distribution:")
            logger.info(f"   - Workflow executed on: {config.consolidation_queue}")
            logger.info(f"   - Transcription tasks on: {config.transcription_queue}")
            logger.info(f"   - Consolidation tasks on: {config.consolidation_queue}")
            
            if result.transcriptions:
                logger.info(f"📝 First few transcriptions:")
                for i, transcription in enumerate(result.transcriptions[:5]):
                    if transcription.success:
                        text_preview = transcription.text[:100] + "..." if len(transcription.text) > 100 else transcription.text
                        logger.info(f"   Segment {i}: \"{text_preview}\" "
                                   f"({transcription.language}, {transcription.processing_time_seconds:.2f}s)")
                    else:
                        logger.info(f"   Segment {i}: FAILED - {transcription.error_message}")
            
            # Show full transcript preview
            if result.full_transcript:
                transcript_preview = result.full_transcript[:500] + "..." if len(result.full_transcript) > 500 else result.full_transcript
                logger.info(f"📄 Full transcript preview:")
                logger.info(f"   \"{transcript_preview}\"")
                    
        else:
            logger.error("❌ Transcription failed!")
            logger.error(f"Error: {result.error_message}")
            logger.error(f"Successful transcriptions: {result.successful_transcriptions}/{result.total_segments}")
            
    except Exception as e:
        logger.error(f"Failed to test transcription: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())