"""Voice synthesis worker for AI dubbing pipeline using Replicate Speech-02-HD."""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker

from config import DubbingConfig
from activities import voice_synthesis
from workflows.voice_synthesis_workflow import VoiceSynthesisWorkflow

# Configure logging to shared worker_test.log file
log_file_path = os.path.join(os.path.dirname(__file__), 'logs', 'worker_test.log')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='a'),  # Append to same file
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)


async def main():
    """Run the voice synthesis worker."""
    try:
        # Load configuration
        config = DubbingConfig.from_env()
        logger.info(f"Starting Voice Synthesis Worker - Environment: {config.environment}")
        logger.info(f"Voice Synthesis Queue: {config.voice_synthesis_queue}")
        
        # Validate Replicate API token
        import os
        replicate_token = os.getenv("REPLICATE_API_TOKEN")
        if not replicate_token:
            logger.error("REPLICATE_API_TOKEN environment variable not set")
            logger.error("Please set REPLICATE_API_TOKEN in your .env.cloud file")
            return
        
        logger.info("Replicate API token found ✓")
        
        # Connect to Temporal
        if config.environment in ["cloud", "production"]:
            # Temporal Cloud connection
            client = await Client.connect(
                config.temporal_cloud_address,
                namespace=config.temporal_cloud_namespace,
                tls=TLSConfig(),
                rpc_metadata={
                    "temporal-namespace": config.temporal_cloud_namespace,
                    "authorization": f"Bearer {config.temporal_cloud_api_key}"
                }
            )
            logger.info(f"Connected to Temporal Cloud: {config.temporal_cloud_namespace}")
        else:
            # Local Temporal connection
            client = await Client.connect("localhost:7233")
            logger.info("Connected to local Temporal server")
        
        # Test database connection
        try:
            from shared.database import get_database_client
            async with get_database_client(config) as db_client:
                if not db_client.pool:
                    await db_client.connect()
                async with db_client.pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
            logger.info("Database connection successful ✓")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return
        
        # Test GCS connection
        try:
            from shared.gcs_client import AsyncGCSClient
            gcs_client = AsyncGCSClient(config)
            await gcs_client.connect()
            logger.info("GCS connection successful ✓")
        except Exception as e:
            logger.error(f"GCS connection failed: {e}")
            return
        
        # Create and run worker
        worker = Worker(
            client,
            task_queue=config.voice_synthesis_queue,
            workflows=[
                VoiceSynthesisWorkflow,
            ],
            activities=[
                voice_synthesis.load_translations_for_tts_activity,
                voice_synthesis.synthesize_voice_segment_activity,
                voice_synthesis.download_and_store_audio_activity,
                voice_synthesis.consolidate_voice_synthesis_activity
            ],
            max_concurrent_activities=config.max_concurrent_activities or 5  # Limit based on Replicate API rate limits
        )
        
        logger.info(f"Voice Synthesis Worker started successfully!")
        logger.info(f"Task Queue: {config.voice_synthesis_queue}")
        logger.info(f"Max Concurrent Activities: {config.max_concurrent_activities or 5}")
        logger.info("Worker is ready to process voice synthesis requests...")
        logger.info("Press Ctrl+C to stop the worker")
        
        # Run worker
        await worker.run()
        
    except KeyboardInterrupt:
        logger.info("\nReceived interrupt signal. Shutting down worker...")
    except Exception as e:
        logger.error(f"Worker failed: {e}")
        raise
    finally:
        logger.info("Voice Synthesis Worker stopped")


if __name__ == "__main__":
    asyncio.run(main())