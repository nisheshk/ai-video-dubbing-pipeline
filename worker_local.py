#!/usr/bin/env python3
"""Local Temporal worker for AI dubbing pipeline development."""

import asyncio
import logging
import sys
import os
from temporalio.client import Client
from temporalio.worker import Worker

from workflows.audio_extraction_workflow import AudioExtractionWorkflow, AudioExtractionMonitorWorkflow
from activities.audio_extraction import audio_extraction_activities
from config import DubbingConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LocalWorkerConfig:
    """Local development configuration for Temporal worker."""
    TEMPORAL_HOST = "localhost"
    TEMPORAL_PORT = 7233
    TASK_QUEUE = "dubbing-task-queue"
    
    # Local development scaling
    MAX_CONCURRENT_ACTIVITIES = 5
    MAX_CONCURRENT_WORKFLOW_TASKS = 5
    MAX_CONCURRENT_ACTIVITY_TASKS = 10
    
    def get_temporal_connection_config(self):
        """Get Temporal connection configuration."""
        return {
            "target_host": f"{self.TEMPORAL_HOST}:{self.TEMPORAL_PORT}",
        }


async def main():
    """Run the local Temporal worker for dubbing pipeline."""
    
    logger.info("Starting AI Dubbing Pipeline Local Worker")
    
    # Load configuration
    config = DubbingConfig.from_env('.env.local')
    worker_config = LocalWorkerConfig()
    
    # Validate required environment variables
    required_vars = [
        "GOOGLE_CLOUD_PROJECT",
        "GCS_BUCKET_NAME", 
        "NEON_DATABASE_URL"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not getattr(config, var.lower(), None):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"✗ Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set them in your .env.local file")
        return 1
    
    # Optional variables with warnings
    optional_vars = [
        ("GOOGLE_APPLICATION_CREDENTIALS", config.google_application_credentials),
        ("FFMPEG_PATH", config.ffmpeg_path),
    ]
    
    for var_name, var_value in optional_vars:
        if not var_value:
            logger.warning(f"⚠ Optional variable {var_name} not set, using default")
    
    try:
        # Connect to Temporal server
        logger.info(f"Connecting to Temporal server at {worker_config.TEMPORAL_HOST}:{worker_config.TEMPORAL_PORT}")
        client = await Client.connect(
            worker_config.get_temporal_connection_config()["target_host"]
        )
        
        # Test Temporal connection
        await client.get_system_info()
        logger.info("✓ Successfully connected to Temporal server")
        
        # Validate configuration by testing connections
        logger.info("Validating service connections...")
        
        # Test database connection
        try:
            from shared.database import get_database_client
            async with get_database_client(config) as db:
                await db.pool.fetchval("SELECT 1")
            logger.info("✓ Database connection successful")
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            return 1
        
        # Test GCS connection
        try:
            from shared.gcs_client import AsyncGCSClient
            gcs_client = AsyncGCSClient(config)
            await gcs_client.connect()
            logger.info("✓ GCS connection successful")
        except Exception as e:
            logger.error(f"✗ GCS connection failed: {e}")
            return 1
        
        # Test FFmpeg availability
        try:
            import subprocess
            result = subprocess.run(
                [config.ffmpeg_path, "-version"], 
                capture_output=True, 
                check=True
            )
            logger.info("✓ FFmpeg available")
        except Exception as e:
            logger.warning(f"⚠ FFmpeg test failed: {e}")
            logger.warning("Audio extraction may fail without proper FFmpeg installation")
        
        # Create and start worker
        logger.info("Creating Temporal worker...")
        worker = Worker(
            client,
            task_queue=worker_config.TASK_QUEUE,
            workflows=[AudioExtractionWorkflow, AudioExtractionMonitorWorkflow],
            activities=[
                # Audio extraction activities
                audio_extraction_activities.download_video_activity,
                audio_extraction_activities.validate_video_activity,
                audio_extraction_activities.extract_audio_activity,
                audio_extraction_activities.upload_audio_activity,
                audio_extraction_activities.cleanup_temp_files_activity,
            ],
            max_concurrent_activities=worker_config.MAX_CONCURRENT_ACTIVITIES,
            max_concurrent_workflow_tasks=worker_config.MAX_CONCURRENT_WORKFLOW_TASKS,
            max_concurrent_activity_tasks=worker_config.MAX_CONCURRENT_ACTIVITY_TASKS,
        )
        
        logger.info("=" * 60)
        logger.info("🚀 AI Dubbing Pipeline Worker Started Successfully!")
        logger.info(f"   Task Queue: {worker_config.TASK_QUEUE}")
        logger.info(f"   Max Concurrent Activities: {worker_config.MAX_CONCURRENT_ACTIVITIES}")
        logger.info(f"   Max Concurrent Workflows: {worker_config.MAX_CONCURRENT_WORKFLOW_TASKS}")
        logger.info("=" * 60)
        logger.info("Worker is ready to process audio extraction workflows...")
        logger.info("Press Ctrl+C to stop the worker")
        logger.info("=" * 60)
        
        # Run worker
        await worker.run()
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Worker shutdown requested by user")
        return 0
    except Exception as e:
        logger.error(f"✗ Worker failed to start: {e}")
        return 1


def check_temporal_server():
    """Check if Temporal server is running."""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 7233))
        sock.close()
        return result == 0
    except Exception:
        return False


if __name__ == "__main__":
    # Check if Temporal server is running
    if not check_temporal_server():
        print("✗ Temporal server is not running on localhost:7233")
        print("Please start Temporal server first:")
        print("  temporal server start-dev")
        sys.exit(1)
    
    # Run the worker
    sys.exit(asyncio.run(main()))