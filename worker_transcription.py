#!/usr/bin/env python3
"""Dedicated Temporal worker for transcription tasks with rate limiting."""

import asyncio
import logging
import sys
import os
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker

from activities.transcription import (
    load_segments_activity,
    transcribe_segment_activity,
    consolidate_transcriptions_activity
)
from workflows.transcription_workflow import TranscriptionWorkflow
from config import DubbingConfig

# Configure structured logging to shared worker_test.log file
log_file_path = os.path.join(os.path.dirname(__file__), 'logs', 'worker_test.log')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='a'),  # Append to same file
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)


class TranscriptionWorkerConfig:
    """Transcription-specific worker configuration with rate limiting."""
    
    def __init__(self, config: DubbingConfig):
        self.config = config
        
        # Temporal Cloud Configuration
        self.temporal_cloud_namespace = config.temporal_cloud_namespace
        self.temporal_cloud_address = config.temporal_cloud_address  
        self.temporal_cloud_api_key = config.temporal_cloud_api_key
        
        # Transcription-specific configuration
        self.transcription_queue = config.transcription_queue
        self.openai_rate_limit_rpm = config.openai_rate_limit_rpm
        self.max_concurrent_transcriptions = config.max_concurrent_transcriptions
        
        # Worker identity
        self.worker_identity = f"transcription-worker-{os.getenv('HOSTNAME', 'unknown')}"
    
    def get_temporal_connection_config(self):
        """Get Temporal Cloud connection configuration."""
        if not all([
            self.temporal_cloud_namespace,
            self.temporal_cloud_address, 
            self.temporal_cloud_api_key
        ]):
            raise ValueError("Missing Temporal Cloud configuration")
            
        return {
            "target_host": self.temporal_cloud_address,
            "namespace": self.temporal_cloud_namespace,
            "tls": TLSConfig(
                server_root_ca_cert=None,
                client_cert=None,
                client_private_key=None,
            ),
            "api_key": self.temporal_cloud_api_key,
        }


async def main():
    """Run the dedicated transcription Temporal worker."""
    
    logger.info("🚀 Starting AI Dubbing Pipeline Transcription Worker")
    
    # Load transcription-specific configuration
    config = DubbingConfig.from_env()
    worker_config = TranscriptionWorkerConfig(config)
    
    # Validate required environment variables
    required_vars = [
        ("OPENAI_API_KEY", config.openai_api_key),
        ("GOOGLE_CLOUD_PROJECT", config.google_cloud_project),
        ("GCS_BUCKET_NAME", config.gcs_bucket_name),
        ("NEON_DATABASE_URL", config.neon_database_url),
        ("TEMPORAL_CLOUD_NAMESPACE", config.temporal_cloud_namespace),
        ("TEMPORAL_CLOUD_ADDRESS", config.temporal_cloud_address),
        ("TEMPORAL_CLOUD_API_KEY", config.temporal_cloud_api_key),
    ]
    
    missing_vars = []
    for var_name, var_value in required_vars:
        if not var_value:
            missing_vars.append(var_name)
    
    if missing_vars:
        logger.error(f"✗ Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set them in your .env.cloud file")
        return 1
    
    try:
        # Connect to Temporal Cloud
        logger.info("Connecting to Temporal Cloud...")
        connection_config = worker_config.get_temporal_connection_config()
        
        client = await Client.connect(
            connection_config["target_host"],
            namespace=connection_config["namespace"],
            tls=connection_config["tls"],
            rpc_metadata={"temporal-namespace": connection_config["namespace"], 
                         "authorization": f"Bearer {connection_config['api_key']}"}
        )
        
        logger.info("✓ Connected to Temporal Cloud")
        
        # Create dedicated transcription worker
        logger.info("Creating dedicated transcription worker...")
        worker = Worker(
            client,
            task_queue=worker_config.transcription_queue,
            workflows=[
                TranscriptionWorkflow,
            ],
            activities=[
                load_segments_activity,
                transcribe_segment_activity,
                consolidate_transcriptions_activity,
            ],
            # Configure for rate-limited transcription workload
            max_concurrent_activities=worker_config.max_concurrent_transcriptions,
        )
        
        logger.info("=" * 80)
        logger.info("🎤 AI DUBBING TRANSCRIPTION WORKER STARTED")
        logger.info(f"   Namespace: {worker_config.temporal_cloud_namespace}")
        logger.info(f"   Task Queue: {worker_config.transcription_queue}")
        logger.info(f"   Worker Identity: {worker_config.worker_identity}")
        logger.info(f"   Max Concurrent Transcriptions: {worker_config.max_concurrent_transcriptions}")
        logger.info(f"   OpenAI Rate Limit: {worker_config.openai_rate_limit_rpm} RPM")
        logger.info("=" * 80)
        logger.info("🎬 Worker is ready to process transcription tasks...")
        logger.info("   Send SIGTERM for graceful shutdown")
        logger.info("=" * 80)
        
        # Run worker with proper signal handling
        await worker.run()
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Transcription worker shutdown requested by user")
        return 0
    except Exception as e:
        logger.error(f"✗ Transcription worker failed to start: {e}", exc_info=True)
        return 1
    finally:
        logger.info("🔄 Transcription worker shutting down...")


if __name__ == "__main__":
    # Set up signal handling for graceful shutdown
    import signal
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run the transcription worker
    sys.exit(asyncio.run(main()))