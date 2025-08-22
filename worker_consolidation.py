#!/usr/bin/env python3
"""Dedicated Temporal worker for consolidation and metadata tasks."""

import asyncio
import logging
import sys
import os
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker

from workflows.translation_workflow import TranslationWorkflow
from workflows.voice_synthesis_workflow import VoiceSynthesisWorkflow
from activities.translation import (
    load_transcriptions_activity,
    store_translation_results_activity,
    consolidate_translations_activity
)
from activities.voice_synthesis import (
    load_translations_for_tts_activity,
    consolidate_voice_synthesis_activity
)
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


class ConsolidationWorkerConfig:
    """Consolidation-specific worker configuration."""
    
    def __init__(self, config: DubbingConfig):
        self.config = config
        
        # Temporal Cloud Configuration
        self.temporal_cloud_namespace = config.temporal_cloud_namespace
        self.temporal_cloud_address = config.temporal_cloud_address  
        self.temporal_cloud_api_key = config.temporal_cloud_api_key
        
        # Queue configuration
        self.consolidation_queue = config.consolidation_queue
        
        # Worker scaling configuration
        self.max_concurrent_activities = 5
        self.max_concurrent_workflow_tasks = 10
        
        # Worker identity
        self.worker_identity = f"consolidation-worker-{os.getenv('HOSTNAME', 'unknown')}"
    
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
    """Run the dedicated consolidation Temporal worker."""
    
    logger.info("🚀 Starting AI Dubbing Pipeline Consolidation Worker")
    
    # Load consolidation-specific configuration
    config = DubbingConfig.from_env()
    worker_config = ConsolidationWorkerConfig(config)
    
    # Validate required environment variables
    required_vars = [
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
        
        # Create dedicated consolidation worker
        logger.info("Creating dedicated consolidation worker...")
        worker = Worker(
            client,
            task_queue=worker_config.consolidation_queue,
            workflows=[
                TranslationWorkflow,
                VoiceSynthesisWorkflow,
            ],
            activities=[
                load_transcriptions_activity,
                store_translation_results_activity,
                consolidate_translations_activity,
                load_translations_for_tts_activity,
                consolidate_voice_synthesis_activity,
            ],
            max_concurrent_activities=worker_config.max_concurrent_activities,
            max_concurrent_workflow_tasks=worker_config.max_concurrent_workflow_tasks,
        )
        
        logger.info("=" * 80)
        logger.info("📋 AI DUBBING CONSOLIDATION WORKER STARTED")
        logger.info(f"   Namespace: {worker_config.temporal_cloud_namespace}")
        logger.info(f"   Task Queue: {worker_config.consolidation_queue}")
        logger.info(f"   Worker Identity: {worker_config.worker_identity}")
        logger.info(f"   Max Concurrent Activities: {worker_config.max_concurrent_activities}")
        logger.info(f"   Max Concurrent Workflows: {worker_config.max_concurrent_workflow_tasks}")
        logger.info("=" * 80)
        logger.info("🎬 Worker is ready to process workflows, metadata loading & consolidation tasks...")
        logger.info("   - Transcription workflows and consolidation")
        logger.info("   - Translation workflows and consolidation")
        logger.info("   - Voice synthesis workflows and consolidation")
        logger.info("   - Database operations and GCS uploads")
        logger.info("   Send SIGTERM for graceful shutdown")
        logger.info("=" * 80)
        
        # Run worker with proper signal handling
        await worker.run()
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Consolidation worker shutdown requested by user")
        return 0
    except Exception as e:
        logger.error(f"✗ Consolidation worker failed to start: {e}", exc_info=True)
        return 1
    finally:
        logger.info("🔄 Consolidation worker shutting down...")


if __name__ == "__main__":
    # Set up signal handling for graceful shutdown
    import signal
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run the consolidation worker
    sys.exit(asyncio.run(main()))