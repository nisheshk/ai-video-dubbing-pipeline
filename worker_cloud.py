#!/usr/bin/env python3
"""Cloud Temporal worker for AI dubbing pipeline production deployment."""

import asyncio
import logging
import sys
import os
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker

from workflows.audio_extraction_workflow import AudioExtractionWorkflow, AudioExtractionMonitorWorkflow  
from activities.audio_extraction import audio_extraction_activities
from config import DubbingConfig

# Configure structured logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)


class CloudWorkerConfig:
    """Production cloud configuration for Temporal worker."""
    
    def __init__(self, config: DubbingConfig):
        self.config = config
        
        # Temporal Cloud Configuration
        self.temporal_cloud_namespace = config.temporal_cloud_namespace
        self.temporal_cloud_address = config.temporal_cloud_address  
        self.temporal_cloud_api_key = config.temporal_cloud_api_key
        
        # Task queue configuration
        self.task_queue = config.task_queue or "dubbing-prod-queue"
        
        # Production scaling configuration
        self.max_concurrent_activities = config.max_concurrent_activities or 20
        self.max_concurrent_workflow_tasks = config.max_concurrent_workflow_tasks or 10
        self.max_concurrent_activity_tasks = config.max_concurrent_activity_tasks or 50
        
        # Worker identity
        self.worker_identity = f"dubbing-worker-{os.getenv('HOSTNAME', 'unknown')}"
    
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
                # For Temporal Cloud, typically use default TLS
                server_root_ca_cert=None,
                # API key authentication
                client_cert=None,
                client_private_key=None,
            ),
            "api_key": self.temporal_cloud_api_key,
        }


async def setup_health_checks(config: DubbingConfig):
    """Set up health checks for production monitoring."""
    logger.info("Setting up production health checks...")
    
    # Test critical dependencies
    health_status = {
        "database": False,
        "gcs": False,
        "temporal": False,
        "ffmpeg": False
    }
    
    # Database health check
    try:
        from shared.database import get_database_client
        async with get_database_client(config) as db:
            await db.pool.fetchval("SELECT 1")
        health_status["database"] = True
        logger.info("✓ Database connection healthy")
    except Exception as e:
        logger.error(f"✗ Database health check failed: {e}")
        raise
    
    # GCS health check  
    try:
        from shared.gcs_client import AsyncGCSClient
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        health_status["gcs"] = True
        logger.info("✓ GCS connection healthy")
    except Exception as e:
        logger.error(f"✗ GCS health check failed: {e}")
        raise
    
    # FFmpeg health check
    try:
        import subprocess
        result = subprocess.run(
            [config.ffmpeg_path, "-version"],
            capture_output=True,
            check=True,
            timeout=10
        )
        health_status["ffmpeg"] = True
        logger.info("✓ FFmpeg available and healthy")
    except Exception as e:
        logger.error(f"✗ FFmpeg health check failed: {e}")
        raise
    
    return health_status


async def main():
    """Run the cloud Temporal worker for production."""
    
    logger.info("🚀 Starting AI Dubbing Pipeline Cloud Worker")
    
    # Load production configuration
    config = DubbingConfig.from_env('.env.cloud')
    worker_config = CloudWorkerConfig(config)
    
    # Validate required environment variables for production
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
        # Run health checks
        logger.info("Running production health checks...")
        await setup_health_checks(config)
        logger.info("✓ All health checks passed")
        
        # Connect to Temporal Cloud
        logger.info("Connecting to Temporal Cloud...")
        connection_config = worker_config.get_temporal_connection_config()
        
        client = await Client.connect(
            connection_config["target_host"],
            namespace=connection_config["namespace"],
            tls=connection_config["tls"],
            api_key=connection_config["api_key"]
        )
        
        # Test Temporal connection
        system_info = await client.get_system_info()
        logger.info(f"✓ Connected to Temporal Cloud: {system_info}")
        
        # Create production worker with optimized settings
        logger.info("Creating production Temporal worker...")
        worker = Worker(
            client,
            task_queue=worker_config.task_queue,
            workflows=[AudioExtractionWorkflow, AudioExtractionMonitorWorkflow],
            activities=[
                # Audio extraction activities
                audio_extraction_activities.download_video_activity,
                audio_extraction_activities.validate_video_activity, 
                audio_extraction_activities.extract_audio_activity,
                audio_extraction_activities.upload_audio_activity,
                audio_extraction_activities.cleanup_temp_files_activity,
            ],
            max_concurrent_activities=worker_config.max_concurrent_activities,
            max_concurrent_workflow_tasks=worker_config.max_concurrent_workflow_tasks,
            max_concurrent_activity_tasks=worker_config.max_concurrent_activity_tasks,
            identity=worker_config.worker_identity,
            # Production optimizations
            max_heartbeat_throttle_interval_seconds=60,
            default_heartbeat_timeout_seconds=30,
        )
        
        logger.info("=" * 80)
        logger.info("🌩️  AI DUBBING PIPELINE CLOUD WORKER STARTED")
        logger.info(f"   Namespace: {worker_config.temporal_cloud_namespace}")
        logger.info(f"   Task Queue: {worker_config.task_queue}")
        logger.info(f"   Worker Identity: {worker_config.worker_identity}")
        logger.info(f"   Max Concurrent Activities: {worker_config.max_concurrent_activities}")
        logger.info(f"   Max Concurrent Workflows: {worker_config.max_concurrent_workflow_tasks}")
        logger.info(f"   Max Concurrent Activity Tasks: {worker_config.max_concurrent_activity_tasks}")
        logger.info("=" * 80)
        logger.info("🎬 Worker is ready to process audio extraction workflows...")
        logger.info("   Send SIGTERM for graceful shutdown")
        logger.info("=" * 80)
        
        # Run worker with proper signal handling
        await worker.run()
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Worker shutdown requested by user")
        return 0
    except Exception as e:
        logger.error(f"✗ Worker failed to start: {e}", exc_info=True)
        # In production, you might want to send alerts here
        return 1
    finally:
        logger.info("🔄 Worker shutting down...")


def validate_cloud_environment():
    """Validate cloud environment before starting worker."""
    
    # Check if running in expected cloud environment
    cloud_indicators = [
        os.getenv('KUBERNETES_SERVICE_HOST'),  # Kubernetes
        os.getenv('GAE_APPLICATION'),          # Google App Engine
        os.getenv('AWS_EXECUTION_ENV'),        # AWS Lambda/ECS
        os.getenv('WEBSITE_SITE_NAME'),        # Azure App Service
    ]
    
    is_cloud = any(cloud_indicators)
    if not is_cloud:
        logger.warning("⚠ Worker appears to be running outside cloud environment")
        logger.warning("  Consider using worker_local.py for local development")
    
    # Check resource limits
    try:
        import resource
        
        # Check memory limit
        memory_limit = resource.getrlimit(resource.RLIMIT_AS)
        if memory_limit[0] != resource.RLIM_INFINITY:
            logger.info(f"Memory limit: {memory_limit[0] // (1024*1024)}MB")
        
        # Check file descriptor limit
        fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        logger.info(f"File descriptor limit: {fd_limit[0]}")
        
        if fd_limit[0] < 1024:
            logger.warning("⚠ Low file descriptor limit may impact performance")
            
    except ImportError:
        # Resource module not available on all platforms
        pass


if __name__ == "__main__":
    # Validate cloud environment
    validate_cloud_environment()
    
    # Set up signal handling for graceful shutdown
    import signal
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        # Temporal worker handles graceful shutdown automatically
        
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run the worker
    sys.exit(asyncio.run(main()))