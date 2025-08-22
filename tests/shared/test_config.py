"""Tests for configuration management."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from config import DubbingConfig, get_config, reload_config


class TestDubbingConfig:
    """Test suite for DubbingConfig."""
    
    def test_from_env_with_defaults(self):
        """Test configuration loading with default values."""
        with patch.dict(os.environ, {}, clear=True):
            config = DubbingConfig.from_env()
            
            # Test defaults
            assert config.environment == "local"
            assert config.debug is False
            assert config.gcs_bucket_name == "dubbing-pipeline"
            assert config.task_queue == "dubbing-task-queue"
            assert config.audio_sample_rate == 16000
            assert config.audio_channels == 1
            assert config.max_retries == 3
    
    def test_from_env_with_custom_values(self):
        """Test configuration loading with custom environment variables."""
        test_env = {
            "GOOGLE_CLOUD_PROJECT": "test-project",
            "GCS_BUCKET_NAME": "custom-bucket",
            "NEON_DATABASE_URL": "postgresql://user:pass@host:5432/db",
            "TEMPORAL_CLOUD_NAMESPACE": "test-namespace",
            "TASK_QUEUE": "custom-queue",
            "AUDIO_SAMPLE_RATE": "22050",
            "AUDIO_CHANNELS": "2",
            "MAX_RETRIES": "5",
            "ENVIRONMENT": "production",
            "DEBUG": "true",
        }
        
        with patch.dict(os.environ, test_env, clear=True):
            config = DubbingConfig.from_env()
            
            assert config.google_cloud_project == "test-project"
            assert config.gcs_bucket_name == "custom-bucket"
            assert config.neon_database_url == "postgresql://user:pass@host:5432/db"
            assert config.temporal_cloud_namespace == "test-namespace"
            assert config.task_queue == "custom-queue"
            assert config.audio_sample_rate == 22050
            assert config.audio_channels == 2
            assert config.max_retries == 5
            assert config.environment == "production"
            assert config.debug is True
    
    def test_from_env_with_env_file(self):
        """Test configuration loading from .env file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("GOOGLE_CLOUD_PROJECT=env-file-project\n")
            f.write("GCS_BUCKET_NAME=env-file-bucket\n")
            f.write("AUDIO_SAMPLE_RATE=44100\n")
            env_file_path = f.name
        
        try:
            config = DubbingConfig.from_env(env_file_path)
            
            assert config.google_cloud_project == "env-file-project"
            assert config.gcs_bucket_name == "env-file-bucket"
            assert config.audio_sample_rate == 44100
        finally:
            os.unlink(env_file_path)
    
    def test_ensure_temp_directory(self):
        """Test temp directory creation."""
        with tempfile.TemporaryDirectory() as temp_root:
            temp_path = Path(temp_root) / "dubbing_temp"
            
            config = DubbingConfig(
                google_cloud_project="test",
                google_application_credentials=None,
                gcs_bucket_name="test",
                neon_database_url="test",
                neon_host="test",
                neon_database="test",
                neon_username="test",
                neon_password="test",
                temporal_cloud_namespace="test",
                temporal_cloud_address="test",
                temporal_cloud_api_key="test",
                task_queue="test",
                temp_storage_path=str(temp_path),
                max_file_size_mb=100,
                audio_sample_rate=16000,
                audio_channels=1,
                ffmpeg_path="/usr/local/bin/ffmpeg",
                ffprobe_path="/usr/local/bin/ffprobe",
                log_level="INFO",
                log_file="test.log",
                max_retries=3,
                retry_delay_seconds=5,
                timeout_seconds=300,
                max_concurrent_activities=10,
                max_concurrent_workflow_tasks=5,
                max_concurrent_activity_tasks=20,
                environment="test",
                debug=False,
            )
            
            # Directory should not exist initially
            assert not temp_path.exists()
            
            # Create directory
            config.ensure_temp_directory()
            assert temp_path.exists()
            assert temp_path.is_dir()
    
    def test_ensure_log_directory(self):
        """Test log directory creation."""
        with tempfile.TemporaryDirectory() as temp_root:
            log_file = Path(temp_root) / "logs" / "test.log"
            
            config = DubbingConfig(
                google_cloud_project="test",
                google_application_credentials=None,
                gcs_bucket_name="test",
                neon_database_url="test",
                neon_host="test",
                neon_database="test",
                neon_username="test",
                neon_password="test",
                temporal_cloud_namespace="test",
                temporal_cloud_address="test",
                temporal_cloud_api_key="test",
                task_queue="test",
                temp_storage_path="/tmp",
                max_file_size_mb=100,
                audio_sample_rate=16000,
                audio_channels=1,
                ffmpeg_path="/usr/local/bin/ffmpeg",
                ffprobe_path="/usr/local/bin/ffprobe",
                log_level="INFO",
                log_file=str(log_file),
                max_retries=3,
                retry_delay_seconds=5,
                timeout_seconds=300,
                max_concurrent_activities=10,
                max_concurrent_workflow_tasks=5,
                max_concurrent_activity_tasks=20,
                environment="test",
                debug=False,
            )
            
            # Log directory should not exist initially
            assert not log_file.parent.exists()
            
            # Create log directory
            config.ensure_log_directory()
            assert log_file.parent.exists()
            assert log_file.parent.is_dir()
    
    def test_validate_required_config_local(self):
        """Test validation of required configuration for local environment."""
        config = DubbingConfig(
            google_cloud_project="",  # Missing
            google_application_credentials=None,
            gcs_bucket_name="",  # Missing
            neon_database_url="",  # Missing
            neon_host="test",
            neon_database="test",
            neon_username="test",
            neon_password="test",
            temporal_cloud_namespace="test",
            temporal_cloud_address="test",
            temporal_cloud_api_key="test",
            task_queue="test",
            temp_storage_path="/tmp",
            max_file_size_mb=100,
            audio_sample_rate=16000,
            audio_channels=1,
            ffmpeg_path="/usr/local/bin/ffmpeg",
            ffprobe_path="/usr/local/bin/ffprobe",
            log_level="INFO",
            log_file="test.log",
            max_retries=3,
            retry_delay_seconds=5,
            timeout_seconds=300,
            max_concurrent_activities=10,
            max_concurrent_workflow_tasks=5,
            max_concurrent_activity_tasks=20,
            environment="local",  # Local environment
            debug=False,
        )
        
        missing = config.validate_required_config()
        expected_missing = ["GOOGLE_CLOUD_PROJECT", "GCS_BUCKET_NAME", "NEON_DATABASE_URL"]
        assert set(missing) == set(expected_missing)
    
    def test_validate_required_config_cloud(self):
        """Test validation of required configuration for cloud environment."""
        config = DubbingConfig(
            google_cloud_project="test-project",
            google_application_credentials=None,
            gcs_bucket_name="test-bucket",
            neon_database_url="test-db-url",
            neon_host="test",
            neon_database="test",
            neon_username="test",
            neon_password="test",
            temporal_cloud_namespace="",  # Missing for cloud
            temporal_cloud_address="",    # Missing for cloud
            temporal_cloud_api_key="",    # Missing for cloud
            task_queue="test",
            temp_storage_path="/tmp",
            max_file_size_mb=100,
            audio_sample_rate=16000,
            audio_channels=1,
            ffmpeg_path="/usr/local/bin/ffmpeg",
            ffprobe_path="/usr/local/bin/ffprobe",
            log_level="INFO",
            log_file="test.log",
            max_retries=3,
            retry_delay_seconds=5,
            timeout_seconds=300,
            max_concurrent_activities=10,
            max_concurrent_workflow_tasks=5,
            max_concurrent_activity_tasks=20,
            environment="cloud",  # Cloud environment
            debug=False,
        )
        
        missing = config.validate_required_config()
        expected_missing = [
            "TEMPORAL_CLOUD_NAMESPACE",
            "TEMPORAL_CLOUD_ADDRESS", 
            "TEMPORAL_CLOUD_API_KEY"
        ]
        assert set(missing) == set(expected_missing)
    
    def test_get_database_config(self):
        """Test database configuration dict generation."""
        config = DubbingConfig.from_env()
        config.neon_database_url = "postgresql://user:pass@host:5432/db"
        config.neon_host = "host"
        config.neon_database = "db"
        config.neon_username = "user"
        config.neon_password = "pass"
        
        db_config = config.get_database_config()
        
        expected = {
            "url": "postgresql://user:pass@host:5432/db",
            "host": "host",
            "database": "db", 
            "username": "user",
            "password": "pass",
        }
        assert db_config == expected
    
    def test_get_gcs_config(self):
        """Test GCS configuration dict generation."""
        config = DubbingConfig.from_env()
        config.google_cloud_project = "test-project"
        config.gcs_bucket_name = "test-bucket"
        config.google_application_credentials = "/path/to/creds.json"
        
        gcs_config = config.get_gcs_config()
        
        expected = {
            "project": "test-project",
            "bucket": "test-bucket",
            "credentials": "/path/to/creds.json",
        }
        assert gcs_config == expected
    
    def test_get_temporal_config_local(self):
        """Test Temporal configuration for local environment."""
        config = DubbingConfig.from_env()
        config.environment = "local"
        config.task_queue = "test-queue"
        
        temporal_config = config.get_temporal_config()
        
        expected = {
            "host": "localhost",
            "port": 7233,
            "task_queue": "test-queue",
        }
        assert temporal_config == expected
    
    def test_get_temporal_config_cloud(self):
        """Test Temporal configuration for cloud environment."""
        config = DubbingConfig.from_env()
        config.environment = "cloud"
        config.temporal_cloud_namespace = "test-namespace"
        config.temporal_cloud_address = "test.tmprl.cloud:7233"
        config.temporal_cloud_api_key = "test-key"
        config.task_queue = "test-queue"
        
        temporal_config = config.get_temporal_config()
        
        expected = {
            "namespace": "test-namespace",
            "address": "test.tmprl.cloud:7233",
            "api_key": "test-key",
            "task_queue": "test-queue",
        }
        assert temporal_config == expected
    
    def test_is_development(self):
        """Test development environment detection."""
        # Local environment
        config = DubbingConfig.from_env()
        config.environment = "local"
        config.debug = False
        assert config.is_development() is True
        
        # Development environment
        config.environment = "development"
        assert config.is_development() is True
        
        # Debug mode
        config.environment = "production"
        config.debug = True
        assert config.is_development() is True
        
        # Production without debug
        config.environment = "production"
        config.debug = False
        assert config.is_development() is False
    
    def test_is_production(self):
        """Test production environment detection."""
        config = DubbingConfig.from_env()
        
        # Production environment without debug
        config.environment = "production"
        config.debug = False
        assert config.is_production() is True
        
        # Production with debug (not considered production)
        config.environment = "production"
        config.debug = True
        assert config.is_production() is False
        
        # Non-production environment
        config.environment = "local"
        config.debug = False
        assert config.is_production() is False


class TestConfigCache:
    """Test configuration caching functions."""
    
    def test_get_config_caching(self):
        """Test that get_config caches the configuration."""
        # First call creates config
        config1 = get_config()
        
        # Second call should return same instance
        config2 = get_config()
        
        assert config1 is config2
    
    def test_reload_config(self):
        """Test configuration reloading."""
        # Get initial config
        config1 = get_config()
        
        # Reload config
        config2 = reload_config()
        
        # Should be different instances
        assert config1 is not config2
        
        # Subsequent calls should return new config
        config3 = get_config()
        assert config3 is config2