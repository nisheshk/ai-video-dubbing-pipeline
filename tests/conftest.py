"""Test configuration and fixtures for AI dubbing pipeline tests."""

import os
import asyncio
import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from temporalio.testing import WorkflowEnvironment

from config import DubbingConfig
from shared.models import AudioExtractionRequest
from shared.database import AsyncDatabaseClient
from shared.gcs_client import AsyncGCSClient


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for the entire test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_config() -> DubbingConfig:
    """Create test configuration."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config = DubbingConfig(
            # Google Cloud Storage
            google_cloud_project="test-project",
            google_application_credentials=None,
            gcs_bucket_name="test-bucket",
            
            # Database - Use real PostgreSQL for integration tests
            neon_database_url="postgresql://postgres:Test%40123@localhost:5432/postgres",
            neon_host="localhost",
            neon_database="postgres",
            neon_username="postgres", 
            neon_password="Test@123",
            
            # Temporal
            temporal_cloud_namespace="test-namespace",
            temporal_cloud_address="test.tmprl.cloud:7233",
            temporal_cloud_api_key="test-key",
            task_queue="test-queue",
            
            # Processing
            temp_storage_path=temp_dir,
            max_file_size_mb=100,
            audio_sample_rate=16000,
            audio_channels=1,
            
            # FFmpeg
            ffmpeg_path="/usr/bin/ffmpeg",
            ffprobe_path="/usr/bin/ffprobe",
            
            # Logging
            log_level="DEBUG",
            log_file=f"{temp_dir}/test.log",
            
            # Error handling
            max_retries=2,
            retry_delay_seconds=1,
            timeout_seconds=30,
            
            # Worker scaling
            max_concurrent_activities=5,
            max_concurrent_workflow_tasks=2,
            max_concurrent_activity_tasks=10,
            
            # Environment
            environment="test",
            debug=True,
        )
        
        # Ensure directories exist
        config.ensure_temp_directory()
        config.ensure_log_directory()
        
        yield config


@pytest.fixture
def sample_video_id() -> str:
    """Generate a sample video ID for testing."""
    return str(uuid.uuid4())


@pytest.fixture
def audio_extraction_request(sample_video_id: str) -> AudioExtractionRequest:
    """Create a sample audio extraction request."""
    return AudioExtractionRequest(
        video_id=sample_video_id,
        gcs_input_path=f"videos/{sample_video_id}/input/video.mp4",
        original_filename="test_video.mp4",
        source_language="en",
        target_language="es"
    )


@pytest.fixture
def mock_gcs_client() -> AsyncMock:
    """Create a mock GCS client."""
    mock_client = AsyncMock(spec=AsyncGCSClient)
    
    # Mock successful operations
    mock_client.connect = AsyncMock()
    mock_client.download_file = AsyncMock()
    mock_client.upload_file = AsyncMock(return_value="gs://test-bucket/test/path.wav")
    mock_client.file_exists = AsyncMock(return_value=True)
    mock_client.get_file_info = AsyncMock(return_value={
        "name": "test.mp4",
        "size": 1000000,  # 1MB
        "size_mb": 1.0,
        "content_type": "video/mp4"
    })
    
    return mock_client


@pytest.fixture
def mock_database_client() -> AsyncMock:
    """Create a mock database client."""
    mock_db = AsyncMock(spec=AsyncDatabaseClient)
    
    # Mock successful operations
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()
    mock_db.create_schema = AsyncMock()
    mock_db.create_video_record = AsyncMock()
    mock_db.update_video_status = AsyncMock()
    mock_db.log_processing_step = AsyncMock()
    mock_db.get_video_info = AsyncMock(return_value={
        "id": "test-video-id",
        "status": "processing",
        "created_at": "2023-01-01T00:00:00Z"
    })
    
    return mock_db


@pytest.fixture
def sample_video_metadata() -> Dict[str, Any]:
    """Sample video metadata for testing."""
    return {
        'duration': 120.5,
        'size_bytes': 10485760,  # 10MB
        'format_name': 'mov,mp4,m4a,3gp,3g2,mj2',
        'video_codec': 'h264',
        'video_width': 1920,
        'video_height': 1080,
        'video_fps': 30.0,
        'audio_codec': 'aac',
        'audio_sample_rate': 48000,
        'audio_channels': 2,
    }


@pytest.fixture
def sample_audio_metadata() -> Dict[str, Any]:
    """Sample extracted audio metadata for testing."""
    return {
        'duration': 120.5,
        'size_bytes': 3865600,  # 3.7MB
        'codec': 'pcm_s16le',
        'sample_rate': 16000,
        'channels': 1,
        'bitrate': 256000,
    }


@pytest_asyncio.fixture  
async def temp_video_file(test_config: DubbingConfig) -> AsyncGenerator[str, None]:
    """Create a temporary test video file."""
    temp_dir = Path(test_config.temp_storage_path)
    temp_video = temp_dir / "test_video.mp4"
    
    # Create a minimal valid MP4 file (just header, not actually playable)
    # This is sufficient for testing file operations
    with open(temp_video, "wb") as f:
        # MP4 file signature and basic structure
        f.write(b'\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2avc1mp41')
        f.write(b'\x00' * 1000)  # Pad to make it a reasonable size
    
    yield str(temp_video)
    
    # Cleanup
    if temp_video.exists():
        temp_video.unlink()


@pytest_asyncio.fixture
async def temp_audio_file(test_config: DubbingConfig) -> AsyncGenerator[str, None]:
    """Create a temporary test audio file."""
    temp_dir = Path(test_config.temp_storage_path)
    temp_audio = temp_dir / "test_audio.wav"
    
    # Create a minimal valid WAV file header
    with open(temp_audio, "wb") as f:
        # WAV file header
        f.write(b'RIFF')
        f.write((1000).to_bytes(4, 'little'))  # File size
        f.write(b'WAVE')
        f.write(b'fmt ')
        f.write((16).to_bytes(4, 'little'))  # Format chunk size
        f.write((1).to_bytes(2, 'little'))   # Audio format (PCM)
        f.write((1).to_bytes(2, 'little'))   # Channels
        f.write((16000).to_bytes(4, 'little'))  # Sample rate
        f.write((32000).to_bytes(4, 'little'))  # Byte rate
        f.write((2).to_bytes(2, 'little'))   # Block align
        f.write((16).to_bytes(2, 'little'))  # Bits per sample
        f.write(b'data')
        f.write((968).to_bytes(4, 'little')) # Data chunk size
        f.write(b'\x00' * 968)  # Audio data (silence)
    
    yield str(temp_audio)
    
    # Cleanup
    if temp_audio.exists():
        temp_audio.unlink()


@pytest_asyncio.fixture
async def temporal_env():
    """Create Temporal test environment."""
    async with WorkflowEnvironment.start_time_skipping() as env:
        yield env


@pytest.fixture
def mock_ffmpeg_probe(sample_video_metadata: Dict[str, Any]):
    """Mock ffmpeg.probe function."""
    def _mock_probe(input_path: str):
        return {
            'format': {
                'duration': str(sample_video_metadata['duration']),
                'size': str(sample_video_metadata['size_bytes']),
                'format_name': sample_video_metadata['format_name']
            },
            'streams': [
                {
                    'codec_type': 'video',
                    'codec_name': sample_video_metadata['video_codec'],
                    'width': sample_video_metadata['video_width'],
                    'height': sample_video_metadata['video_height'],
                    'r_frame_rate': f"{sample_video_metadata['video_fps']}/1"
                },
                {
                    'codec_type': 'audio',
                    'codec_name': sample_video_metadata['audio_codec'],
                    'sample_rate': str(sample_video_metadata['audio_sample_rate']),
                    'channels': sample_video_metadata['audio_channels']
                }
            ]
        }
    
    return _mock_probe


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Set up test environment variables."""
    test_env_vars = {
        'ENVIRONMENT': 'test',
        'DEBUG': 'true',
        'LOG_LEVEL': 'DEBUG',
        'MAX_RETRIES': '2',
        'TIMEOUT_SECONDS': '30',
    }
    
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)


class AsyncContextManager:
    """Helper for creating async context managers in tests."""
    
    def __init__(self, return_value=None):
        self.return_value = return_value
    
    async def __aenter__(self):
        return self.return_value
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def async_context_manager():
    """Helper fixture for async context managers."""
    return AsyncContextManager