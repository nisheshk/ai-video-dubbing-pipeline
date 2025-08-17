"""Real scenario tests for audio extraction activities using actual GCS files."""

import os
import asyncio
import tempfile
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock
from typing import Dict, Any

import pytest
from temporalio.testing import ActivityEnvironment

from activities.audio_extraction import AudioExtractionActivities
from shared.models import AudioExtractionRequest
from config import DubbingConfig
from shared.gcs_client import AsyncGCSClient


class TestAudioExtractionActivitiesReal:
    """Test suite for AudioExtractionActivities with real GCS operations."""
    
    @pytest.fixture
    def activities(self, test_config: DubbingConfig) -> AudioExtractionActivities:
        """Create AudioExtractionActivities instance for testing."""
        with patch.object(AudioExtractionActivities, '__init__', lambda x: None):
            activities = AudioExtractionActivities()
            activities.config = test_config
            return activities
    
    @pytest.fixture
    def sample_video_file(self, test_config: DubbingConfig) -> str:
        """Create a sample video file using FFmpeg for testing."""
        temp_dir = Path(test_config.temp_storage_path)
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        video_file = temp_dir / "test_input_video.mp4"
        
        # Create a real 5-second test video with audio using FFmpeg
        # This creates a small video with a sine wave audio track
        try:
            cmd = [
                "ffmpeg", "-y",  # -y to overwrite
                "-f", "lavfi",
                "-i", "testsrc2=duration=5:size=320x240:rate=30",  # Video: 5sec test pattern
                "-f", "lavfi", 
                "-i", "sine=frequency=1000:duration=5",  # Audio: 1kHz sine wave for 5 seconds
                "-c:v", "libx264", "-preset", "ultrafast",  # Fast video encoding
                "-c:a", "aac", "-ar", "48000", "-ac", "2",  # Audio: AAC, 48kHz, stereo
                "-t", "5",  # Duration: 5 seconds
                str(video_file)
            ]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=30
            )
            
            if result.returncode != 0:
                pytest.skip(f"FFmpeg not available or failed: {result.stderr}")
                
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pytest.skip("FFmpeg not available for creating test video")
        
        if not video_file.exists():
            pytest.skip("Test video creation failed")
            
        yield str(video_file)
        
        # Cleanup
        if video_file.exists():
            video_file.unlink()
    
    @pytest.fixture
    def real_gcs_config(self, test_config: DubbingConfig) -> DubbingConfig:
        """Configure for real GCS operations."""
        # Override test config with real GCS settings
        test_config.google_cloud_project = os.getenv("GOOGLE_CLOUD_PROJECT", "")
        test_config.gcs_bucket_name = os.getenv("GCS_BUCKET_NAME", "dubbing-pipeline-test")
        test_config.google_application_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
        if not test_config.google_cloud_project:
            pytest.skip("GOOGLE_CLOUD_PROJECT environment variable required for real GCS tests")
        if not test_config.gcs_bucket_name:
            pytest.skip("GCS_BUCKET_NAME environment variable required for real GCS tests")
            
        return test_config
    
    @pytest.fixture
    async def real_gcs_client(self, real_gcs_config: DubbingConfig) -> AsyncGCSClient:
        """Create real GCS client for testing."""
        client = AsyncGCSClient(real_gcs_config)
        try:
            await client.connect()
            yield client
        except Exception as e:
            pytest.skip(f"Cannot connect to real GCS: {e}")
    
    @pytest.fixture
    async def test_video_in_gcs(
        self, 
        real_gcs_client: AsyncGCSClient, 
        sample_video_file: str,
        sample_video_id: str
    ) -> str:
        """Upload test video to GCS and return the GCS path."""
        gcs_path = f"test-videos/{sample_video_id}/input/video.mp4"
        
        try:
            # Upload test video to GCS
            await real_gcs_client.upload_file(sample_video_file, gcs_path)
            yield gcs_path
        finally:
            # Cleanup: delete test video from GCS
            try:
                await real_gcs_client.delete_file(gcs_path)
            except Exception:
                pass  # Ignore cleanup errors
    
    @pytest.fixture
    def mock_database_operations(self):
        """Mock database operations for testing."""
        mock_db = AsyncMock()
        mock_db.log_processing_step = AsyncMock()
        return mock_db
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_download_video_activity_real_gcs(
        self,
        activities: AudioExtractionActivities,
        test_video_in_gcs: str,
        sample_video_id: str,
        real_gcs_config: DubbingConfig,
        mock_database_operations: AsyncMock
    ):
        """Test video download activity with real GCS operations."""
        # Update activities config to use real GCS settings
        activities.config = real_gcs_config
        
        # Create request with real GCS path
        request = AudioExtractionRequest(
            video_id=sample_video_id,
            gcs_input_path=test_video_in_gcs,  # Real GCS path
            original_filename="test_video.mp4"
        )
        
        expected_video_path = Path(activities.config.temp_storage_path) / sample_video_id / "video.mp4"
        
        # Use real GCS client (no mocking)
        with patch('activities.audio_extraction.get_database_client') as mock_db_context:
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_operations)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            async with ActivityEnvironment() as env:
                result = await env.run(
                    activities.download_video_activity,
                    request
                )
            
            # Verify results
            assert result["success"] is True
            assert result["local_video_path"] == str(expected_video_path)
            assert Path(result["local_video_path"]).exists()
            assert result["file_size_bytes"] > 0
            assert result["download_time_seconds"] >= 0
            
            # Verify the downloaded file is a valid video
            assert Path(expected_video_path).stat().st_size > 1000  # At least 1KB
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_validate_video_activity_real_file(
        self,
        activities: AudioExtractionActivities,
        sample_video_file: str,
        sample_video_id: str,
        mock_database_operations: AsyncMock
    ):
        """Test video validation with real video file."""
        with patch('activities.audio_extraction.get_database_client') as mock_db_context:
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_operations)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            async with ActivityEnvironment() as env:
                result = await env.run(
                    activities.validate_video_activity,
                    sample_video_file,
                    sample_video_id
                )
            
            # Verify results
            assert result["success"] is True
            assert "metadata" in result
            assert result["validation_time"] >= 0
            
            metadata = result["metadata"]
            assert metadata["duration"] > 0  # Should have duration
            assert metadata["size_bytes"] > 0
            assert "video_codec" in metadata
            assert "audio_codec" in metadata
            assert metadata["video_width"] > 0
            assert metadata["video_height"] > 0
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_extract_audio_activity_real_extraction(
        self,
        activities: AudioExtractionActivities,
        sample_video_file: str,
        sample_video_id: str,
        mock_database_operations: AsyncMock
    ):
        """Test audio extraction with real FFmpeg processing."""
        # Check if FFmpeg is available
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        except (FileNotFoundError, subprocess.CalledProcessError):
            pytest.skip("FFmpeg not available for audio extraction test")
        
        with patch('activities.audio_extraction.get_database_client') as mock_db_context:
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_operations)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            async with ActivityEnvironment() as env:
                result = await env.run(
                    activities.extract_audio_activity,
                    sample_video_file,
                    sample_video_id
                )
            
            # Verify results
            assert result["success"] is True
            audio_path = result["audio_path"]
            assert Path(audio_path).exists()
            assert result["extraction_time"] >= 0
            
            # Verify extracted audio properties
            audio_metadata = result["audio_metadata"]
            assert audio_metadata["duration"] > 0
            assert audio_metadata["sample_rate"] == activities.config.audio_sample_rate  # 16kHz
            assert audio_metadata["channels"] == activities.config.audio_channels  # Mono
            assert audio_metadata["codec"] == "pcm_s16le"  # 16-bit PCM
            
            # Verify file size is reasonable
            audio_file_size = Path(audio_path).stat().st_size
            assert audio_file_size > 1000  # At least 1KB for 5 seconds of audio
    
    @pytest.mark.asyncio
    @pytest.mark.integration 
    async def test_upload_audio_activity_real_gcs(
        self,
        activities: AudioExtractionActivities,
        temp_audio_file: str,
        sample_video_id: str,
        real_gcs_config: DubbingConfig,
        real_gcs_client: AsyncGCSClient,
        mock_database_operations: AsyncMock
    ):
        """Test audio upload activity with real GCS operations."""
        # Update activities config to use real GCS settings
        activities.config = real_gcs_config
        
        expected_gcs_path = f"test-output/{sample_video_id}/audio/extracted.wav"
        expected_gcs_url = f"gs://{real_gcs_config.gcs_bucket_name}/{expected_gcs_path}"
        
        # Use real GCS client (no mocking)
        with patch('activities.audio_extraction.get_database_client') as mock_db_context:
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_operations)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            try:
                async with ActivityEnvironment() as env:
                    result = await env.run(
                        activities.upload_audio_activity,
                        temp_audio_file,
                        sample_video_id
                    )
                
                # Verify results
                assert result["success"] is True
                assert expected_gcs_path in result["gcs_audio_url"]
                assert result["upload_time"] >= 0
                assert result["file_size_bytes"] > 0
                
                # Verify file actually exists in GCS
                file_exists = await real_gcs_client.file_exists(expected_gcs_path)
                assert file_exists, f"Uploaded file not found in GCS: {expected_gcs_path}"
                
            finally:
                # Cleanup: delete uploaded test file from GCS
                try:
                    await real_gcs_client.delete_file(expected_gcs_path)
                except Exception:
                    pass  # Ignore cleanup errors
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_full_audio_extraction_pipeline_real_gcs(
        self,
        activities: AudioExtractionActivities,
        test_video_in_gcs: str,
        sample_video_id: str,
        real_gcs_config: DubbingConfig,
        real_gcs_client: AsyncGCSClient,
        mock_database_operations: AsyncMock
    ):
        """Test the complete audio extraction pipeline with real GCS operations."""
        # Check FFmpeg availability
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        except (FileNotFoundError, subprocess.CalledProcessError):
            pytest.skip("FFmpeg not available for full pipeline test")
        
        # Update activities config to use real GCS settings
        activities.config = real_gcs_config
        
        # Create request with real GCS path
        request = AudioExtractionRequest(
            video_id=sample_video_id,
            gcs_input_path=test_video_in_gcs,  # Real GCS path
            original_filename="test_video.mp4"
        )
        
        expected_audio_gcs_path = f"test-output/{sample_video_id}/audio/extracted.wav"
        
        # Use real GCS operations (no mocking)
        with patch('activities.audio_extraction.get_database_client') as mock_db_context:
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_operations)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            try:
                async with ActivityEnvironment() as env:
                    # Step 1: Download from real GCS
                    download_result = await env.run(
                        activities.download_video_activity,
                        request
                    )
                    assert download_result["success"] is True
                    video_path = download_result["local_video_path"]
                    
                    # Step 2: Validate real video file
                    validation_result = await env.run(
                        activities.validate_video_activity,
                        video_path,
                        sample_video_id
                    )
                    assert validation_result["success"] is True
                    
                    # Step 3: Extract audio with real FFmpeg
                    extraction_result = await env.run(
                        activities.extract_audio_activity,
                        video_path,
                        sample_video_id
                    )
                    assert extraction_result["success"] is True
                    audio_path = extraction_result["audio_path"]
                    
                    # Verify extracted audio file
                    assert Path(audio_path).exists()
                    assert Path(audio_path).stat().st_size > 0
                    
                    # Step 4: Upload audio to real GCS
                    upload_result = await env.run(
                        activities.upload_audio_activity,
                        audio_path,
                        sample_video_id
                    )
                    assert upload_result["success"] is True
                    
                    # Verify file exists in GCS
                    file_exists = await real_gcs_client.file_exists(expected_audio_gcs_path)
                    assert file_exists, f"Audio file not found in GCS: {expected_audio_gcs_path}"
                    
                    # Step 5: Cleanup local files
                    cleanup_result = await env.run(
                        activities.cleanup_temp_files_activity,
                        sample_video_id
                    )
                    assert cleanup_result["success"] is True
                    
                    # Verify local cleanup worked
                    assert not Path(video_path).exists()
                    assert not Path(audio_path).exists()
                    
            finally:
                # Cleanup: delete uploaded audio from GCS
                try:
                    await real_gcs_client.delete_file(expected_audio_gcs_path)
                except Exception:
                    pass  # Ignore cleanup errors
        
        # Verify all database logging calls were made
        assert mock_database_operations.log_processing_step.call_count >= 5  # At least one per step
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_extract_audio_with_invalid_video(
        self,
        activities: AudioExtractionActivities,
        sample_video_id: str,
        mock_database_operations: AsyncMock
    ):
        """Test audio extraction failure with invalid video file."""
        # Create a fake "video" file with invalid content
        temp_dir = Path(activities.config.temp_storage_path)
        temp_dir.mkdir(parents=True, exist_ok=True)
        fake_video = temp_dir / "fake_video.mp4"
        fake_video.write_text("This is not a video file")
        
        with patch('activities.audio_extraction.get_database_client') as mock_db_context:
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_operations)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            async with ActivityEnvironment() as env:
                with pytest.raises(Exception):  # Should raise an exception for invalid video
                    await env.run(
                        activities.extract_audio_activity,
                        str(fake_video),
                        sample_video_id
                    )
        
        # Cleanup
        if fake_video.exists():
            fake_video.unlink()