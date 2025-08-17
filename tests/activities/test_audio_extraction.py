"""Tests for audio extraction activities."""

import os
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock
from typing import Dict, Any

import pytest
from temporalio.testing import ActivityEnvironment

from activities.audio_extraction import AudioExtractionActivities
from shared.models import AudioExtractionRequest
from config import DubbingConfig


class TestAudioExtractionActivities:
    """Test suite for AudioExtractionActivities."""
    
    @pytest.fixture
    def activities(self, test_config: DubbingConfig) -> AudioExtractionActivities:
        """Create AudioExtractionActivities instance for testing."""
        with patch.object(AudioExtractionActivities, '__init__', lambda x: None):
            activities = AudioExtractionActivities()
            activities.config = test_config
            return activities
    
    @pytest.mark.asyncio
    async def test_download_video_activity_success(
        self,
        activities: AudioExtractionActivities,
        audio_extraction_request: AudioExtractionRequest,
        mock_gcs_client: AsyncMock,
        mock_database_client: AsyncMock
    ):
        """Test successful video download."""
        # Setup
        expected_video_path = Path(activities.config.temp_storage_path) / audio_extraction_request.video_id / "video.mp4"
        
        # Mock dependencies
        with patch('activities.audio_extraction.AsyncGCSClient', return_value=mock_gcs_client), \
             patch('activities.audio_extraction.get_database_client') as mock_db_context:
            
            # Setup async context manager for database
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_client)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Create temp file to simulate download
            expected_video_path.parent.mkdir(parents=True, exist_ok=True)
            expected_video_path.write_bytes(b"fake video content")
            
            # Run activity in test environment
            async with ActivityEnvironment() as env:
                result = await env.run(
                    activities.download_video_activity,
                    audio_extraction_request
                )
            
            # Verify results
            assert result["success"] is True
            assert result["local_video_path"] == str(expected_video_path)
            assert result["file_size_bytes"] > 0
            assert result["download_time_seconds"] >= 0
            
            # Verify mocks were called
            mock_gcs_client.connect.assert_called_once()
            mock_gcs_client.download_file.assert_called_once_with(
                audio_extraction_request.gcs_input_path,
                str(expected_video_path)
            )
            mock_database_client.log_processing_step.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_download_video_activity_failure(
        self,
        activities: AudioExtractionActivities,
        audio_extraction_request: AudioExtractionRequest,
        mock_gcs_client: AsyncMock,
        mock_database_client: AsyncMock
    ):
        """Test video download failure handling."""
        # Setup mock to raise exception
        mock_gcs_client.download_file.side_effect = Exception("Download failed")
        
        with patch('activities.audio_extraction.AsyncGCSClient', return_value=mock_gcs_client), \
             patch('activities.audio_extraction.get_database_client') as mock_db_context:
            
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_client)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Run activity and expect exception
            async with ActivityEnvironment() as env:
                with pytest.raises(Exception, match="Download failed"):
                    await env.run(
                        activities.download_video_activity,
                        audio_extraction_request
                    )
            
            # Verify error logging
            mock_database_client.log_processing_step.assert_called_once()
            call_args = mock_database_client.log_processing_step.call_args
            assert call_args[0][2] == "failed"  # status parameter
    
    @pytest.mark.asyncio
    async def test_validate_video_activity_success(
        self,
        activities: AudioExtractionActivities,
        temp_video_file: str,
        sample_video_id: str,
        sample_video_metadata: Dict[str, Any],
        mock_database_client: AsyncMock,
        mock_ffmpeg_probe
    ):
        """Test successful video validation."""
        with patch('activities.audio_extraction.ffmpeg.probe', mock_ffmpeg_probe), \
             patch('activities.audio_extraction.get_database_client') as mock_db_context:
            
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_client)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            async with ActivityEnvironment() as env:
                result = await env.run(
                    activities.validate_video_activity,
                    temp_video_file,
                    sample_video_id
                )
            
            # Verify results
            assert result["success"] is True
            assert "metadata" in result
            assert result["validation_time"] >= 0
            
            metadata = result["metadata"]
            assert metadata["duration"] == sample_video_metadata["duration"]
            assert metadata["video_codec"] == sample_video_metadata["video_codec"]
            assert metadata["audio_codec"] == sample_video_metadata["audio_codec"]
            
            # Verify database logging
            mock_database_client.log_processing_step.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_video_activity_no_audio_streams(
        self,
        activities: AudioExtractionActivities,
        temp_video_file: str,
        sample_video_id: str,
        mock_database_client: AsyncMock
    ):
        """Test video validation with no audio streams."""
        # Mock ffmpeg.probe to return video with no audio streams
        def mock_probe_no_audio(input_path: str):
            return {
                'format': {'duration': '120.0', 'size': '1000000'},
                'streams': [
                    {'codec_type': 'video', 'codec_name': 'h264', 'width': 1920, 'height': 1080}
                    # No audio stream
                ]
            }
        
        with patch('activities.audio_extraction.ffmpeg.probe', mock_probe_no_audio), \
             patch('activities.audio_extraction.get_database_client') as mock_db_context:
            
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_client)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            async with ActivityEnvironment() as env:
                with pytest.raises(ValueError, match="No audio streams found"):
                    await env.run(
                        activities.validate_video_activity,
                        temp_video_file,
                        sample_video_id
                    )
            
            # Verify error logging
            mock_database_client.log_processing_step.assert_called_once()
            call_args = mock_database_client.log_processing_step.call_args
            assert call_args[0][2] == "failed"
    
    @pytest.mark.asyncio
    async def test_extract_audio_activity_success(
        self,
        activities: AudioExtractionActivities,
        temp_video_file: str,
        sample_video_id: str,
        sample_audio_metadata: Dict[str, Any],
        mock_database_client: AsyncMock,
        mock_ffmpeg_probe
    ):
        """Test successful audio extraction."""
        audio_path = Path(activities.config.temp_storage_path) / sample_video_id / "audio.wav"
        
        # Mock asyncio.create_subprocess_exec for FFmpeg
        mock_process = AsyncMock()
        mock_process.communicate = AsyncMock(return_value=(b"", b""))
        mock_process.returncode = 0
        
        with patch('activities.audio_extraction.asyncio.create_subprocess_exec', return_value=mock_process), \
             patch('activities.audio_extraction.asyncio.wait_for', return_value=(b"", b"")), \
             patch('activities.audio_extraction.get_database_client') as mock_db_context, \
             patch.object(activities, '_validate_extracted_audio', return_value=sample_audio_metadata):
            
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_client)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Create expected audio file
            audio_path.parent.mkdir(parents=True, exist_ok=True)
            audio_path.write_bytes(b"fake audio content")
            
            async with ActivityEnvironment() as env:
                result = await env.run(
                    activities.extract_audio_activity,
                    temp_video_file,
                    sample_video_id
                )
            
            # Verify results
            assert result["success"] is True
            assert result["audio_path"] == str(audio_path)
            assert result["extraction_time"] >= 0
            assert result["audio_metadata"] == sample_audio_metadata
            
            # Verify database logging
            mock_database_client.log_processing_step.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_extract_audio_activity_ffmpeg_failure(
        self,
        activities: AudioExtractionActivities,
        temp_video_file: str,
        sample_video_id: str,
        mock_database_client: AsyncMock
    ):
        """Test audio extraction with FFmpeg failure."""
        # Mock FFmpeg failure
        mock_process = AsyncMock()
        mock_process.communicate = AsyncMock(return_value=(b"", b"FFmpeg error"))
        mock_process.returncode = 1  # Error exit code
        
        with patch('activities.audio_extraction.asyncio.create_subprocess_exec', return_value=mock_process), \
             patch('activities.audio_extraction.asyncio.wait_for', return_value=(b"", b"FFmpeg error")), \
             patch('activities.audio_extraction.get_database_client') as mock_db_context:
            
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_client)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            async with ActivityEnvironment() as env:
                with pytest.raises(RuntimeError, match="Audio extraction failed"):
                    await env.run(
                        activities.extract_audio_activity,
                        temp_video_file,
                        sample_video_id
                    )
            
            # Verify error logging
            mock_database_client.log_processing_step.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_upload_audio_activity_success(
        self,
        activities: AudioExtractionActivities,
        temp_audio_file: str,
        sample_video_id: str,
        mock_gcs_client: AsyncGCSClient,
        mock_database_client: AsyncMock
    ):
        """Test successful audio upload."""
        expected_gcs_url = f"gs://test-bucket/{sample_video_id}/audio/extracted.wav"
        mock_gcs_client.upload_file.return_value = expected_gcs_url
        
        with patch('activities.audio_extraction.AsyncGCSClient', return_value=mock_gcs_client), \
             patch('activities.audio_extraction.get_database_client') as mock_db_context:
            
            mock_db_context.return_value.__aenter__ = AsyncMock(return_value=mock_database_client)
            mock_db_context.return_value.__aexit__ = AsyncMock(return_value=None)
            
            async with ActivityEnvironment() as env:
                result = await env.run(
                    activities.upload_audio_activity,
                    temp_audio_file,
                    sample_video_id
                )
            
            # Verify results
            assert result["success"] is True
            assert result["gcs_audio_url"] == expected_gcs_url
            assert result["upload_time"] >= 0
            assert result["file_size_bytes"] > 0
            
            # Verify GCS upload was called
            mock_gcs_client.upload_file.assert_called_once()
            # Verify database logging
            mock_database_client.log_processing_step.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cleanup_temp_files_activity_success(
        self,
        activities: AudioExtractionActivities,
        sample_video_id: str
    ):
        """Test successful cleanup of temporary files."""
        # Create temp directory with files
        temp_dir = Path(activities.config.temp_storage_path) / sample_video_id
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        test_files = [
            temp_dir / "video.mp4",
            temp_dir / "audio.wav",
            temp_dir / "metadata.json"
        ]
        
        for file_path in test_files:
            file_path.write_text("test content")
        
        async with ActivityEnvironment() as env:
            result = await env.run(
                activities.cleanup_temp_files_activity,
                sample_video_id
            )
        
        # Verify results
        assert result["success"] is True
        assert "Cleanup completed" in result["message"]
        
        # Verify files were removed
        for file_path in test_files:
            assert not file_path.exists()
        assert not temp_dir.exists()
    
    @pytest.mark.asyncio
    async def test_cleanup_temp_files_activity_nonexistent_directory(
        self,
        activities: AudioExtractionActivities,
        sample_video_id: str
    ):
        """Test cleanup with nonexistent directory."""
        async with ActivityEnvironment() as env:
            result = await env.run(
                activities.cleanup_temp_files_activity,
                sample_video_id
            )
        
        # Should still succeed even if directory doesn't exist
        assert result["success"] is True
    
    @pytest.mark.asyncio
    async def test_validate_extracted_audio_success(
        self,
        activities: AudioExtractionActivities,
        temp_audio_file: str,
        sample_audio_metadata: Dict[str, Any]
    ):
        """Test audio validation success."""
        def mock_probe_audio(input_path: str):
            return {
                'format': {
                    'duration': str(sample_audio_metadata['duration']),
                    'size': str(sample_audio_metadata['size_bytes'])
                },
                'streams': [
                    {
                        'codec_type': 'audio',
                        'codec_name': sample_audio_metadata['codec'],
                        'sample_rate': str(sample_audio_metadata['sample_rate']),
                        'channels': sample_audio_metadata['channels'],
                        'bit_rate': str(sample_audio_metadata['bitrate'])
                    }
                ]
            }
        
        with patch('activities.audio_extraction.ffmpeg.probe', mock_probe_audio):
            result = await activities._validate_extracted_audio(temp_audio_file)
        
        # Verify metadata extraction
        assert result['duration'] == sample_audio_metadata['duration']
        assert result['codec'] == sample_audio_metadata['codec']
        assert result['sample_rate'] == sample_audio_metadata['sample_rate']
        assert result['channels'] == sample_audio_metadata['channels']
    
    @pytest.mark.asyncio 
    async def test_validate_extracted_audio_too_short(
        self,
        activities: AudioExtractionActivities,
        temp_audio_file: str
    ):
        """Test audio validation with too short duration."""
        def mock_probe_short_audio(input_path: str):
            return {
                'format': {'duration': '0.5', 'size': '1000'},  # Only 0.5 seconds
                'streams': [
                    {
                        'codec_type': 'audio',
                        'codec_name': 'pcm_s16le',
                        'sample_rate': '16000',
                        'channels': 1,
                        'bit_rate': '256000'
                    }
                ]
            }
        
        with patch('activities.audio_extraction.ffmpeg.probe', mock_probe_short_audio):
            with pytest.raises(ValueError, match="Extracted audio too short"):
                await activities._validate_extracted_audio(temp_audio_file)