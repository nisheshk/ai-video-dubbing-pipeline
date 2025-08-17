"""Tests for speech segmentation functionality."""

import pytest
import asyncio
import tempfile
import os
from unittest.mock import Mock, patch, AsyncMock
import torch
import numpy as np

from activities.speech_segmentation import (
    speech_segmentation_activity,
    _load_vad_model,
    _perform_vad_segmentation,
    _post_process_segments,
    _calculate_quality_metrics
)
from shared.models import SpeechSegmentationRequest, ProcessingStatus


@pytest.fixture
def sample_audio_data():
    """Create sample audio data for testing."""
    # Create 10 seconds of sample audio at 16kHz
    sample_rate = 16000
    duration = 10.0
    samples = int(duration * sample_rate)
    
    # Create audio with speech-like patterns (sine waves with varying amplitude)
    t = np.linspace(0, duration, samples)
    audio = np.sin(2 * np.pi * 440 * t) * 0.5  # 440 Hz tone
    
    # Add some silence regions
    audio[samples//4:samples//3] = 0.0  # Silence from 2.5s to 3.3s
    audio[2*samples//3:3*samples//4] = 0.0  # Silence from 6.6s to 7.5s
    
    return torch.FloatTensor(audio), sample_rate


@pytest.fixture
def segmentation_request():
    """Create test segmentation request."""
    return SpeechSegmentationRequest(
        video_id="test-video-123",
        gcs_audio_path="gs://test-bucket/test-video-123/audio/extracted.wav",
        vad_threshold=0.5,
        min_speech_duration_ms=1000,
        max_segment_duration_s=30,
        min_silence_gap_ms=500,
        speech_padding_ms=50
    )


class TestSpeechSegmentation:
    """Test cases for speech segmentation."""
    
    @pytest.mark.asyncio
    async def test_vad_model_loading(self):
        """Test VAD model loading and caching."""
        with patch('torch.hub.load') as mock_load:
            mock_model = Mock()
            mock_utils = [Mock(), Mock(), Mock()]
            mock_load.return_value = (mock_model, mock_utils)
            
            # First call should load model
            model1, utils1 = _load_vad_model()
            assert mock_load.called
            assert model1 == mock_model
            assert utils1 == mock_utils
            
            # Reset mock to verify caching
            mock_load.reset_mock()
            
            # Second call should use cached model
            model2, utils2 = _load_vad_model()
            assert not mock_load.called
            assert model2 == mock_model
            assert utils2 == mock_utils
    
    @pytest.mark.asyncio
    async def test_vad_segmentation(self, sample_audio_data, segmentation_request):
        """Test VAD segmentation functionality."""
        audio_data, sample_rate = sample_audio_data
        
        with patch('activities.speech_segmentation._load_vad_model') as mock_load_vad:
            # Mock VAD model and utils
            mock_model = Mock()
            mock_get_timestamps = Mock()
            mock_utils = [mock_get_timestamps, Mock(), Mock()]
            mock_load_vad.return_value = (mock_model, mock_utils)
            
            # Mock VAD timestamps (simulate detecting 2 speech segments)
            mock_get_timestamps.return_value = [
                {'start': 0, 'end': 32000},      # 0-2s speech
                {'start': 64000, 'end': 96000}   # 4-6s speech
            ]
            
            # Mock confidence estimation
            with patch('activities.speech_segmentation._estimate_speech_confidence', return_value=0.85):
                segments = await _perform_vad_segmentation(
                    audio_data, sample_rate, segmentation_request
                )
            
            assert len(segments) == 2
            assert segments[0]['start_time'] == 0.0
            assert segments[0]['end_time'] == 2.0
            assert segments[0]['duration'] == 2.0
            assert segments[0]['confidence_score'] == 0.85
            
            assert segments[1]['start_time'] == 4.0
            assert segments[1]['end_time'] == 6.0
            assert segments[1]['duration'] == 2.0
    
    @pytest.mark.asyncio
    async def test_segment_post_processing(self, segmentation_request):
        """Test segment post-processing (merge, split, validate)."""
        # Create test segments with some that should be merged
        segments = [
            {'start_time': 0.0, 'end_time': 2.0, 'duration': 2.0, 'confidence_score': 0.8},
            {'start_time': 2.3, 'end_time': 4.0, 'duration': 1.7, 'confidence_score': 0.9},  # Close gap, should merge
            {'start_time': 6.0, 'end_time': 38.0, 'duration': 32.0, 'confidence_score': 0.7},  # Too long, should split
        ]
        
        processed = await _post_process_segments(segments, segmentation_request)
        
        # First two segments should be merged
        assert processed[0]['start_time'] == 0.0
        assert processed[0]['end_time'] == 4.0
        assert processed[0]['duration'] == 4.0
        
        # Long segment should be split into multiple segments
        long_segments = [seg for seg in processed if seg['start_time'] >= 6.0]
        assert len(long_segments) > 1
        
        # All segments should have IDs and proper indexing
        for i, segment in enumerate(processed):
            assert 'id' in segment
            assert segment['segment_index'] == i
    
    @pytest.mark.asyncio
    async def test_quality_metrics_calculation(self, sample_audio_data):
        """Test quality metrics calculation."""
        audio_data, sample_rate = sample_audio_data
        
        segments = [
            {'duration': 2.0, 'confidence_score': 0.8},
            {'duration': 1.5, 'confidence_score': 0.9},
            {'duration': 3.0, 'confidence_score': 0.7}
        ]
        
        metrics = _calculate_quality_metrics(segments, audio_data, sample_rate)
        
        assert 'speech_to_silence_ratio' in metrics
        assert 'avg_segment_duration' in metrics
        assert 'confidence_scores' in metrics
        
        expected_total_speech = 6.5  # 2.0 + 1.5 + 3.0
        expected_avg_duration = expected_total_speech / 3
        
        assert metrics['avg_segment_duration'] == expected_avg_duration
        assert metrics['confidence_scores'] == [0.8, 0.9, 0.7]
        assert 0 <= metrics['speech_to_silence_ratio'] <= 1
    
    @pytest.mark.asyncio
    async def test_empty_segments_handling(self):
        """Test handling of empty segment lists."""
        sample_rate = 16000
        audio_data = torch.zeros(sample_rate)  # 1 second of silence
        
        metrics = _calculate_quality_metrics([], audio_data, sample_rate)
        
        assert metrics['speech_to_silence_ratio'] == 0.0
        assert metrics['avg_segment_duration'] == 0.0
        assert metrics['confidence_scores'] == []
    
    @pytest.mark.asyncio 
    async def test_segmentation_request_validation(self):
        """Test segmentation request parameter validation."""
        # Valid request
        request = SpeechSegmentationRequest(
            video_id="test-123",
            gcs_audio_path="gs://bucket/path/audio.wav"
        )
        assert request.vad_threshold == 0.5  # Default value
        assert request.min_speech_duration_ms == 1000  # Default value
        
        # Custom parameters
        custom_request = SpeechSegmentationRequest(
            video_id="test-456",
            gcs_audio_path="gs://bucket/path/audio.wav",
            vad_threshold=0.7,
            min_speech_duration_ms=500,
            max_segment_duration_s=20
        )
        assert custom_request.vad_threshold == 0.7
        assert custom_request.min_speech_duration_ms == 500
        assert custom_request.max_segment_duration_s == 20


class TestSpeechSegmentationIntegration:
    """Integration tests for complete speech segmentation workflow."""
    
    @pytest.mark.asyncio
    async def test_full_segmentation_workflow_mock(self, segmentation_request):
        """Test complete segmentation workflow with mocked dependencies."""
        with patch('activities.speech_segmentation._download_audio_from_gcs') as mock_download, \
             patch('activities.speech_segmentation._load_and_validate_audio') as mock_load, \
             patch('activities.speech_segmentation._perform_vad_segmentation') as mock_vad, \
             patch('activities.speech_segmentation._create_segment_audio_files') as mock_create, \
             patch('activities.speech_segmentation._upload_segments_to_gcs') as mock_upload, \
             patch('activities.speech_segmentation._cleanup_local_files') as mock_cleanup, \
             patch('activities.speech_segmentation.get_database_client') as mock_db:
            
            # Mock all dependencies
            mock_download.return_value = "/tmp/test_audio.wav"
            mock_load.return_value = (torch.zeros(16000), 16000)
            mock_vad.return_value = [
                {'segment_index': 0, 'start_time': 0.0, 'end_time': 2.0, 'duration': 2.0, 'confidence_score': 0.8}
            ]
            mock_create.return_value = ["/tmp/segment_000.wav"]
            mock_upload.return_value = ("gs://bucket/manifest.json", "gs://bucket/segments/")
            
            # Mock database client
            mock_db_client = AsyncMock()
            mock_db_client.create_segments.return_value = ["seg_id_123"]
            mock_db.__aenter__.return_value = mock_db_client
            
            # Run segmentation activity
            result = await speech_segmentation_activity(segmentation_request)
            
            # Verify successful result
            assert result.success
            assert result.total_segments == 1
            assert result.video_id == segmentation_request.video_id
            assert result.gcs_manifest_path == "gs://bucket/manifest.json"
            
            # Verify all functions were called
            mock_download.assert_called_once()
            mock_load.assert_called_once()
            mock_vad.assert_called_once()
            mock_create.assert_called_once()
            mock_upload.assert_called_once()
            mock_cleanup.assert_called_once()


# Test fixtures and utilities

@pytest.fixture
def temp_audio_file():
    """Create temporary audio file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
        # Create minimal WAV file (just headers, no actual audio data for testing)
        tmp.write(b'RIFF' + b'\x00' * 44)  # Minimal WAV header
        tmp.flush()
        yield tmp.name
    
    # Cleanup
    if os.path.exists(tmp.name):
        os.remove(tmp.name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])