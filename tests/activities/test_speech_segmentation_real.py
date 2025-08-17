"""Real integration tests for speech segmentation with actual GCS audio file."""

import pytest
import asyncio
import os
import tempfile
from datetime import datetime

from activities.speech_segmentation import speech_segmentation_activity
from shared.models import SpeechSegmentationRequest
from config import DubbingConfig


class TestSpeechSegmentationReal:
    """Real integration tests using actual GCS audio file."""
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_real_speech_segmentation(self):
        """Test speech segmentation with real audio file from GCS."""
        
        print("\n🎵 Starting real speech segmentation test...")
        print("=" * 60)
        
        # Use the actual GCS path provided
        video_id = "d82c5c2a-3099-476d-937b-caf03bcc4043"
        gcs_audio_path = f"https://storage.googleapis.com/dubbing-pipeline/{video_id}/audio/extracted.wav"
        
        print(f"📁 Video ID: {video_id}")
        print(f"🔗 Audio URL: {gcs_audio_path}")
        print()
        
        # Create segmentation request
        request = SpeechSegmentationRequest(
            video_id=video_id,
            gcs_audio_path=gcs_audio_path,
            vad_threshold=0.5,
            min_speech_duration_ms=1000,
            max_segment_duration_s=30,
            min_silence_gap_ms=500,
            speech_padding_ms=50
        )
        
        print("⚙️ VAD Configuration:")
        print(f"   - Threshold: {request.vad_threshold}")
        print(f"   - Min speech duration: {request.min_speech_duration_ms}ms")
        print(f"   - Max segment duration: {request.max_segment_duration_s}s")
        print(f"   - Min silence gap: {request.min_silence_gap_ms}ms")
        print(f"   - Speech padding: {request.speech_padding_ms}ms")
        print()
        
        # Run the actual speech segmentation
        start_time = datetime.now()
        print(f"🚀 Starting segmentation at {start_time.strftime('%H:%M:%S')}")
        
        try:
            result = await speech_segmentation_activity(request)
            end_time = datetime.now()
            
            print()
            print("=" * 60)
            print("📊 SEGMENTATION RESULTS")
            print("=" * 60)
            
            if result.success:
                print("✅ Status: SUCCESS")
                print()
                
                # Basic metrics
                print("📈 Basic Metrics:")
                print(f"   - Total segments: {result.total_segments}")
                print(f"   - Total speech duration: {result.total_speech_duration:.2f}s")
                print(f"   - Processing time: {result.processing_time_seconds:.2f}s")
                print(f"   - Speech/silence ratio: {result.speech_to_silence_ratio:.3f}")
                print(f"   - Average segment duration: {result.avg_segment_duration:.2f}s")
                print()
                
                # Storage locations
                print("💾 Output Storage:")
                print(f"   - Manifest path: {result.gcs_manifest_path}")
                print(f"   - Segments folder: {result.gcs_segments_folder}")
                print()
                
                # Segment details
                if result.segments and len(result.segments) > 0:
                    print("🎞️ Segment Details:")
                    for i, segment in enumerate(result.segments[:10]):  # Show first 10
                        print(f"   Segment {i:2d}: {segment['start_time']:6.2f}s - {segment['end_time']:6.2f}s "
                              f"({segment['duration']:5.2f}s, confidence: {segment['confidence_score']:.3f})")
                        if 'gcs_audio_path' in segment:
                            print(f"              🔗 {segment['gcs_audio_path']}")
                    
                    if len(result.segments) > 10:
                        print(f"   ... and {len(result.segments) - 10} more segments")
                    print()
                
                # Quality metrics
                if result.confidence_scores:
                    avg_confidence = sum(result.confidence_scores) / len(result.confidence_scores)
                    min_confidence = min(result.confidence_scores)
                    max_confidence = max(result.confidence_scores)
                    
                    print("📊 Confidence Statistics:")
                    print(f"   - Average confidence: {avg_confidence:.3f}")
                    print(f"   - Min confidence: {min_confidence:.3f}")
                    print(f"   - Max confidence: {max_confidence:.3f}")
                    print()
                
                # Performance metrics
                print("⚡ Performance:")
                total_duration = (end_time - start_time).total_seconds()
                if result.total_speech_duration > 0:
                    processing_speed = result.total_speech_duration / result.processing_time_seconds
                    print(f"   - Wall clock time: {total_duration:.2f}s")
                    print(f"   - Processing speed: {processing_speed:.2f}x real-time")
                print()
                
                # Verification
                assert result.total_segments > 0, "Should detect at least one speech segment"
                assert result.total_speech_duration > 0, "Should have non-zero speech duration"
                assert result.speech_to_silence_ratio > 0, "Should have positive speech ratio"
                assert result.gcs_manifest_path.startswith("gs://"), "Should have valid GCS manifest path"
                assert result.gcs_segments_folder.startswith("gs://"), "Should have valid GCS segments folder"
                
                print("✅ All assertions passed!")
                
            else:
                print("❌ Status: FAILED")
                print(f"   Error: {result.error_message}")
                
                # Still run basic assertions for failed cases
                assert not result.success, "Failed result should have success=False"
                pytest.fail(f"Speech segmentation failed: {result.error_message}")
                
        except Exception as e:
            end_time = datetime.now()
            print()
            print("❌ SEGMENTATION FAILED")
            print("=" * 60)
            print(f"Error: {str(e)}")
            print(f"Processing time: {(end_time - start_time).total_seconds():.2f}s")
            raise
        
        finally:
            print()
            print("=" * 60)
            print("🏁 Test completed")
            print("=" * 60)
    
    @pytest.mark.asyncio 
    @pytest.mark.integration
    async def test_config_validation(self):
        """Test that configuration is properly loaded for real testing."""
        
        print("\n🔧 Testing configuration...")
        
        try:
            config = DubbingConfig.from_env()
            
            print(f"   - GCS Bucket: {config.gcs_bucket_name}")
            print(f"   - Environment: {config.environment}")
            print(f"   - Debug mode: {config.debug}")
            
            # Validate required fields for real testing
            missing_fields = config.validate_required_config()
            if missing_fields:
                print(f"   ⚠️ Missing required config: {', '.join(missing_fields)}")
                pytest.skip(f"Missing required configuration: {', '.join(missing_fields)}")
            else:
                print("   ✅ All required configuration present")
                
        except Exception as e:
            print(f"   ❌ Configuration error: {e}")
            pytest.skip(f"Configuration error: {e}")


if __name__ == "__main__":
    # Run the real test directly
    import asyncio
    
    async def run_real_test():
        test_instance = TestSpeechSegmentationReal()
        await test_instance.test_config_validation()
        await test_instance.test_real_speech_segmentation()
    
    asyncio.run(run_real_test())