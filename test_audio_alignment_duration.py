#!/usr/bin/env python3
"""
Test script to verify audio alignment ensures exact duration matching between 
voice synthesis segments and original segment timing.

Tests video: 0aee4d10-1e97-41eb-87dc-f391519d4ca3
Outputs all results to GCS test_output/ folder.
"""

import asyncio
import logging
import tempfile
import subprocess
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
from datetime import datetime

from config import DubbingConfig
from shared.database import get_database_client
from shared.gcs_client import AsyncGCSClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_audio_duration_ffprobe(audio_path: str, config: DubbingConfig) -> float:
    """Get precise audio duration using ffprobe."""
    try:
        cmd = [
            config.ffprobe_path, "-v", "quiet",
            "-show_entries", "format=duration",
            "-of", "csv=p=0",
            str(audio_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        duration = float(result.stdout.strip())
        return duration
        
    except Exception as e:
        logger.error(f"Failed to get duration from {audio_path}: {e}")
        return 0.0


async def download_audio_from_gcs(gcs_path: str, local_path: str, config: DubbingConfig) -> bool:
    """Download audio file from GCS to local path."""
    try:
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        
        blob_path = gcs_path.replace(f"gs://{config.gcs_bucket_name}/", "")
        await gcs_client.download_file(blob_path, local_path)
        return True
        
    except Exception as e:
        logger.error(f"Failed to download {gcs_path}: {e}")
        return False


async def align_audio_to_target_duration(
    input_audio_path: str,
    target_duration: float,
    output_audio_path: str,
    config: DubbingConfig
) -> Dict[str, Any]:
    """
    Align audio to exact target duration using FFmpeg time-stretching.
    This is the core logic being tested.
    """
    try:
        # Get current duration
        current_duration = await get_audio_duration_ffprobe(input_audio_path, config)
        if current_duration <= 0:
            raise ValueError(f"Invalid current duration: {current_duration}")
        
        # Calculate speed ratio for atempo (original/target = speed multiplier)
        speed_ratio = current_duration / target_duration
        logger.info(f"Aligning audio: {current_duration:.3f}s → {target_duration:.3f}s (speed: {speed_ratio:.3f}x)")
        
        # Build FFmpeg filter chain for time-stretching
        atempo_filters = []
        remaining_ratio = speed_ratio
        
        # Handle ratios outside 0.5-2.0 with chained atempo filters
        while remaining_ratio > 2.0:
            atempo_filters.append("atempo=2.0")
            remaining_ratio /= 2.0
        while remaining_ratio < 0.5:
            atempo_filters.append("atempo=0.5")
            remaining_ratio *= 2.0
        
        # Add final atempo filter for remaining ratio (with precision tolerance)
        if abs(remaining_ratio - 1.0) > 0.001:
            atempo_filters.append(f"atempo={remaining_ratio:.6f}")
        
        # Use only tempo filters to change speed - NO trimming/cutting
        if not atempo_filters:
            # No tempo change needed - audio already matches target duration
            filter_chain = "acopy"  # Just copy without changes
        else:
            # Apply tempo change to speed up or slow down entire audio content
            filter_chain = ",".join(atempo_filters)
        
        # Execute FFmpeg alignment
        ffmpeg_cmd = [
            "ffmpeg", "-y",
            "-i", str(input_audio_path),
            "-filter:a", filter_chain,
            "-acodec", "pcm_s16le",
            "-ar", "44100",
            str(output_audio_path)
        ]
        
        logger.debug(f"FFmpeg command: {' '.join(ffmpeg_cmd)}")
        
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
        
        if process.returncode != 0:
            raise RuntimeError(f"FFmpeg alignment failed: {stderr.decode()}")
        
        # Measure final duration
        final_duration = await get_audio_duration_ffprobe(output_audio_path, config)
        timing_accuracy_ms = abs(final_duration - target_duration) * 1000
        
        return {
            'success': True,
            'original_duration': current_duration,
            'target_duration': target_duration,
            'final_duration': final_duration,
            'speed_ratio': speed_ratio,
            'timing_accuracy_ms': timing_accuracy_ms,
            'filter_chain': filter_chain,
            'ffmpeg_command': ' '.join(ffmpeg_cmd)
        }
        
    except Exception as e:
        logger.error(f"Audio alignment failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'original_duration': current_duration if 'current_duration' in locals() else 0,
            'target_duration': target_duration
        }


async def test_audio_alignment_for_video(
    video_id: str = "0aee4d10-1e97-41eb-87dc-f391519d4ca3"
) -> Dict[str, Any]:
    """Test audio alignment duration matching for specified video."""
    logger.info(f"🧪 Testing audio alignment for video {video_id}")
    
    config = DubbingConfig.from_env()
    test_results = {
        'video_id': video_id,
        'test_timestamp': datetime.now().isoformat(),
        'segments_tested': 0,
        'successful_alignments': 0,
        'failed_alignments': 0,
        'segment_results': [],
        'summary': {}
    }
    
    try:
        # Step 1: Query database for segment timing and voice synthesis data
        logger.info("📊 Querying database for segment and voice synthesis data...")
        
        async with get_database_client(config) as db_client:
            # Get original segments with timing
            segments_query = """
            SELECT id, segment_index, start_time, end_time, 
                   (end_time - start_time) as target_duration
            FROM segments 
            WHERE video_id = $1 
            ORDER BY segment_index
            """
            
            async with db_client.pool.acquire() as conn:
                segment_rows = await conn.fetch(segments_query, video_id)
                segments = [dict(row) for row in segment_rows]
            
            # Get voice synthesis data
            voice_query = """
            SELECT segment_id, segment_index, gcs_audio_path, 
                   audio_duration_seconds, status
            FROM voice_synthesis 
            WHERE video_id = $1 AND status = 'completed' 
              AND gcs_audio_path IS NOT NULL
            ORDER BY segment_index
            """
            
            async with db_client.pool.acquire() as conn:
                voice_rows = await conn.fetch(voice_query, video_id)
                voice_data = [dict(row) for row in voice_rows]
        
        if not segments:
            raise ValueError(f"No segments found for video {video_id}")
        if not voice_data:
            raise ValueError(f"No voice synthesis data found for video {video_id}")
        
        logger.info(f"Found {len(segments)} segments and {len(voice_data)} voice synthesis files")
        
        # Step 2: Test alignment for each segment
        logger.info("\n📏 Testing audio alignment for segments:")
        logger.info("=" * 90)
        logger.info(f"{'Seg':<3} {'Target':<8} {'Original':<8} {'Aligned':<8} {'Accuracy':<10} {'Status':<8} {'Filter'}")
        logger.info("-" * 90)
        
        # Initialize GCS client for uploads
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Test first 5 segments to avoid overwhelming the test
            segments_to_test = segments[:5] if len(segments) > 5 else segments
            
            for segment in segments_to_test:
                segment_id = str(segment['id'])
                segment_index = segment['segment_index']
                target_duration = float(segment['target_duration'])
                
                # Find matching voice synthesis
                voice_match = next(
                    (v for v in voice_data if str(v['segment_id']) == segment_id), 
                    None
                )
                
                if not voice_match:
                    logger.warning(f"No voice synthesis found for segment {segment_index}")
                    continue
                
                try:
                    # Download synthesized audio
                    input_audio_path = temp_path / f"segment_{segment_index:03d}_synthesized.mp3"
                    if not await download_audio_from_gcs(voice_match['gcs_audio_path'], str(input_audio_path), config):
                        raise RuntimeError("Failed to download synthesized audio")
                    
                    # Apply alignment
                    aligned_audio_path = temp_path / f"segment_{segment_index:03d}_aligned.wav"
                    alignment_result = await align_audio_to_target_duration(
                        str(input_audio_path), target_duration, str(aligned_audio_path), config
                    )
                    
                    if alignment_result['success']:
                        # Upload aligned audio to test_output folder
                        output_gcs_path = f"test_output/audio_alignment_test/segment_{segment_index:03d}_aligned.wav"
                        await gcs_client.upload_file(str(aligned_audio_path), output_gcs_path)
                        alignment_result['aligned_gcs_path'] = f"gs://{config.gcs_bucket_name}/{output_gcs_path}"
                        
                        # Log results
                        final_duration = alignment_result['final_duration']
                        accuracy_ms = alignment_result['timing_accuracy_ms']
                        status = "✅ Good" if accuracy_ms < 10 else "⚠️ OK" if accuracy_ms < 50 else "❌ Poor"
                        filter_summary = alignment_result['filter_chain'][:30] + "..." if len(alignment_result['filter_chain']) > 30 else alignment_result['filter_chain']
                        
                        logger.info(f"{segment_index:>3} {target_duration:>8.3f} {alignment_result['original_duration']:>8.3f} {final_duration:>8.3f} {accuracy_ms:>10.1f} {status:<8} {filter_summary}")
                        
                        test_results['successful_alignments'] += 1
                    else:
                        logger.error(f"{segment_index:>3} FAILED: {alignment_result.get('error', 'Unknown error')}")
                        test_results['failed_alignments'] += 1
                    
                    # Store detailed results
                    segment_result = {
                        'segment_index': segment_index,
                        'segment_id': segment_id,
                        'target_duration': target_duration,
                        'voice_synthesis_path': voice_match['gcs_audio_path'],
                        'alignment_result': alignment_result
                    }
                    test_results['segment_results'].append(segment_result)
                    test_results['segments_tested'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing segment {segment_index}: {e}")
                    test_results['failed_alignments'] += 1
                    test_results['segment_results'].append({
                        'segment_index': segment_index,
                        'segment_id': segment_id,
                        'target_duration': target_duration,
                        'error': str(e)
                    })
        
        # Step 3: Generate summary
        successful_results = [r for r in test_results['segment_results'] if r.get('alignment_result', {}).get('success', False)]
        if successful_results:
            accuracies = [r['alignment_result']['timing_accuracy_ms'] for r in successful_results]
            avg_accuracy = sum(accuracies) / len(accuracies)
            max_accuracy = max(accuracies)
            min_accuracy = min(accuracies)
            
            excellent_count = len([a for a in accuracies if a < 10])
            good_count = len([a for a in accuracies if 10 <= a < 50])
            poor_count = len([a for a in accuracies if a >= 50])
        else:
            avg_accuracy = max_accuracy = min_accuracy = 0
            excellent_count = good_count = poor_count = 0
        
        test_results['summary'] = {
            'total_segments_found': len(segments),
            'segments_tested': test_results['segments_tested'],
            'successful_alignments': test_results['successful_alignments'],
            'failed_alignments': test_results['failed_alignments'],
            'success_rate': test_results['successful_alignments'] / max(test_results['segments_tested'], 1),
            'average_timing_accuracy_ms': avg_accuracy,
            'best_timing_accuracy_ms': min_accuracy,
            'worst_timing_accuracy_ms': max_accuracy,
            'excellent_alignments': excellent_count,  # <10ms
            'good_alignments': good_count,            # 10-50ms  
            'poor_alignments': poor_count             # >50ms
        }
        
        # Step 4: Upload results to GCS test_output folder
        logger.info("\n📤 Uploading test results to GCS...")
        
        # Upload JSON results
        results_filename = f"audio_alignment_test_results_{video_id}.json"
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as temp_file:
            json.dump(test_results, temp_file.file, indent=2, default=str)
            temp_file.flush()
            
            results_gcs_path = f"test_output/{results_filename}"
            await gcs_client.upload_file(temp_file.name, results_gcs_path)
            logger.info(f"📄 Results uploaded: gs://{config.gcs_bucket_name}/{results_gcs_path}")
        
        # Upload summary report
        summary_content = f"""# Audio Alignment Test Summary

Video ID: {video_id}
Test Date: {test_results['test_timestamp']}

## Results Overview
- Total segments found: {test_results['summary']['total_segments_found']}
- Segments tested: {test_results['summary']['segments_tested']}
- Successful alignments: {test_results['summary']['successful_alignments']}
- Failed alignments: {test_results['summary']['failed_alignments']}
- Success rate: {test_results['summary']['success_rate']:.1%}

## Timing Accuracy
- Average accuracy: {test_results['summary']['average_timing_accuracy_ms']:.1f}ms
- Best accuracy: {test_results['summary']['best_timing_accuracy_ms']:.1f}ms
- Worst accuracy: {test_results['summary']['worst_timing_accuracy_ms']:.1f}ms

## Quality Distribution
- Excellent (<10ms): {test_results['summary']['excellent_alignments']}
- Good (10-50ms): {test_results['summary']['good_alignments']}
- Poor (>50ms): {test_results['summary']['poor_alignments']}

## Test Conclusion
{'✅ PASSED: Audio alignment successfully matches segment durations!' if test_results['summary']['success_rate'] > 0.8 and avg_accuracy < 50 else '❌ FAILED: Audio alignment needs improvement.'}
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt') as temp_file:
            temp_file.write(summary_content)
            temp_file.flush()
            
            summary_gcs_path = "test_output/audio_alignment_summary.txt"
            await gcs_client.upload_file(temp_file.name, summary_gcs_path)
            logger.info(f"📄 Summary uploaded: gs://{config.gcs_bucket_name}/{summary_gcs_path}")
        
        # Final console summary
        logger.info("\n" + "=" * 60)
        logger.info("🎉 AUDIO ALIGNMENT TEST COMPLETED!")
        logger.info("=" * 60)
        logger.info(f"✅ Success rate: {test_results['summary']['success_rate']:.1%}")
        logger.info(f"📊 Average accuracy: {avg_accuracy:.1f}ms")
        logger.info(f"📁 Results saved to: test_output/ folder")
        logger.info("=" * 60)
        
        return test_results
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        test_results['error'] = str(e)
        return test_results


async def main():
    """Run the audio alignment duration test."""
    print("""
    ╔══════════════════════════════════════════════════════════════════╗
    ║                AUDIO ALIGNMENT DURATION TEST                     ║
    ║            Testing Exact Duration Matching Logic                ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)
    
    try:
        result = await test_audio_alignment_for_video()
        
        if result.get('summary', {}).get('success_rate', 0) > 0.8:
            logger.info("🎊 Test completed successfully!")
            return 0
        else:
            logger.error("💥 Test revealed issues with audio alignment!")
            return 1
            
    except Exception as e:
        logger.error(f"💥 Test execution failed: {e}")
        return 1


if __name__ == "__main__":
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)