"""Alignment and stitching workflow for creating final dubbed video with precise timing synchronization."""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities.audio_alignment import (
        load_alignment_data_activity,
        align_audio_segment_activity,
        generate_silence_activity
    )
    from activities.audio_stitching import (
        stitch_aligned_audio_activity,
        mux_video_with_dubbed_audio_activity,
        validate_final_video_activity
    )
    from shared.models import ProcessingStatus
    from shared.database import get_database_client
    from config import DubbingConfig

logger = logging.getLogger(__name__)


@workflow.defn
class AlignmentStitchingWorkflow:
    """
    Workflow for Stage 7 (Alignment & Pacing) and Stage 8 (Audio Stitching & Video Muxing).
    
    This workflow:
    1. Loads segment timing and voice synthesis data
    2. Aligns each audio segment to match original video timing
    3. Stitches aligned segments into continuous audio track
    4. Muxes final audio with original video
    5. Validates final dubbed video quality
    """
    
    def __init__(self) -> None:
        self.retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_interval=timedelta(seconds=60),
            maximum_attempts=3,
            backoff_coefficient=2.0
        )
    
    @workflow.run
    async def run(
        self,
        video_id: str,
        target_language: str,
        create_multitrack: bool = True,
        max_stretch_ratio: float = 2.0,
        timing_tolerance_ms: float = 50.0
    ) -> Dict[str, Any]:
        """
        Execute complete alignment and stitching workflow.
        
        Args:
            video_id: Video identifier
            target_language: Target language code for output naming
            create_multitrack: Whether to create version with both audio tracks
            max_stretch_ratio: Maximum allowed stretch ratio (default: 2.0x)
            timing_tolerance_ms: Maximum timing deviation tolerance (default: 50ms)
            
        Returns:
            Complete workflow result with final video paths and quality metrics
        """
        workflow.logger.info(f"Starting alignment and stitching workflow for video {video_id}")
        
        start_time = workflow.now()
        
        try:
            # Define GCS output folders (without bucket name - activities will add it)
            output_base = video_id
            aligned_audio_folder = f"{output_base}/aligned_audio"
            final_output_folder = f"{output_base}/final_output"
            
            # Stage 1: Load alignment data (segments + voice synthesis)
            workflow.logger.info("Loading alignment data...")
            alignment_data = await workflow.execute_activity(
                load_alignment_data_activity,
                video_id,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=self.retry_policy
            )
            
            segments_data = alignment_data['alignment_data']
            workflow.logger.info(f"Loaded {len(segments_data)} segments for alignment")
            
            # Validate speed ratios
            problematic_segments = [
                seg for seg in segments_data 
                if seg['speed_ratio'] > max_stretch_ratio or seg['speed_ratio'] < (1.0 / max_stretch_ratio)
            ]
            
            if problematic_segments:
                workflow.logger.warning(f"{len(problematic_segments)} segments exceed speed ratio limits")
                for seg in problematic_segments:
                    workflow.logger.warning(f"Segment {seg['segment_index']}: speed_ratio={seg['speed_ratio']:.2f}")
            
            # Stage 2: Align audio segments in parallel
            workflow.logger.info("Aligning audio segments...")
            
            # Create alignment tasks for parallel execution
            alignment_tasks = []
            for segment_data in segments_data:
                task = workflow.execute_activity(
                    align_audio_segment_activity,
                    args=[video_id, segment_data, aligned_audio_folder],
                    start_to_close_timeout=timedelta(minutes=10),
                    retry_policy=self.retry_policy
                )
                alignment_tasks.append(task)
            
            # Wait for all alignments to complete
            alignment_results = await asyncio.gather(*alignment_tasks, return_exceptions=True)
            
            # Process alignment results
            successful_alignments = []
            failed_alignments = []
            timing_issues = []
            
            for i, result in enumerate(alignment_results):
                if isinstance(result, Exception):
                    failed_alignments.append({
                        'segment_index': segments_data[i]['segment_index'],
                        'error': str(result)
                    })
                elif result['success']:
                    successful_alignments.append(result)
                    
                    # Check timing accuracy
                    if result['timing_accuracy_ms'] > timing_tolerance_ms:
                        timing_issues.append({
                            'segment_index': result['segment_index'],
                            'timing_accuracy_ms': result['timing_accuracy_ms']
                        })
                else:
                    failed_alignments.append({
                        'segment_index': result['segment_index'],
                        'error': result.get('error_message', 'Unknown error')
                    })
            
            if failed_alignments:
                error_msg = f"{len(failed_alignments)} segments failed alignment: {failed_alignments[:3]}"
                raise RuntimeError(error_msg)
            
            if timing_issues:
                workflow.logger.warning(f"{len(timing_issues)} segments exceed timing tolerance")
            
            # Stage 3: Stitch aligned audio segments
            workflow.logger.info("Stitching aligned audio segments...")
            
            stitching_result = await workflow.execute_activity(
                stitch_aligned_audio_activity,
                args=[video_id, final_output_folder],
                start_to_close_timeout=timedelta(minutes=20),
                retry_policy=self.retry_policy
            )
            
            if not stitching_result['success']:
                raise RuntimeError(f"Audio stitching failed: {stitching_result['error_message']}")
            
            final_audio_path = stitching_result['final_audio_gcs_path']
            
            # Stage 4: Mux video with dubbed audio
            workflow.logger.info("Muxing video with dubbed audio...")
            
            muxing_result = await workflow.execute_activity(
                mux_video_with_dubbed_audio_activity,
                args=[video_id, final_audio_path, final_output_folder, create_multitrack],
                start_to_close_timeout=timedelta(minutes=30),
                retry_policy=self.retry_policy
            )
            
            if not muxing_result['success']:
                raise RuntimeError(f"Video muxing failed: {muxing_result['error_message']}")
            
            final_video_path = muxing_result['final_video_gcs_path']
            
            # Stage 5: Validate final video
            workflow.logger.info("Validating final dubbed video...")
            
            validation_result = await workflow.execute_activity(
                validate_final_video_activity,
                args=[video_id, final_video_path],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=self.retry_policy
            )
            
            # Calculate overall metrics
            processing_time = (workflow.now() - start_time).total_seconds()
            
            avg_timing_accuracy = sum(r['timing_accuracy_ms'] for r in successful_alignments) / len(successful_alignments)
            avg_speed_ratio = sum(r['speed_ratio'] for r in successful_alignments) / len(successful_alignments)
            
            # Calculate overall sync score (1.0 is perfect)
            timing_score = max(0.0, 1.0 - (avg_timing_accuracy / 100.0))  # Degrade after 100ms
            speed_score = max(0.0, 1.0 - abs(1.0 - avg_speed_ratio))  # Penalize extreme speed changes
            overall_sync_score = (timing_score + speed_score) / 2.0
            
            # Compile final result
            result = {
                'video_id': video_id,
                'target_language': target_language,
                'success': True,
                
                # Output files
                'final_video_gcs_path': final_video_path,
                'final_audio_gcs_path': final_audio_path,
                'multitrack_video_gcs_path': muxing_result.get('multitrack_video_gcs_path'),
                
                # Processing metrics
                'segments_processed': len(successful_alignments),
                'segments_failed': len(failed_alignments),
                'segments_with_timing_issues': len(timing_issues),
                'processing_time_seconds': processing_time,
                
                # Quality metrics
                'avg_timing_accuracy_ms': avg_timing_accuracy,
                'avg_speed_ratio': avg_speed_ratio,
                'overall_sync_score': overall_sync_score,
                'duration_accuracy_ms': muxing_result.get('duration_accuracy_ms', 0.0),
                
                # Validation
                'validation_passed': validation_result.get('validation_passed', False),
                'validation_issues': validation_result.get('issues', []),
                
                # Detailed results
                'alignment_results': successful_alignments,
                'stitching_result': stitching_result,
                'muxing_result': muxing_result,
                'validation_result': validation_result,
                
                'error_message': '',
                'failed_segments': failed_alignments
            }
            
            workflow.logger.info(
                f"Alignment and stitching completed for video {video_id}: "
                f"{len(successful_alignments)} segments, "
                f"avg timing accuracy: {avg_timing_accuracy:.1f}ms, "
                f"sync score: {overall_sync_score:.2f}"
            )
            
            return result
            
        except Exception as e:
            workflow.logger.error(f"Alignment and stitching workflow failed: {str(e)}")
            raise


# Workflow execution helper functions

async def start_alignment_stitching_workflow(
    video_id: str,
    target_language: str = "en",
    create_multitrack: bool = True,
    workflow_id: Optional[str] = None
) -> str:
    """
    Start alignment and stitching workflow.
    
    Args:
        video_id: Video identifier
        target_language: Target language code
        create_multitrack: Whether to create multitrack version
        workflow_id: Optional custom workflow ID
        
    Returns:
        Workflow ID
    """
    from temporalio.client import Client
    
    client = await Client.connect("localhost:7233")  # Local Temporal server
    
    if not workflow_id:
        workflow_id = f"alignment-stitching-{video_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    await client.start_workflow(
        AlignmentStitchingWorkflow.run,
        video_id,
        target_language,
        create_multitrack,
        id=workflow_id,
        task_queue="dubbing-pipeline"
    )
    
    return workflow_id


async def get_alignment_stitching_result(workflow_id: str) -> Dict[str, Any]:
    """
    Get alignment and stitching workflow result.
    
    Args:
        workflow_id: Workflow identifier
        
    Returns:
        Workflow result
    """
    from temporalio.client import Client
    
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)
    
    return await handle.result()


# Client script for testing
if __name__ == "__main__":
    import asyncio
    
    async def test_workflow():
        """Test the alignment and stitching workflow."""
        video_id = "a82c5c2a-3099-476d-937b-caf03bcc4043"  # Test video ID
        
        print(f"Starting alignment and stitching workflow for video {video_id}")
        
        workflow_id = await start_alignment_stitching_workflow(
            video_id=video_id,
            target_language="es",  # Spanish
            create_multitrack=True
        )
        
        print(f"Workflow started: {workflow_id}")
        print("Waiting for completion...")
        
        result = await get_alignment_stitching_result(workflow_id)
        
        print("\n=== Alignment & Stitching Results ===")
        print(f"Success: {result['success']}")
        print(f"Segments processed: {result['segments_processed']}")
        print(f"Avg timing accuracy: {result.get('avg_timing_accuracy_ms', 0):.1f}ms")
        print(f"Overall sync score: {result.get('overall_sync_score', 0):.2f}")
        print(f"Duration accuracy: {result.get('duration_accuracy_ms', 0):.1f}ms")
        print(f"Final video: {result.get('final_video_gcs_path', 'N/A')}")
        print(f"Multitrack video: {result.get('multitrack_video_gcs_path', 'N/A')}")
        print(f"Processing time: {result.get('processing_time_seconds', 0):.1f}s")
        
        if result.get('validation_issues'):
            print(f"\nValidation issues: {result['validation_issues']}")
        
        if not result['success']:
            print(f"\nError: {result.get('error_message', 'Unknown error')}")
    
    asyncio.run(test_workflow())