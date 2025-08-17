"""Speech segmentation activities using Silero VAD for voice activity detection."""

import os
import json
import logging
import asyncio
import tempfile
from datetime import datetime
from typing import Dict, List, Any, Tuple
from pathlib import Path

import torch
import librosa
import soundfile as sf
from temporalio import activity
from google.cloud import storage

from shared.models import SpeechSegmentationRequest, SpeechSegmentationResult, AudioSegment, ProcessingStatus
from shared.database import get_database_client
from config import DubbingConfig

logger = logging.getLogger(__name__)

# Global VAD model cache
_vad_model = None
_vad_utils = None

def _load_vad_model():
    """Load Silero VAD model (cached globally)."""
    global _vad_model, _vad_utils
    
    if _vad_model is None:
        logger.info("Loading Silero VAD model...")
        try:
            model, utils = torch.hub.load(
                repo_or_dir='snakers4/silero-vad',
                model='silero_vad',
                force_reload=False,
                onnx=False
            )
            _vad_model = model
            _vad_utils = utils
            logger.info("Silero VAD model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load VAD model: {e}")
            raise
    
    return _vad_model, _vad_utils


@activity.defn
async def speech_segmentation_activity(request: SpeechSegmentationRequest) -> SpeechSegmentationResult:
    """
    Main speech segmentation activity using Silero VAD.
    
    Args:
        request: Speech segmentation request with video ID and audio path
        
    Returns:
        SpeechSegmentationResult with segment data and metadata
    """
    start_time = datetime.now()
    logger.info(f"Starting speech segmentation for video {request.video_id}")
    
    config = DubbingConfig.from_env()
    
    try:
        # Log processing start to database
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "speech_segmentation", 
                "processing",
                "Starting speech segmentation (VAD) processing"
            )
        
        # Download audio file from GCS
        logger.info("Step 1: Downloading audio from GCS")
        download_start = datetime.now()
        local_audio_path = await _download_audio_from_gcs(
            request.gcs_audio_path, 
            config.gcs_bucket_name
        )
        download_time = (datetime.now() - download_start).total_seconds()
        
        # Log download completion
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "audio_download", 
                "completed",
                f"Downloaded audio file in {download_time:.2f}s",
                execution_time_ms=int(download_time * 1000)
            )
        
        # Load and validate audio
        logger.info("Step 2: Loading and validating audio")
        load_start = datetime.now()
        audio_data, sample_rate = await _load_and_validate_audio(local_audio_path)
        load_time = (datetime.now() - load_start).total_seconds()
        
        # Log audio loading completion
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "audio_loading", 
                "completed",
                f"Loaded audio: {len(audio_data)/sample_rate:.1f}s duration, {sample_rate}Hz",
                execution_time_ms=int(load_time * 1000)
            )
        
        # Perform VAD segmentation
        logger.info("Step 3: Performing VAD speech segmentation")
        vad_start = datetime.now()
        segments = await _perform_vad_segmentation(
            audio_data=audio_data,
            sample_rate=sample_rate,
            request=request
        )
        vad_time = (datetime.now() - vad_start).total_seconds()
        
        # Log VAD completion
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "vad_segmentation", 
                "completed",
                f"VAD detected {len(segments)} speech segments in {vad_time:.2f}s",
                execution_time_ms=int(vad_time * 1000)
            )
        
        # Post-process segments (merge, split, validate)
        logger.info("Step 4: Post-processing segments")
        process_start = datetime.now()
        processed_segments = await _post_process_segments(segments, request)
        process_time = (datetime.now() - process_start).total_seconds()
        
        # Log post-processing completion
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "segment_processing", 
                "completed",
                f"Post-processed {len(segments)} → {len(processed_segments)} segments in {process_time:.2f}s",
                execution_time_ms=int(process_time * 1000)
            )
        
        # Create individual segment audio files
        logger.info("Step 5: Creating individual segment audio files")
        create_start = datetime.now()
        segment_files = await _create_segment_audio_files(
            audio_data=audio_data,
            sample_rate=sample_rate,
            segments=processed_segments,
            video_id=request.video_id,
            config=config
        )
        create_time = (datetime.now() - create_start).total_seconds()
        
        # Log segment file creation completion
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "segment_file_creation", 
                "completed",
                f"Created {len(segment_files)} segment audio files in {create_time:.2f}s",
                execution_time_ms=int(create_time * 1000)
            )
        
        # Upload segments to GCS
        logger.info("Step 6: Uploading segments to GCS")
        upload_start = datetime.now()
        gcs_manifest_path, gcs_segments_folder = await _upload_segments_to_gcs(
            segments=processed_segments,
            segment_files=segment_files,
            video_id=request.video_id,
            config=config
        )
        upload_time = (datetime.now() - upload_start).total_seconds()
        
        # Log upload completion
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "segment_upload", 
                "completed",
                f"Uploaded {len(processed_segments)} segments to GCS in {upload_time:.2f}s",
                execution_time_ms=int(upload_time * 1000)
            )
        
        # Store segments in database
        logger.info("Step 7: Storing segment metadata in database")
        db_start = datetime.now()
        async with get_database_client(config) as db_client:
            segment_ids = await db_client.create_segments(
                request.video_id,
                processed_segments
            )
        db_time = (datetime.now() - db_start).total_seconds()
        
        # Log database storage completion
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "segment_database_storage", 
                "completed",
                f"Stored {len(segment_ids)} segment records in database in {db_time:.2f}s",
                execution_time_ms=int(db_time * 1000)
            )
            
        # Calculate quality metrics
        metrics = _calculate_quality_metrics(processed_segments, audio_data, sample_rate)
        
        # Clean up local files
        await _cleanup_local_files([local_audio_path] + segment_files)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Log successful completion
        total_speech_duration = sum(seg['duration'] for seg in processed_segments)
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                request.video_id, 
                "speech_segmentation", 
                "completed",
                f"Speech segmentation completed: {len(processed_segments)} segments, {total_speech_duration:.1f}s speech",
                execution_time_ms=int(processing_time * 1000)
            )
        
        # Create result
        result = SpeechSegmentationResult(
            video_id=request.video_id,
            success=True,
            total_segments=len(processed_segments),
            total_speech_duration=sum(seg['duration'] for seg in processed_segments),
            processing_time_seconds=processing_time,
            segments=processed_segments,
            gcs_manifest_path=gcs_manifest_path,
            gcs_segments_folder=gcs_segments_folder,
            **metrics
        )
        
        logger.info(f"Speech segmentation completed for video {request.video_id}: "
                   f"{len(processed_segments)} segments in {processing_time:.2f}s")
        
        return result
        
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"Speech segmentation failed for video {request.video_id}: {e}")
        
        # Log error to database
        try:
            import json
            async with get_database_client(config) as db_client:
                await db_client.log_processing_step(
                    request.video_id, 
                    "speech_segmentation", 
                    "failed",
                    f"Speech segmentation failed: {str(e)}",
                    error_details=json.dumps({"error_type": type(e).__name__, "error_message": str(e)}),
                    execution_time_ms=int(processing_time * 1000)
                )
        except Exception as db_error:
            logger.error(f"Failed to log error to database: {db_error}")
        
        return SpeechSegmentationResult(
            video_id=request.video_id,
            success=False,
            total_segments=0,
            total_speech_duration=0.0,
            processing_time_seconds=processing_time,
            speech_to_silence_ratio=0.0,
            avg_segment_duration=0.0,
            error_message=str(e)
        )


async def _download_audio_from_gcs(gcs_path: str, bucket_name: str) -> str:
    """Download audio file from GCS to local temporary file."""
    from shared.gcs_client import AsyncGCSClient
    from config import DubbingConfig
    
    try:
        # Use the same GCS client as audio extraction
        config = DubbingConfig.from_env()
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        
        # Convert HTTPS URL to gs:// format if needed
        if gcs_path.startswith("https://storage.googleapis.com/"):
            # Convert https://storage.googleapis.com/bucket/path to path
            path_parts = gcs_path.replace("https://storage.googleapis.com/", "").split("/", 1)
            if len(path_parts) == 2:
                bucket_from_url, gcs_blob_path = path_parts
                logger.info(f"Converted HTTPS URL to GCS path: {gcs_blob_path}")
            else:
                raise ValueError(f"Invalid GCS HTTPS URL format: {gcs_path}")
        elif gcs_path.startswith("gs://"):
            # Extract path from gs:// URL
            gs_path = gcs_path.replace("gs://", "")
            path_parts = gs_path.split("/", 1)
            if len(path_parts) == 2:
                bucket_from_url, gcs_blob_path = path_parts
            else:
                raise ValueError(f"Invalid GCS URL format: {gcs_path}")
        else:
            raise ValueError(f"Unsupported GCS path format: {gcs_path}")
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
        temp_file.close()
        
        # Download using AsyncGCSClient
        await gcs_client.download_file(gcs_blob_path, temp_file.name)
        
        logger.info(f"Downloaded audio from GCS: {gcs_path} -> {temp_file.name}")
        return temp_file.name
        
    except Exception as e:
        logger.error(f"Failed to download audio from GCS: {e}")
        raise


async def _load_and_validate_audio(audio_path: str) -> Tuple[torch.Tensor, int]:
    """Load audio file and validate format for VAD processing."""
    try:
        # Load audio using librosa
        audio_data, sample_rate = librosa.load(audio_path, sr=16000, mono=True)
        
        # Convert to torch tensor
        audio_tensor = torch.FloatTensor(audio_data)
        
        # Validate audio properties
        if len(audio_tensor) == 0:
            raise ValueError("Audio file is empty")
        
        if sample_rate != 16000:
            logger.warning(f"Audio sample rate is {sample_rate}, expected 16000")
            
        duration = len(audio_tensor) / sample_rate
        logger.info(f"Loaded audio: {duration:.2f}s, {sample_rate}Hz, {len(audio_tensor)} samples")
        
        return audio_tensor, sample_rate
        
    except Exception as e:
        logger.error(f"Failed to load audio file: {e}")
        raise


async def _perform_vad_segmentation(
    audio_data: torch.Tensor, 
    sample_rate: int,
    request: SpeechSegmentationRequest
) -> List[Dict[str, Any]]:
    """Perform VAD segmentation using Silero VAD."""
    try:
        # Load VAD model
        model, utils = _load_vad_model()
        get_speech_timestamps = utils[0]
        
        # Get speech timestamps from VAD
        speech_timestamps = get_speech_timestamps(
            audio_data, 
            model,
            threshold=request.vad_threshold,
            sampling_rate=sample_rate,
            min_speech_duration_ms=request.min_speech_duration_ms,
            min_silence_duration_ms=request.min_silence_gap_ms,
            window_size_samples=512,
            speech_pad_ms=request.speech_padding_ms
        )
        
        # Convert timestamps to segment format
        segments = []
        for idx, timestamp in enumerate(speech_timestamps):
            start_time = timestamp['start'] / sample_rate
            end_time = timestamp['end'] / sample_rate
            duration = end_time - start_time
            
            # Calculate confidence score (Silero doesn't provide this directly)
            segment_audio = audio_data[timestamp['start']:timestamp['end']]
            confidence_score = _estimate_speech_confidence(segment_audio, model)
            
            segment = {
                'segment_index': idx,
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration,
                'confidence_score': confidence_score
            }
            segments.append(segment)
            
        logger.info(f"VAD detected {len(segments)} speech segments")
        return segments
        
    except Exception as e:
        logger.error(f"VAD segmentation failed: {e}")
        raise


def _estimate_speech_confidence(segment_audio: torch.Tensor, vad_model) -> float:
    """Estimate speech confidence for a segment using VAD model."""
    try:
        if len(segment_audio) == 0:
            return 0.0
            
        # Use VAD model to get confidence for the segment
        with torch.no_grad():
            speech_prob = vad_model(segment_audio, 16000).item()
            
        return min(max(speech_prob, 0.0), 1.0)  # Clamp between 0 and 1
        
    except Exception as e:
        logger.warning(f"Could not estimate confidence: {e}")
        return 0.5  # Default confidence


async def _post_process_segments(
    segments: List[Dict[str, Any]], 
    request: SpeechSegmentationRequest
) -> List[Dict[str, Any]]:
    """Post-process segments: merge close segments, split long ones, add IDs."""
    if not segments:
        return segments
        
    processed = []
    
    # Sort segments by start time
    segments.sort(key=lambda x: x['start_time'])
    
    current_segment = segments[0].copy()
    
    for next_segment in segments[1:]:
        # Check if segments should be merged (gap less than threshold)
        gap = next_segment['start_time'] - current_segment['end_time']
        
        if gap < (request.min_silence_gap_ms / 1000.0):
            # Merge segments
            current_segment['end_time'] = next_segment['end_time']
            current_segment['duration'] = current_segment['end_time'] - current_segment['start_time']
            current_segment['confidence_score'] = max(
                current_segment['confidence_score'],
                next_segment['confidence_score']
            )
        else:
            # Add current segment and start new one
            processed.append(current_segment)
            current_segment = next_segment.copy()
    
    # Add final segment
    processed.append(current_segment)
    
    # Split overly long segments
    final_segments = []
    for segment in processed:
        if segment['duration'] > request.max_segment_duration_s:
            # Split into smaller segments
            split_segments = _split_long_segment(segment, request.max_segment_duration_s)
            final_segments.extend(split_segments)
        else:
            final_segments.append(segment)
    
    # Re-index and add UUIDs
    for idx, segment in enumerate(final_segments):
        segment['segment_index'] = idx
        segment['id'] = f"seg_{request.video_id}_{idx:03d}"
    
    logger.info(f"Post-processing: {len(segments)} -> {len(final_segments)} segments")
    return final_segments


def _split_long_segment(segment: Dict[str, Any], max_duration: float) -> List[Dict[str, Any]]:
    """Split a long segment into smaller segments."""
    segments = []
    start_time = segment['start_time']
    total_duration = segment['duration']
    confidence = segment['confidence_score']
    
    num_splits = int(total_duration / max_duration) + 1
    split_duration = total_duration / num_splits
    
    for i in range(num_splits):
        split_start = start_time + (i * split_duration)
        split_end = min(split_start + split_duration, segment['end_time'])
        
        split_segment = {
            'start_time': split_start,
            'end_time': split_end,
            'duration': split_end - split_start,
            'confidence_score': confidence
        }
        segments.append(split_segment)
    
    return segments


async def _create_segment_audio_files(
    audio_data: torch.Tensor,
    sample_rate: int,
    segments: List[Dict[str, Any]],
    video_id: str,
    config: DubbingConfig
) -> List[str]:
    """Extract individual segment audio files."""
    segment_files = []
    
    try:
        for segment in segments:
            # Calculate sample indices
            start_sample = int(segment['start_time'] * sample_rate)
            end_sample = int(segment['end_time'] * sample_rate)
            
            # Extract segment audio
            segment_audio = audio_data[start_sample:end_sample]
            
            # Create temporary file for segment
            temp_file = tempfile.NamedTemporaryFile(
                suffix=f"_seg_{segment['segment_index']:03d}.wav", 
                delete=False
            )
            temp_file.close()
            
            # Save segment audio
            sf.write(temp_file.name, segment_audio.numpy(), sample_rate)
            segment_files.append(temp_file.name)
            
        logger.info(f"Created {len(segment_files)} segment audio files")
        return segment_files
        
    except Exception as e:
        logger.error(f"Failed to create segment audio files: {e}")
        # Clean up any created files
        await _cleanup_local_files(segment_files)
        raise


async def _upload_segments_to_gcs(
    segments: List[Dict[str, Any]],
    segment_files: List[str],
    video_id: str,
    config: DubbingConfig
) -> Tuple[str, str]:
    """Upload segment files and manifest to GCS."""
    from shared.gcs_client import AsyncGCSClient
    
    try:
        # Use the same GCS client as download
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        
        # Define GCS paths
        segments_folder = f"{video_id}/segments/"
        manifest_path = f"{segments_folder}manifest.json"
        
        # Upload individual segment files
        for segment, local_file in zip(segments, segment_files):
            segment_filename = f"{segment['segment_index']:03d}.wav"
            gcs_segment_path = f"{segments_folder}{segment_filename}"
            
            # Upload using AsyncGCSClient
            uploaded_url = await gcs_client.upload_file(local_file, gcs_segment_path)
            
            # Update segment with GCS path
            segment['gcs_audio_path'] = uploaded_url
        
        # Create and upload manifest
        manifest_data = {
            "video_id": video_id,
            "total_segments": len(segments),
            "total_speech_duration": sum(seg['duration'] for seg in segments),
            "segments": segments,
            "created_at": datetime.now().isoformat()
        }
        
        # Create temporary manifest file
        import tempfile
        import json
        temp_manifest = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(manifest_data, temp_manifest, indent=2)
        temp_manifest.close()
        
        # Upload manifest
        await gcs_client.upload_file(temp_manifest.name, manifest_path)
        
        # Clean up temporary manifest file
        os.remove(temp_manifest.name)
        
        gcs_manifest_url = f"gs://{config.gcs_bucket_name}/{manifest_path}"
        gcs_segments_url = f"gs://{config.gcs_bucket_name}/{segments_folder}"
        
        logger.info(f"Uploaded {len(segments)} segments and manifest to GCS")
        return gcs_manifest_url, gcs_segments_url
        
    except Exception as e:
        logger.error(f"Failed to upload segments to GCS: {e}")
        raise


def _calculate_quality_metrics(
    segments: List[Dict[str, Any]], 
    audio_data: torch.Tensor, 
    sample_rate: int
) -> Dict[str, Any]:
    """Calculate quality metrics for segmentation."""
    if not segments:
        return {
            'speech_to_silence_ratio': 0.0,
            'avg_segment_duration': 0.0,
            'confidence_scores': []
        }
    
    total_speech_duration = sum(seg['duration'] for seg in segments)
    total_audio_duration = len(audio_data) / sample_rate
    
    speech_to_silence_ratio = total_speech_duration / max(total_audio_duration, 0.001)
    avg_segment_duration = total_speech_duration / len(segments)
    confidence_scores = [seg['confidence_score'] for seg in segments]
    
    return {
        'speech_to_silence_ratio': speech_to_silence_ratio,
        'avg_segment_duration': avg_segment_duration,
        'confidence_scores': confidence_scores
    }


async def _cleanup_local_files(file_paths: List[str]) -> None:
    """Clean up local temporary files."""
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Cleaned up file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to clean up file {file_path}: {e}")