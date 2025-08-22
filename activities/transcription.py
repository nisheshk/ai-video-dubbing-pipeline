"""Transcription activities using OpenAI Whisper API for AI dubbing pipeline."""

import os
import json
import time
import asyncio
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import Counter

import aiofiles
from openai import OpenAI
from temporalio import activity

from shared.models import TranscriptionRequest, TranscriptionResult, ConsolidatedTranscriptionResult
from shared.database import get_database_client
from shared.gcs_client import AsyncGCSClient
from config import DubbingConfig

# Get configuration
config = DubbingConfig.from_env()


@activity.defn
async def load_segments_activity(video_id: str) -> List[Dict[str, Any]]:
    """Load audio segments from speech segmentation results."""
    
    activity.logger.info(f"Loading segments for video {video_id}")
    
    async with get_database_client(config) as db_client:
        await db_client.log_processing_step(
            video_id, 
            "transcription_load_segments", 
            "processing",
            "Loading audio segments from speech segmentation"
        )
    
    try:
        # Download segment manifest from GCS
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        
        manifest_path = f"{video_id}/segments/manifest.json"
        
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as temp_file:
            local_manifest_path = temp_file.name
        
        try:
            await gcs_client.download_file(manifest_path, local_manifest_path)
            
            async with aiofiles.open(local_manifest_path, 'r') as f:
                content = await f.read()
                manifest_data = json.loads(content)
            
            segments = manifest_data.get('segments', [])
            
            activity.logger.info(f"Loaded {len(segments)} segments for transcription")
            
            async with get_database_client(config) as db_client:
                await db_client.log_processing_step(
                    video_id, 
                    "transcription_load_segments", 
                    "completed",
                    f"Successfully loaded {len(segments)} segments"
                )
            
            return segments
            
        finally:
            if os.path.exists(local_manifest_path):
                os.unlink(local_manifest_path)
                
    except Exception as e:
        activity.logger.error(f"Failed to load segments: {e}")
        
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "transcription_load_segments", 
                "failed",
                f"Failed to load segments: {str(e)}",
                {"error": str(e)}
            )
        
        raise


@activity.defn 
async def transcribe_segment_activity(
    video_id: str,
    segment_data: Dict[str, Any],
    transcription_config: Optional[Dict[str, Any]] = None
) -> TranscriptionResult:
    """Transcribe a single audio segment using OpenAI Whisper API."""
    
    segment_id = segment_data.get('id', '')
    segment_index = segment_data.get('segment_index', 0)
    gcs_audio_path = segment_data.get('gcs_audio_path', '')
    
    activity.logger.info(f"Starting transcription for segment {segment_index}")
    
    start_time = time.time()
    
    # Default configuration
    config_params = {
        "model": "whisper-1",
        "response_format": "json",
        "language": None,
        "prompt": None,
        "temperature": 0
    }
    
    if transcription_config:
        config_params.update(transcription_config)
    
    # Reduce database calls - only log processing start for critical segments
    if segment_index < 3:  # Only log first few segments to reduce DB load
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                f"transcription_segment_{segment_index}", 
                "processing",
                f"Starting transcription for segment {segment_index}"
            )
    
    try:
        # Initialize OpenAI client
        openai_client = OpenAI(api_key=config.openai_api_key)
        
        # Download audio segment from GCS
        gcs_client = AsyncGCSClient(config)
        await gcs_client.connect()
        
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
            local_audio_path = temp_file.name
        
        try:
            # Extract GCS path without gs:// prefix
            gcs_path = gcs_audio_path.replace('gs://' + config.gcs_bucket_name + '/', '')
            await gcs_client.download_file(gcs_path, local_audio_path)
            
            # Call OpenAI Whisper API
            api_start_time = time.time()
            
            with open(local_audio_path, "rb") as audio_file:
                transcript_response = openai_client.audio.transcriptions.create(
                    model=config_params["model"],
                    file=audio_file,
                    response_format=config_params["response_format"],
                    language=config_params["language"],
                    prompt=config_params["prompt"], 
                    temperature=config_params["temperature"]
                )
            
            api_time = time.time() - api_start_time
            
            # Parse response based on format
            if config_params["response_format"] == "json":
                transcribed_text = transcript_response.text
                detected_language = getattr(transcript_response, 'language', None)
            else:
                transcribed_text = str(transcript_response)
                detected_language = None
            
            processing_time = time.time() - start_time
            
            # Create successful result
            result = TranscriptionResult(
                video_id=video_id,
                segment_id=segment_id,
                segment_index=segment_index,
                text=transcribed_text.strip(),
                language=detected_language,
                processing_time_seconds=processing_time,
                api_request_id=getattr(transcript_response, 'id', None),
                success=True
            )
            
            activity.logger.info(f"Successfully transcribed segment {segment_index}: '{transcribed_text[:100]}...'")
            
            # Store transcription result in database with minimal operations
            try:
                async with get_database_client(config) as db_client:
                    # Single transaction to minimize deadlock risk
                    async with db_client.pool.acquire() as conn:
                        async with conn.transaction():
                            # Look up segment UUID and insert transcription in one transaction
                            segment_uuid = await conn.fetchval(
                                "SELECT id FROM segments WHERE video_id = $1 AND segment_index = $2",
                                video_id, segment_index
                            )
                            
                            if not segment_uuid:
                                raise ValueError(f"No segment found for video {video_id}, segment_index {segment_index}")
                            
                            # Store transcription result
                            await conn.execute(
                                """
                                INSERT INTO transcriptions (video_id, segment_id, segment_index, text, language, 
                                                          processing_time_seconds, api_request_id, status)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, 'completed')
                                """,
                                video_id, segment_uuid, segment_index, transcribed_text, detected_language,
                                processing_time, result.api_request_id
                            )
                
                activity.logger.info(f"Stored transcription for segment {segment_index} in database")
            except Exception as db_error:
                activity.logger.error(f"Failed to store transcription for segment {segment_index}: {db_error}")
                # Continue anyway - the transcription result is still valid for the workflow
            
            return result
            
        finally:
            if os.path.exists(local_audio_path):
                os.unlink(local_audio_path)
                
    except Exception as e:
        processing_time = time.time() - start_time
        error_msg = str(e)
        
        activity.logger.error(f"Failed to transcribe segment {segment_index}: {error_msg}")
        
        # Create failed result
        result = TranscriptionResult(
            video_id=video_id,
            segment_id=segment_id,
            segment_index=segment_index,
            text="",
            language=None,
            processing_time_seconds=processing_time,
            success=False,
            error_message=error_msg
        )
        
        # Log error to database
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                f"transcription_segment_{segment_index}", 
                "failed",
                f"Failed to transcribe segment {segment_index}: {error_msg}",
                {"error": error_msg, "segment_id": segment_id}
            )
            
            # Look up the actual segment UUID from the database for error case too
            try:
                segment_uuid = await db_client.pool.fetchval(
                    """
                    SELECT id FROM segments 
                    WHERE video_id = $1 AND segment_index = $2
                    """,
                    video_id, segment_index
                )
                
                if segment_uuid:
                    # Store failed transcription in database using proper segment UUID
                    await db_client.pool.execute(
                        """
                        INSERT INTO transcriptions (video_id, segment_id, segment_index, text, language, 
                                                  processing_time_seconds, status, error_message)
                        VALUES ($1, $2, $3, $4, $5, $6, 'failed', $7)
                        """,
                        video_id, segment_uuid, segment_index, "", None, processing_time, error_msg
                    )
            except Exception as db_error:
                activity.logger.warning(f"Could not store failed transcription to database: {db_error}")
        
        return result


@activity.defn
async def consolidate_transcriptions_activity(
    video_id: str,
    transcription_results: List[TranscriptionResult]
) -> ConsolidatedTranscriptionResult:
    """Consolidate individual transcription results into final output."""
    
    activity.logger.info(f"Consolidating {len(transcription_results)} transcriptions for video {video_id}")
    
    start_time = time.time()
    
    async with get_database_client(config) as db_client:
        await db_client.log_processing_step(
            video_id, 
            "transcription_consolidation", 
            "processing",
            f"Consolidating {len(transcription_results)} transcription results"
        )
    
    try:
        # Sort results by segment index
        sorted_results = sorted(transcription_results, key=lambda x: x.segment_index)
        
        # Calculate metrics
        successful_results = [r for r in sorted_results if r.success]
        total_segments = len(sorted_results)
        successful_transcriptions = len(successful_results)
        success_rate = successful_transcriptions / total_segments if total_segments > 0 else 0
        
        # Detect primary language
        languages = [r.language for r in successful_results if r.language]
        language_counter = Counter(languages)
        primary_language = language_counter.most_common(1)[0][0] if language_counter else "unknown"
        
        # Calculate language consistency
        language_consistency = language_counter.most_common(1)[0][1] / len(languages) if languages else 0
        
        # Build full transcript
        transcript_parts = []
        for result in sorted_results:
            if result.success and result.text.strip():
                transcript_parts.append(result.text.strip())
        
        full_transcript = " ".join(transcript_parts)
        total_words = len(full_transcript.split()) if full_transcript else 0
        
        # Calculate total processing time
        total_processing_time = sum(r.processing_time_seconds for r in sorted_results)
        
        # Save transcript to GCS
        gcs_transcript_path = ""
        if successful_transcriptions > 0:
            gcs_client = AsyncGCSClient(config)
            await gcs_client.connect()
            
            transcript_data = {
                "video_id": video_id,
                "total_segments": total_segments,
                "successful_transcriptions": successful_transcriptions,
                "primary_language": primary_language,
                "full_transcript": full_transcript,
                "transcriptions": [
                    {
                        "segment_index": r.segment_index,
                        "segment_id": r.segment_id,
                        "text": r.text,
                        "language": r.language,
                        "success": r.success,
                        "processing_time_seconds": r.processing_time_seconds
                    }
                    for r in sorted_results
                ],
                "quality_metrics": {
                    "total_words": total_words,
                    "language_consistency_score": language_consistency,
                    "success_rate": success_rate,
                    "processing_time_total": total_processing_time
                }
            }
            
            # Upload transcript JSON to GCS
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                json.dump(transcript_data, temp_file, indent=2)
                local_transcript_path = temp_file.name
            
            try:
                gcs_path = f"{video_id}/transcription/full_transcript.json"
                await gcs_client.upload_file(local_transcript_path, gcs_path)
                gcs_transcript_path = f"gs://{config.gcs_bucket_name}/{gcs_path}"
                
                activity.logger.info(f"Uploaded transcript to: {gcs_transcript_path}")
                
            finally:
                if os.path.exists(local_transcript_path):
                    os.unlink(local_transcript_path)
        
        # Create consolidated result
        consolidation_time = time.time() - start_time
        
        result = ConsolidatedTranscriptionResult(
            video_id=video_id,
            success=success_rate > 0.8,  # Consider successful if >80% segments transcribed
            total_segments=total_segments,
            successful_transcriptions=successful_transcriptions,
            primary_language=primary_language,
            transcriptions=sorted_results,
            full_transcript=full_transcript,
            gcs_transcript_path=gcs_transcript_path,
            total_words=total_words,
            language_consistency_score=language_consistency,
            processing_time_seconds=total_processing_time + consolidation_time,
            api_requests_count=total_segments,
            success_rate=success_rate
        )
        
        activity.logger.info(f"Consolidation complete: {successful_transcriptions}/{total_segments} successful "
                           f"({success_rate:.1%} success rate)")
        
        # Log final result to database
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "transcription_consolidation", 
                "completed" if result.success else "partial",
                f"Consolidated {successful_transcriptions}/{total_segments} transcriptions "
                f"({success_rate:.1%} success rate, {total_words} words)"
            )
        
        return result
        
    except Exception as e:
        consolidation_time = time.time() - start_time
        error_msg = str(e)
        
        activity.logger.error(f"Failed to consolidate transcriptions: {error_msg}")
        
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "transcription_consolidation", 
                "failed",
                f"Failed to consolidate transcriptions: {error_msg}",
                {"error": error_msg}
            )
        
        # Return failed result
        return ConsolidatedTranscriptionResult(
            video_id=video_id,
            success=False,
            total_segments=len(transcription_results),
            successful_transcriptions=0,
            primary_language="unknown",
            transcriptions=transcription_results,
            full_transcript="",
            gcs_transcript_path="",
            total_words=0,
            language_consistency_score=0.0,
            processing_time_seconds=consolidation_time,
            api_requests_count=len(transcription_results),
            success_rate=0.0,
            error_message=error_msg
        )