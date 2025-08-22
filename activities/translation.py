"""Translation activities using Google Translate v3 + OpenAI refinement for AI dubbing pipeline."""

import os
import json
import time
import asyncio
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import Counter

import aiofiles
from google.cloud import translate_v3 as translate
from google.oauth2 import service_account
from openai import OpenAI
from temporalio import activity

from shared.models import TranslationRequest, SegmentTranslationResult, ConsolidatedTranslationResult
from shared.database import get_database_client
from shared.gcs_client import AsyncGCSClient
from config import DubbingConfig

# Get configuration
config = DubbingConfig.from_env()


@activity.defn
async def load_transcriptions_activity(video_id: str) -> List[Dict[str, Any]]:
    """Load transcriptions from database by video_id."""
    
    activity.logger.info(f"Loading transcriptions for video {video_id}")
    
    async with get_database_client(config) as db_client:
        await db_client.log_processing_step(
            video_id, 
            "translation_load_transcriptions", 
            "processing",
            "Loading transcriptions from database"
        )
    
    try:
        async with get_database_client(config) as db_client:
            # Query transcriptions table for all segments of this video
            rows = await db_client.pool.fetch(
                """
                SELECT t.segment_id, t.segment_index, t.text, t.language
                FROM transcriptions t
                WHERE t.video_id = $1 AND t.status = 'completed'
                ORDER BY t.segment_index
                """,
                video_id
            )
            
            if not rows:
                raise ValueError(f"No completed transcriptions found for video {video_id}")
            
            transcriptions = []
            for row in rows:
                transcriptions.append({
                    "segment_id": str(row["segment_id"]),
                    "segment_index": row["segment_index"],
                    "text": row["text"],
                    "language": row["language"] or "en"  # Default to English if None
                })
            
            activity.logger.info(f"Loaded {len(transcriptions)} transcriptions for video {video_id}")
            
            await db_client.log_processing_step(
                video_id, 
                "translation_load_transcriptions", 
                "completed",
                f"Successfully loaded {len(transcriptions)} transcriptions"
            )
            
            return transcriptions
            
    except Exception as e:
        activity.logger.error(f"Failed to load transcriptions: {e}")
        
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "translation_load_transcriptions", 
                "failed",
                f"Failed to load transcriptions: {str(e)}",
                {"error": str(e)}
            )
        
        raise


@activity.defn
async def translate_segment_google_activity(
    video_id: str,
    transcription_data: Dict[str, Any],
    target_language: str,
    source_language: Optional[str] = None
) -> Dict[str, Any]:
    """Translate a single segment using Google Translate v3 API."""
    
    segment_id = transcription_data.get('segment_id', '')
    segment_index = transcription_data.get('segment_index', 0)
    source_text = transcription_data.get('text', '')
    detected_language = source_language or transcription_data.get('language', 'en')
    
    activity.logger.info(f"Translating segment {segment_index} for video {video_id} using Google Translate")
    
    start_time = time.time()
    
    async with get_database_client(config) as db_client:
        await db_client.log_processing_step(
            video_id, 
            f"translation_google_segment_{segment_index}", 
            "processing",
            f"Starting Google translation for segment {segment_index}"
        )
    
    try:
        # Use same authentication pattern as test_translate_simple.py
        credentials_value = config.google_application_credentials
        
        # Check if it's a file path or JSON content
        if os.path.isfile(credentials_value):
            credentials = service_account.Credentials.from_service_account_file(credentials_value)
        else:
            # Parse JSON credentials
            credentials_info = json.loads(credentials_value)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Initialize Google Translate client
        client = translate.TranslationServiceClient(credentials=credentials)
        parent = f"projects/{config.google_cloud_project}/locations/global"
        
        # Retry logic with 1-minute wait for Google Translate API
        max_retries = 2
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                activity.logger.info(f"Calling Google Translate API for segment {segment_index} (attempt {retry_count + 1}/{max_retries + 1})")
                
                response = client.translate_text(
                    request={
                        "parent": parent,
                        "contents": [source_text],
                        "mime_type": "text/plain",
                        "source_language_code": detected_language,
                        "target_language_code": target_language,
                    }
                )
                
                # Success - break out of retry loop
                break
                
            except Exception as api_error:
                retry_count += 1
                if retry_count > max_retries:
                    # Final attempt failed, re-raise the error
                    raise api_error
                
                # Wait 1 minute before retry
                activity.logger.warning(f"Google Translate API failed for segment {segment_index} (attempt {retry_count}/{max_retries + 1}): {api_error}. Waiting 1 minute before retry...")
                await asyncio.sleep(60)  # Wait 1 minute
        
        translation_result = response.translations[0]
        processing_time = time.time() - start_time
        
        result = {
            "video_id": video_id,
            "segment_id": segment_id,
            "segment_index": segment_index,
            "source_text": source_text,
            "source_language": detected_language,
            "target_language": target_language,
            "google_translation": translation_result.translated_text,
            "google_confidence_score": getattr(translation_result, 'confidence', None),
            "google_detected_language": translation_result.detected_language_code,
            "google_processing_time_seconds": processing_time,
            "success": True
        }
        
        activity.logger.info(f"Successfully translated segment {segment_index} with Google: '{translation_result.translated_text[:100]}...'")
        
        # Log to database
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                f"translation_google_segment_{segment_index}", 
                "completed",
                f"Successfully translated segment {segment_index} with Google ({len(translation_result.translated_text)} chars)"
            )
        
        return result
        
    except Exception as e:
        processing_time = time.time() - start_time
        error_msg = str(e)
        
        activity.logger.error(f"Failed to translate segment {segment_index} with Google: {error_msg}")
        
        result = {
            "video_id": video_id,
            "segment_id": segment_id,
            "segment_index": segment_index,
            "source_text": source_text,
            "source_language": detected_language,
            "target_language": target_language,
            "google_translation": "",
            "google_processing_time_seconds": processing_time,
            "success": False,
            "error_message": error_msg
        }
        
        # Log error to database
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                f"translation_google_segment_{segment_index}", 
                "failed",
                f"Failed to translate segment {segment_index} with Google: {error_msg}",
                {"error": error_msg, "segment_id": segment_id}
            )
        
        return result


@activity.defn
async def refine_translation_openai_activity(
    video_id: str,
    google_result: Dict[str, Any],
    cultural_context: str = "general"
) -> Dict[str, Any]:
    """Refine Google translation with OpenAI for cultural appropriateness."""
    
    segment_index = google_result.get('segment_index', 0)
    source_text = google_result.get('source_text', '')
    google_translation = google_result.get('google_translation', '')
    source_language = google_result.get('source_language', 'en')
    target_language = google_result.get('target_language', 'es')
    
    activity.logger.info(f"Refining translation for segment {segment_index} with OpenAI")
    
    start_time = time.time()
    
    async with get_database_client(config) as db_client:
        await db_client.log_processing_step(
            video_id, 
            f"translation_openai_segment_{segment_index}", 
            "processing",
            f"Starting OpenAI refinement for segment {segment_index}"
        )
    
    try:
        # Initialize OpenAI client
        openai_client = OpenAI(api_key=config.openai_api_key)
        
        # Cultural refinement prompt
        prompt = f"""You are a professional translator and cultural consultant specializing in nuanced, culturally-aware translation refinement.

**Task**: Refine the following machine translation to improve cultural appropriateness, natural flow, and local context while maintaining technical accuracy and timing constraints for audio dubbing.

**Source Language**: {source_language}
**Target Language**: {target_language}
**Cultural Context**: {cultural_context}

**Original Text**: "{source_text}"
**Machine Translation**: "{google_translation}"

**Cultural Refinement Guidelines**:
- Adapt idioms, expressions, and cultural references appropriately for {target_language} speakers
- Ensure natural speech patterns and conversational flow
- Maintain technical accuracy and preserve original meaning
- Consider regional variations and appropriate formality levels
- Preserve speaker tone, emotion, and intent
- Adapt humor, metaphors, and cultural references to local context
- Keep similar length for audio dubbing timing constraints
- Ensure naturalness for native {target_language} speakers

**Requirements**:
- Provide ONLY the refined translation text
- Maintain appropriate length/timing for dubbing
- Preserve speaker personality and emotional tone
- Ensure cultural appropriateness and natural flow

**Culturally Refined Translation**:"""
        
        # Retry logic with 1-minute wait
        max_retries = 2
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                activity.logger.info(f"Calling OpenAI API for segment {segment_index} (attempt {retry_count + 1}/{max_retries + 1})")
                
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    max_tokens=1000,
                    timeout=60  # 1-minute timeout to prevent hanging
                )
                
                # Success - break out of retry loop
                break
                
            except Exception as api_error:
                retry_count += 1
                if retry_count > max_retries:
                    # Final attempt failed, re-raise the error
                    raise api_error
                
                # Wait 1 minute before retry
                activity.logger.warning(f"OpenAI API failed for segment {segment_index} (attempt {retry_count}/{max_retries + 1}): {api_error}. Waiting 1 minute before retry...")
                await asyncio.sleep(60)  # Wait 1 minute
        
        refined_text = response.choices[0].message.content.strip()
        processing_time = time.time() - start_time
        
        # Update the result with OpenAI refinement
        result = google_result.copy()
        result.update({
            "openai_refined_translation": refined_text,
            "openai_processing_time_seconds": processing_time,
            "openai_model_used": response.model,
            "openai_api_request_id": response.id,
            "cultural_context": cultural_context,
            "processing_time_total_seconds": google_result.get('google_processing_time_seconds', 0) + processing_time
        })
        
        activity.logger.info(f"Successfully refined segment {segment_index} with OpenAI: '{refined_text[:100]}...'")
        
        # Log to database
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                f"translation_openai_segment_{segment_index}", 
                "completed",
                f"Successfully refined segment {segment_index} with OpenAI ({len(refined_text)} chars)"
            )
        
        return result
        
    except Exception as e:
        processing_time = time.time() - start_time
        error_msg = str(e)
        
        activity.logger.error(f"Failed to refine segment {segment_index} with OpenAI: {error_msg}")
        
        # Return Google translation as fallback
        result = google_result.copy()
        result.update({
            "openai_refined_translation": google_translation,  # Fallback to Google translation
            "openai_processing_time_seconds": processing_time,
            "cultural_context": cultural_context,
            "processing_time_total_seconds": google_result.get('google_processing_time_seconds', 0) + processing_time,
            "error_message": f"OpenAI refinement failed: {error_msg}. Using Google translation as fallback."
        })
        
        # Log error to database
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                f"translation_openai_segment_{segment_index}", 
                "failed",
                f"Failed to refine segment {segment_index} with OpenAI: {error_msg}. Using Google translation as fallback.",
                {"error": error_msg, "segment_id": google_result.get('segment_id', '')}
            )
        
        return result


@activity.defn
async def store_translation_results_activity(
    video_id: str,
    translation_results: List[Dict[str, Any]]
) -> bool:
    """Store translation results in database with both Google and OpenAI versions."""
    
    activity.logger.info(f"Storing {len(translation_results)} translation results for video {video_id}")
    
    async with get_database_client(config) as db_client:
        await db_client.log_processing_step(
            video_id, 
            "translation_store_results", 
            "processing",
            f"Storing {len(translation_results)} translation results"
        )
    
    try:
        async with get_database_client(config) as db_client:
            for result in translation_results:
                # Look up the actual segment UUID from the database
                segment_uuid = await db_client.pool.fetchval(
                    """
                    SELECT id FROM segments 
                    WHERE video_id = $1 AND segment_index = $2
                    """,
                    video_id, result.get('segment_index')
                )
                
                if not segment_uuid:
                    activity.logger.warning(f"No segment found for video {video_id}, segment_index {result.get('segment_index')}")
                    continue
                
                # Store translation in database
                await db_client.pool.execute(
                    """
                    INSERT INTO translations (
                        video_id, segment_id, segment_index, 
                        source_text, source_language, target_language,
                        google_translation, google_confidence_score, google_detected_language, 
                        google_processing_time_seconds, google_api_request_id,
                        openai_refined_translation, openai_processing_time_seconds, 
                        openai_model_used, openai_api_request_id, cultural_context,
                        translation_quality_score, processing_time_total_seconds, status
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, 'completed')
                    """,
                    video_id, segment_uuid, result.get('segment_index'),
                    result.get('source_text'), result.get('source_language'), result.get('target_language'),
                    result.get('google_translation'), result.get('google_confidence_score'), result.get('google_detected_language'),
                    result.get('google_processing_time_seconds'), result.get('google_api_request_id'),
                    result.get('openai_refined_translation'), result.get('openai_processing_time_seconds'),
                    result.get('openai_model_used'), result.get('openai_api_request_id'), result.get('cultural_context'),
                    result.get('translation_quality_score'), result.get('processing_time_total_seconds')
                )
            
            activity.logger.info(f"Successfully stored {len(translation_results)} translation results")
            
            await db_client.log_processing_step(
                video_id, 
                "translation_store_results", 
                "completed",
                f"Successfully stored {len(translation_results)} translation results"
            )
            
            return True
            
    except Exception as e:
        error_msg = str(e)
        activity.logger.error(f"Failed to store translation results: {error_msg}")
        
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "translation_store_results", 
                "failed",
                f"Failed to store translation results: {error_msg}",
                {"error": error_msg}
            )
        
        raise


@activity.defn
async def consolidate_translations_activity(
    video_id: str,
    translation_results: List[Dict[str, Any]],
    target_language: str
) -> ConsolidatedTranslationResult:
    """Consolidate individual translation results into final output."""
    
    activity.logger.info(f"Consolidating {len(translation_results)} translations for video {video_id}")
    
    start_time = time.time()
    
    async with get_database_client(config) as db_client:
        await db_client.log_processing_step(
            video_id, 
            "translation_consolidation", 
            "processing",
            f"Consolidating {len(translation_results)} translation results"
        )
    
    try:
        # Sort results by segment index
        sorted_results = sorted(translation_results, key=lambda x: x.get('segment_index', 0))
        
        # Calculate metrics
        successful_results = [r for r in sorted_results if r.get('success', False)]
        total_segments = len(sorted_results)
        successful_translations = len(successful_results)
        success_rate = successful_translations / total_segments if total_segments > 0 else 0
        
        # Detect primary source language
        source_languages = [r.get('source_language') for r in successful_results if r.get('source_language')]
        language_counter = Counter(source_languages)
        primary_source_language = language_counter.most_common(1)[0][0] if language_counter else "unknown"
        
        # Calculate average confidence scores
        google_confidences = [r.get('google_confidence_score') for r in successful_results if r.get('google_confidence_score')]
        avg_google_confidence = sum(google_confidences) / len(google_confidences) if google_confidences else 0.0
        
        # Calculate processing times
        total_google_time = sum(r.get('google_processing_time_seconds', 0) for r in sorted_results)
        total_openai_time = sum(r.get('openai_processing_time_seconds', 0) for r in sorted_results)
        total_processing_time = total_google_time + total_openai_time
        
        # Convert to SegmentTranslationResult objects
        segment_results = []
        for result in sorted_results:
            segment_result = SegmentTranslationResult(
                video_id=result.get('video_id', video_id),
                segment_id=result.get('segment_id', ''),
                segment_index=result.get('segment_index', 0),
                source_text=result.get('source_text', ''),
                source_language=result.get('source_language', ''),
                target_language=result.get('target_language', target_language),
                google_translation=result.get('google_translation', ''),
                google_confidence_score=result.get('google_confidence_score'),
                google_detected_language=result.get('google_detected_language'),
                google_processing_time_seconds=result.get('google_processing_time_seconds', 0),
                google_api_request_id=result.get('google_api_request_id'),
                openai_refined_translation=result.get('openai_refined_translation', ''),
                openai_processing_time_seconds=result.get('openai_processing_time_seconds', 0),
                openai_model_used=result.get('openai_model_used', 'gpt-4'),
                openai_api_request_id=result.get('openai_api_request_id'),
                cultural_context=result.get('cultural_context', 'general'),
                translation_quality_score=result.get('translation_quality_score'),
                processing_time_total_seconds=result.get('processing_time_total_seconds', 0),
                success=result.get('success', False),
                error_message=result.get('error_message', '')
            )
            segment_results.append(segment_result)
        
        # Save consolidated translation to GCS
        gcs_translation_path = ""
        if successful_translations > 0:
            gcs_client = AsyncGCSClient(config)
            await gcs_client.connect()
            
            # Create consolidated translation data
            translation_data = {
                "video_id": video_id,
                "source_language": primary_source_language,
                "target_language": target_language,
                "total_segments": total_segments,
                "successful_translations": successful_translations,
                "translations": [
                    {
                        "segment_index": r.segment_index,
                        "segment_id": r.segment_id,
                        "source_text": r.source_text,
                        "google_translation": r.google_translation,
                        "google_confidence": r.google_confidence_score,
                        "openai_refined_translation": r.openai_refined_translation,
                        "cultural_context": r.cultural_context,
                        "success": r.success,
                        "processing_time": r.processing_time_total_seconds
                    }
                    for r in segment_results
                ],
                "quality_metrics": {
                    "avg_google_confidence": avg_google_confidence,
                    "success_rate": success_rate,
                    "processing_time_google": total_google_time,
                    "processing_time_openai": total_openai_time,
                    "processing_time_total": total_processing_time
                }
            }
            
            # Upload consolidated translation JSON to GCS
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                json.dump(translation_data, temp_file, indent=2, ensure_ascii=False)
                local_translation_path = temp_file.name
            
            try:
                gcs_path = f"{video_id}/translation/consolidated_translation_{target_language}.json"
                await gcs_client.upload_file(local_translation_path, gcs_path)
                gcs_translation_path = f"gs://{config.gcs_bucket_name}/{gcs_path}"
                
                activity.logger.info(f"Uploaded consolidated translation to: {gcs_translation_path}")
                
            finally:
                if os.path.exists(local_translation_path):
                    os.unlink(local_translation_path)
        
        # Create consolidated result
        consolidation_time = time.time() - start_time
        
        result = ConsolidatedTranslationResult(
            video_id=video_id,
            source_language=primary_source_language,
            target_language=target_language,
            cultural_context=sorted_results[0].get('cultural_context', 'general') if sorted_results else 'general',
            total_segments=total_segments,
            successful_translations=successful_translations,
            success_rate=success_rate,
            translations=segment_results,
            avg_google_confidence=avg_google_confidence,
            avg_translation_quality=0.95 if success_rate > 0.8 else success_rate,  # Placeholder quality score
            total_processing_time_seconds=total_processing_time + consolidation_time,
            google_processing_time_seconds=total_google_time,
            openai_processing_time_seconds=total_openai_time,
            gcs_translation_path=gcs_translation_path,
            success=success_rate > 0.8,  # Consider successful if >80% segments translated
            error_message="" if success_rate > 0.8 else f"Low success rate: {success_rate:.1%}"
        )
        
        activity.logger.info(f"Consolidation complete: {successful_translations}/{total_segments} successful "
                           f"({success_rate:.1%} success rate)")
        
        # Log final result to database
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "translation_consolidation", 
                "completed" if result.success else "partial",
                f"Consolidated {successful_translations}/{total_segments} translations "
                f"({success_rate:.1%} success rate)"
            )
        
        return result
        
    except Exception as e:
        consolidation_time = time.time() - start_time
        error_msg = str(e)
        
        activity.logger.error(f"Failed to consolidate translations: {error_msg}")
        
        async with get_database_client(config) as db_client:
            await db_client.log_processing_step(
                video_id, 
                "translation_consolidation", 
                "failed",
                f"Failed to consolidate translations: {error_msg}",
                {"error": error_msg}
            )
        
        # Return failed result
        return ConsolidatedTranslationResult(
            video_id=video_id,
            source_language="unknown",
            target_language=target_language,
            cultural_context="general",
            total_segments=len(translation_results),
            successful_translations=0,
            success_rate=0.0,
            translations=[],
            avg_google_confidence=0.0,
            avg_translation_quality=0.0,
            total_processing_time_seconds=consolidation_time,
            google_processing_time_seconds=0.0,
            openai_processing_time_seconds=0.0,
            gcs_translation_path="",
            success=False,
            error_message=error_msg
        )