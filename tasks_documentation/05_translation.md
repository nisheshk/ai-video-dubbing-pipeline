# Task 5: Translation (Google Translate v3 + OpenAI Cultural Refinement)

## Overview
The Translation stage converts transcribed text from the source language to the target language using a two-stage process: Google Translate v3 API for accurate base translation, followed by OpenAI GPT-4 for cultural refinement and localization. The OpenAI refined version serves as the final output, while Google Translate results are stored for audit and comparison purposes.

## Input
- **Source**: Transcriptions from previous stage via `transcriptions` table lookup by video_id
- **Data**: Segment-level transcribed text with timing information
- **Languages**: Source language (auto-detected from transcription) and target language specified
- **Format**: Text segments with segment indices and timing alignment

## Processing Requirements

### Core Functionality
1. **Database Integration**: Load transcriptions by video_id from transcriptions table
2. **Google Translate v3**: Primary translation for accuracy and speed
3. **OpenAI Cultural Refinement**: Context-aware cultural adaptation (final output)
4. **Dual Storage**: Store both versions for audit and quality comparison
5. **Pipeline Integration**: Provide refined translations to next stage

### Technical Specifications
- **Primary Service**: Google Cloud Translation v3 API
- **Refinement Service**: OpenAI GPT-4 with specialized cultural prompts
- **Input Source**: Database query on transcriptions table
- **Output Format**: Cultural-aware translated text per segment
- **Storage**: Both Google and OpenAI versions stored for audit

## Architecture

### Workflow Design
```
Video ID → Load Transcriptions → Google Translate → OpenAI Refinement → Store Both → Use OpenAI Output
(input)   → (database query)   → (base accuracy) → (cultural enhancement) → (audit) → (final)
```

### Data Flow
1. **Input Processing**: Receive video_id and target language parameters
2. **Transcription Loading**: Query transcriptions table for all segments
3. **Google Translation**: Parallel translation of all text segments
4. **OpenAI Refinement**: Cultural adaptation with context-aware prompts
5. **Database Storage**: Store both versions with audit trail
6. **Output Delivery**: Provide OpenAI refined translations to next pipeline stage

### Task Distribution
- **Input**: Video ID and target language configuration
- **Parallel Processing**: Each transcribed segment processed independently
- **Dual Translation**: Google Translate + OpenAI refinement per segment
- **Final Output**: OpenAI culturally refined translations

## Implementation Details

### Database Schema
```sql
-- Translations table with dual storage
CREATE TABLE translations (
    id UUID DEFAULT uuid_generate_v4(),
    video_id UUID NOT NULL,
    segment_id UUID NOT NULL,
    segment_index INTEGER NOT NULL,
    
    -- Source data from transcriptions
    source_text TEXT NOT NULL,
    source_language VARCHAR(10),
    target_language VARCHAR(10),
    
    -- Google Translate v3 Results (for audit/logging)
    google_translation TEXT NOT NULL,
    google_confidence_score FLOAT,
    google_detected_language VARCHAR(10),
    google_processing_time_seconds FLOAT,
    google_api_request_id VARCHAR(100),
    
    -- OpenAI Refinement Results (FINAL OUTPUT)
    openai_refined_translation TEXT NOT NULL,
    openai_processing_time_seconds FLOAT,
    openai_model_used VARCHAR(50) DEFAULT 'gpt-4',
    openai_api_request_id VARCHAR(100),
    cultural_context TEXT,
    
    -- Quality and Processing Metrics
    translation_quality_score FLOAT,
    processing_time_total_seconds FLOAT,
    
    status TEXT DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_translations_video_id ON translations(video_id);
CREATE INDEX idx_translations_segment_index ON translations(video_id, segment_index);
CREATE INDEX idx_translations_status ON translations(status);
```

### Activity Structure
- **`load_transcriptions_activity`**: Load transcribed text segments by video_id
- **`translate_segment_google_activity`**: Google Translate v3 API processing
- **`refine_translation_openai_activity`**: Cultural refinement with OpenAI GPT-4
- **`store_translation_results_activity`**: Database storage with audit trail
- **`consolidate_translations_activity`**: Final result compilation and validation

### Google Translate v3 Integration
```python
from google.cloud import translate_v3 as translate
from google.oauth2 import service_account
import json

async def translate_with_google_v3(text: str, source_lang: str, target_lang: str, project_id: str):
    """Translate text using Google Cloud Translation v3 API."""
    
    # Use same authentication pattern as test_translate_simple.py
    config = DubbingConfig.from_env()
    credentials_info = json.loads(config.google_application_credentials)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    
    client = translate.TranslationServiceClient(credentials=credentials)
    parent = f"projects/{project_id}/locations/global"
    
    response = client.translate_text(
        request={
            "parent": parent,
            "contents": [text],
            "mime_type": "text/plain",
            "source_language_code": source_lang,
            "target_language_code": target_lang,
        }
    )
    
    return {
        "translated_text": response.translations[0].translated_text,
        "detected_language": response.translations[0].detected_language_code,
        "confidence_score": getattr(response.translations[0], 'confidence', 0.0)
    }
```

### OpenAI Cultural Refinement Integration
```python
from openai import OpenAI

async def refine_translation_with_openai(
    source_text: str, 
    google_translation: str, 
    source_lang: str, 
    target_lang: str,
    cultural_context: str = "general"
):
    """Refine Google translation with OpenAI for cultural appropriateness."""
    
    client = OpenAI(api_key=config.openai_api_key)
    
    # Cultural refinement prompt
    prompt = f"""You are a professional translator and cultural consultant specializing in nuanced, culturally-aware translation refinement.

**Task**: Refine the following machine translation to improve cultural appropriateness, natural flow, and local context while maintaining technical accuracy and timing constraints for audio dubbing.

**Source Language**: {source_lang}
**Target Language**: {target_lang}
**Cultural Context**: {cultural_context}

**Original Text**: "{source_text}"
**Machine Translation**: "{google_translation}"

**Cultural Refinement Guidelines**:
- Adapt idioms, expressions, and cultural references appropriately for {target_lang} speakers
- Ensure natural speech patterns and conversational flow
- Maintain technical accuracy and preserve original meaning
- Consider regional variations and appropriate formality levels
- Preserve speaker tone, emotion, and intent
- Adapt humor, metaphors, and cultural references to local context
- Keep similar length for audio dubbing timing constraints
- Ensure naturalness for native {target_lang} speakers

**Requirements**:
- Provide ONLY the refined translation text
- Maintain appropriate length/timing for dubbing
- Preserve speaker personality and emotional tone
- Ensure cultural appropriateness and natural flow

**Culturally Refined Translation**:"""
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=1000
    )
    
    refined_text = response.choices[0].message.content.strip()
    
    return {
        "refined_translation": refined_text,
        "model_used": response.model,
        "api_request_id": response.id,
        "processing_time": response.usage.total_tokens
    }
```

### Configuration Parameters
```python
translation_config = {
    # Google Translate v3 Configuration
    "google_project_id": "engaged-lamp-314820",
    "google_location": "global",
    "google_model": "general",  # or "premium" for higher quality
    "google_timeout_seconds": 30,
    
    # OpenAI Configuration
    "openai_model": "gpt-4",
    "openai_temperature": 0.3,  # Balance creativity with consistency
    "openai_max_tokens": 1000,
    "openai_timeout_seconds": 60,
    
    # Processing Configuration
    "max_concurrent_translations": 10,
    "enable_cultural_refinement": True,
    "store_both_versions": True,  # For audit purposes
    "use_refined_as_output": True,
    
    # Quality Settings
    "min_confidence_threshold": 0.7,
    "cultural_context_default": "general",
    "preserve_timing_constraints": True
}
```

## Output Structure

### Translation Request Model
```python
@dataclass
class TranslationRequest:
    video_id: str
    target_language: str
    source_language: Optional[str] = None  # Auto-detect from transcriptions
    cultural_context: str = "general"
    enable_refinement: bool = True
    translation_queue: str = "dubbing-translation-queue"
```

### Translation Result Models
```python
@dataclass
class SegmentTranslationResult:
    video_id: str
    segment_id: str
    segment_index: int
    source_text: str
    source_language: str
    target_language: str
    
    # Google Translation (audit)
    google_translation: str
    google_confidence: float
    google_processing_time: float
    
    # OpenAI Refinement (final output)
    openai_refined_translation: str
    openai_processing_time: float
    cultural_context: str
    
    # Quality metrics
    translation_quality_score: float
    success: bool
    error_message: str = ""

@dataclass
class ConsolidatedTranslationResult:
    video_id: str
    source_language: str
    target_language: str
    total_segments: int
    successful_translations: int
    
    # Translation results
    translations: List[SegmentTranslationResult]
    
    # Quality metrics
    avg_google_confidence: float
    avg_translation_quality: float
    total_processing_time: float
    success_rate: float
    
    # Storage locations
    gcs_translation_path: str
    
    success: bool
    error_message: str = ""
```

### Database Output Format
```json
{
  "video_id": "a82c5c2a-3099-476d-937b-caf03bcc4043",
  "source_language": "en",
  "target_language": "es",
  "total_segments": 33,
  "successful_translations": 33,
  "translations": [
    {
      "segment_index": 0,
      "segment_id": "uuid-segment-001",
      "source_text": "Hello and welcome to this video tutorial about machine learning fundamentals.",
      "google_translation": "Hola y bienvenidos a este videotutorial sobre fundamentos de aprendizaje automático.",
      "google_confidence": 0.98,
      "openai_refined_translation": "Hola y bienvenidos a este tutorial en video sobre los fundamentos del machine learning.",
      "cultural_context": "educational",
      "translation_quality_score": 0.95
    }
  ],
  "quality_metrics": {
    "avg_google_confidence": 0.96,
    "avg_translation_quality": 0.94,
    "success_rate": 1.0,
    "total_processing_time": 45.2,
    "google_processing_time": 12.3,
    "openai_processing_time": 32.9
  }
}
```

## Workflow Integration

### Temporal Workflow Implementation
```python
@workflow.defn
class TranslationWorkflow:
    """Workflow for translating transcribed segments with cultural refinement."""
    
    async def run(self, request: TranslationRequest) -> ConsolidatedTranslationResult:
        logger.info(f"Starting translation workflow for video {request.video_id}")
        
        # Define retry policy
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=5),
            maximum_attempts=3
        )
        
        try:
            # Step 1: Load transcriptions from database
            transcriptions = await workflow.execute_activity(
                load_transcriptions_activity,
                args=[request.video_id],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
            )
            
            if not transcriptions:
                return ConsolidatedTranslationResult(
                    video_id=request.video_id,
                    success=False,
                    error_message="No transcriptions found for video"
                )
            
            # Step 2: Parallel Google Translate processing
            google_tasks = []
            for transcription in transcriptions:
                task = workflow.execute_activity(
                    translate_segment_google_activity,
                    args=[transcription, request.target_language],
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=retry_policy,
                    task_queue=request.translation_queue
                )
                google_tasks.append(task)
            
            google_results = await asyncio.gather(*google_tasks)
            
            # Step 3: Parallel OpenAI refinement
            refinement_tasks = []
            for transcription, google_result in zip(transcriptions, google_results):
                task = workflow.execute_activity(
                    refine_translation_openai_activity,
                    args=[
                        transcription.text,
                        google_result.translation,
                        request.source_language or transcription.language,
                        request.target_language,
                        request.cultural_context
                    ],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=retry_policy,
                    task_queue=request.translation_queue
                )
                refinement_tasks.append(task)
            
            openai_results = await asyncio.gather(*refinement_tasks)
            
            # Step 4: Store results and consolidate
            final_result = await workflow.execute_activity(
                consolidate_translations_activity,
                args=[request.video_id, google_results, openai_results],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
            )
            
            return final_result
            
        except Exception as e:
            logger.error(f"Translation workflow failed: {e}")
            return ConsolidatedTranslationResult(
                video_id=request.video_id,
                success=False,
                error_message=str(e)
            )
```

### Queue Configuration
- **Task Queue**: `dubbing-translation-queue`
- **Worker Scaling**: Auto-scaling based on API rate limits
- **Timeout**: 5 minutes per activity (longer for OpenAI refinement)
- **Retry Policy**: 3 attempts with exponential backoff
- **Concurrency**: Limited by API rate limits (Google: 100 RPS, OpenAI: varies by tier)

## Quality Assurance

### Validation Checks
1. **Translation Completeness**: Ensure all segments have both Google and OpenAI translations
2. **Language Consistency**: Verify consistent target language across segments
3. **Cultural Appropriateness**: OpenAI refinement quality assessment
4. **Length Constraints**: Maintain appropriate length for dubbing timing
5. **API Response Validation**: Check for successful API responses and proper formatting

### Error Handling
- **API Rate Limits**: Exponential backoff and retry for both Google and OpenAI APIs
- **Translation Quality**: Fallback to Google translation if OpenAI refinement fails
- **Network Errors**: Robust retry mechanisms with jitter
- **Invalid Input**: Handle empty or malformed transcription text
- **API Quota**: Monitor usage and implement graceful degradation

### Cultural Refinement Quality
- **Context Awareness**: Ensure OpenAI understands cultural context and target audience
- **Natural Flow**: Validate that refined translations sound natural to native speakers
- **Technical Accuracy**: Preserve technical terms and specific meanings
- **Timing Preservation**: Maintain appropriate length for audio dubbing constraints

## Performance Considerations

### API Rate Limits
- **Google Translate v3**: 100 requests per second, 10M characters per month (free tier)
- **OpenAI GPT-4**: Varies by tier (typically 40K tokens per minute for Tier 1)
- **Implementation**: Queue-based rate limiting with intelligent batching

### Cost Management
- **Google Translate**: ~$20 per 1M characters
- **OpenAI GPT-4**: ~$30 per 1M input tokens + $60 per 1M output tokens
- **Combined Cost**: Approximately $2-4 per typical video (depending on length and language complexity)
- **Optimization**: Batch similar segments, cache common translations

### Performance Optimization
1. **Parallel Processing**: Process multiple segments concurrently within API limits
2. **Intelligent Batching**: Group short segments for more efficient API usage
3. **Caching Strategy**: Cache common phrases and technical terms
4. **Resource Management**: Efficient memory usage for large videos
5. **API Optimization**: Use appropriate models and parameters for cost/quality balance

## Integration Points

### Previous Stage: Transcription
- **Input**: Query transcriptions table by video_id
- **Data**: Segment-level transcribed text with timing information
- **Language Detection**: Use detected language from transcription as source

### Next Stage: Voice Cloning / TTS
- **Output**: OpenAI refined translations as primary input
- **Format**: Segment-level culturally appropriate text
- **Timing**: Preserved segment timing for audio synchronization
- **Quality**: Cultural adaptation for natural voice synthesis

### Database Integration
- **Storage**: Both Google and OpenAI translations stored with full audit trail
- **Querying**: Efficient retrieval of final translations for downstream stages
- **Monitoring**: API usage tracking, quality metrics, and performance monitoring

## Testing Strategy

### Unit Tests
- Google Translate v3 API integration and error handling
- OpenAI cultural refinement prompt effectiveness
- Database storage and retrieval operations
- Quality scoring and validation logic

### Integration Tests
- End-to-end translation workflow with real transcriptions
- Parallel processing with multiple segments
- API rate limiting and retry behavior
- Database consistency and audit trail verification

### Cultural Quality Tests
- Native speaker evaluation of refined translations
- A/B testing between Google and OpenAI refined versions
- Cultural appropriateness assessment across different target languages
- Timing constraint validation for dubbing compatibility

## Monitoring and Metrics

### Key Performance Indicators
- **Translation Accuracy**: Quality scores for both Google and OpenAI versions
- **Processing Throughput**: Segments processed per minute
- **API Success Rates**: Success percentage for both translation services
- **Cost Efficiency**: Translation cost per video/segment
- **Cultural Quality**: Native speaker feedback and quality scores

### Alerting Configuration
- **API Failures**: High error rates or service unavailability
- **Quality Degradation**: Sudden drops in translation quality scores
- **Cost Overruns**: Unexpected high API usage or cost spikes
- **Queue Backup**: Alert if translation queue depth exceeds capacity
- **Worker Health**: Monitor worker availability and performance

## Dependencies

### Required Packages
```txt
google-cloud-translate==3.12.1
openai>=1.12.0
asyncio-throttle==1.0.2
langdetect==1.0.9
```

### System Requirements
- **CPU**: Standard compute (no GPU needed for API calls)
- **Memory**: Minimal (for temporary data processing)
- **Network**: Reliable internet for API calls to Google Cloud and OpenAI
- **Database**: PostgreSQL connection for transcriptions and translations storage

## Configuration

### Environment Variables
```bash
# Google Cloud Translation
GOOGLE_CLOUD_PROJECT=engaged-lamp-314820
GOOGLE_CLOUD_CREDENTIALS={"type":"service_account",...}

# OpenAI Configuration
OPENAI_API_KEY=sk-...

# Processing Configuration
MAX_CONCURRENT_TRANSLATIONS=10
TRANSLATION_TIMEOUT_MINUTES=5
ENABLE_CULTURAL_REFINEMENT=true
CULTURAL_CONTEXT_DEFAULT=general

# Quality Settings
MIN_TRANSLATION_CONFIDENCE=0.7
STORE_AUDIT_VERSIONS=true
USE_REFINED_AS_OUTPUT=true
```

### Cost Estimation
- **Google Translate**: ~$20 per 1M characters
- **OpenAI GPT-4**: ~$90 per 1M tokens (combined input/output)
- **Example Video**: 33 segments × avg 50 words = ~1,650 words ≈ $1.50-3.00 total cost
- **Optimization**: Cultural context reuse, intelligent batching, quality-based selection

This translation stage provides culturally aware, high-quality translations using the accuracy of Google Translate v3 combined with the cultural intelligence of OpenAI GPT-4, ensuring natural and appropriate translations for the target audience while maintaining audit trails and cost efficiency.