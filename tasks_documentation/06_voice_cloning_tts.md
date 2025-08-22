# Task 6: Voice Cloning & Text-to-Speech (Replicate Speech-02-HD)

## Overview
The Voice Cloning & TTS stage converts culturally-refined translated text into natural-sounding speech audio using Replicate's Speech-02-HD model. This stage processes the OpenAI refined translations from the previous stage and generates high-quality multilingual audio segments with emotional expression and natural voice synthesis.

## Input
- **Source**: OpenAI refined translations from `translations` table by video_id
- **Data**: Segment-level culturally-refined text with timing information
- **Text Field**: `openai_refined_translation` column (final output from translation stage)
- **Languages**: Target language with automatic language boost optimization

## Processing Requirements

### Core Functionality
1. **Database Integration**: Load OpenAI refined translations by video_id from translations table
2. **Replicate Speech-02-HD**: High-fidelity text-to-speech synthesis
3. **Voice Consistency**: Maintain same voice across all segments of a video
4. **Audio Download**: Retrieve generated MP3 files from Replicate delivery
5. **GCS Storage**: Organized audio file storage with metadata tracking

### Technical Specifications
- **TTS Service**: Replicate Speech-02-HD model (`minimax/speech-02-hd`)
- **Audio Format**: MP3 with high-fidelity quality
- **Voice Selection**: Configurable voice IDs with emotion support
- **Language Support**: Automatic language detection and optimization
- **Processing**: Parallel segment processing with rate limiting

## Architecture

### Workflow Design
```
Video ID → Load Translations → Replicate TTS → Download Audio → GCS Storage → Database Logging
(input)  → (openai_refined)   → (MP3 generation) → (audio files) → (organized)  → (metadata)
```

### Data Flow
1. **Input Processing**: Receive video_id and voice configuration parameters
2. **Translation Loading**: Query translations table for OpenAI refined text
3. **TTS Generation**: Parallel processing through Replicate Speech-02-HD
4. **Audio Download**: Download MP3 files from Replicate delivery URLs
5. **GCS Upload**: Store audio files with organized naming structure
6. **Database Storage**: Store audio metadata and processing results

### Task Distribution
- **Input**: Video ID and voice synthesis configuration
- **Parallel Processing**: Each translation segment processed independently
- **Voice Synthesis**: Replicate API with consistent voice parameters
- **Audio Management**: Download, store, and track audio files

## Implementation Details

### Database Schema
```sql
-- Voice synthesis table for TTS results
CREATE TABLE voice_synthesis (
    id UUID DEFAULT uuid_generate_v4(),
    video_id UUID NOT NULL,
    translation_id UUID NOT NULL,  -- Links to translations.id
    segment_id UUID NOT NULL,
    segment_index INTEGER NOT NULL,
    
    -- Input data from translations
    source_text TEXT NOT NULL,
    target_language VARCHAR(10),
    
    -- Replicate TTS Configuration
    voice_id VARCHAR(50) DEFAULT 'Friendly_Person',
    emotion VARCHAR(20) DEFAULT 'neutral',
    language_boost VARCHAR(20) DEFAULT 'Automatic',
    english_normalization BOOLEAN DEFAULT true,
    
    -- TTS Results
    replicate_audio_url TEXT NOT NULL,
    gcs_audio_path TEXT,
    audio_duration_seconds FLOAT,
    processing_time_seconds FLOAT,
    replicate_request_id VARCHAR(100),
    
    -- Audio Quality Metrics
    audio_quality_score FLOAT,
    voice_similarity_score FLOAT,
    
    status TEXT DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_voice_synthesis_video_id ON voice_synthesis(video_id);
CREATE INDEX idx_voice_synthesis_segment_index ON voice_synthesis(video_id, segment_index);
CREATE INDEX idx_voice_synthesis_status ON voice_synthesis(status);
CREATE INDEX idx_voice_synthesis_translation_id ON voice_synthesis(translation_id);
```

### Activity Structure
- **`load_translations_for_tts_activity`**: Load OpenAI refined translations by video_id
- **`synthesize_voice_segment_activity`**: Generate audio using Replicate Speech-02-HD
- **`download_and_store_audio_activity`**: Download MP3 and upload to GCS
- **`consolidate_voice_synthesis_activity`**: Create audio manifest and quality metrics

### Replicate Speech-02-HD Integration
```python
import replicate
import requests
from pathlib import Path

async def synthesize_voice_with_replicate(text: str, voice_config: dict):
    """Generate speech audio using Replicate Speech-02-HD model."""
    
    # Initialize Replicate client
    replicate_client = replicate.Client(api_token=config.replicate_api_token)
    
    # Prepare TTS input
    input_params = {
        "text": text,
        "voice_id": voice_config.get("voice_id", "Friendly_Person"),
        "emotion": voice_config.get("emotion", "neutral"),
        "language_boost": voice_config.get("language_boost", "Automatic"),
        "english_normalization": voice_config.get("english_normalization", True)
    }
    
    # Call Replicate Speech-02-HD
    output = replicate_client.run(
        "minimax/speech-02-hd",
        input=input_params
    )
    
    # Get audio URL
    audio_url = output.url() if hasattr(output, 'url') else str(output)
    
    return {
        "audio_url": audio_url,
        "request_id": getattr(output, 'id', None),
        "processing_time": output.get('processing_time', 0)
    }

async def download_audio_from_url(audio_url: str, local_path: str):
    """Download audio file from Replicate delivery URL."""
    
    async with httpx.AsyncClient() as client:
        response = await client.get(audio_url)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        return {
            "local_path": local_path,
            "file_size": len(response.content),
            "content_type": response.headers.get('content-type', 'audio/mpeg')
        }
```

### Configuration Parameters
```python
voice_synthesis_config = {
    # Replicate Configuration
    "model": "minimax/speech-02-hd",
    "voice_id": "Friendly_Person",  # Available voices: Friendly_Person, Professional_Speaker, etc.
    "emotion": "neutral",           # Options: neutral, happy, sad, excited, calm
    "language_boost": "Automatic",  # Automatic language detection and optimization
    "english_normalization": True,  # Text normalization for better pronunciation
    
    # Processing Configuration
    "max_concurrent_requests": 5,   # Rate limiting for Replicate API
    "request_timeout_seconds": 300, # 5 minutes timeout per request
    "retry_attempts": 3,            # Retry failed requests
    "audio_format": "mp3",          # Output format
    "quality": "hd",               # High-definition audio
    
    # GCS Storage Configuration
    "gcs_audio_folder": "{video_id}/voice_synthesis/",
    "audio_filename_pattern": "segment_{segment_index:03d}.mp3",
    "manifest_filename": "audio_manifest.json",
    
    # Voice Selection Strategy
    "voice_selection_mode": "consistent",  # consistent, adaptive, or custom
    "emotion_detection": False,           # Analyze text for emotion (future enhancement)
    "voice_cloning": False               # Use custom voice cloning (future enhancement)
}
```

## Output Structure

### Voice Synthesis Request Model
```python
@dataclass
class VoiceSynthesisRequest:
    video_id: str
    target_language: str
    voice_id: str = "Friendly_Person"
    emotion: str = "neutral"
    language_boost: str = "Automatic"
    english_normalization: bool = True
    voice_synthesis_queue: str = "dubbing-voice-synthesis-queue"
```

### Voice Synthesis Result Models
```python
@dataclass
class SegmentVoiceSynthesisResult:
    video_id: str
    translation_id: str
    segment_id: str
    segment_index: int
    source_text: str
    target_language: str
    
    # TTS Configuration
    voice_id: str
    emotion: str
    language_boost: str
    
    # Replicate Results
    replicate_audio_url: str
    gcs_audio_path: str
    audio_duration_seconds: float
    processing_time_seconds: float
    replicate_request_id: str
    
    # Quality Metrics
    audio_quality_score: float
    voice_similarity_score: float
    
    success: bool
    error_message: str = ""

@dataclass
class ConsolidatedVoiceSynthesisResult:
    video_id: str
    target_language: str
    voice_id: str
    total_segments: int
    successful_synthesis: int
    
    # Audio Results
    voice_segments: List[SegmentVoiceSynthesisResult]
    
    # Quality Metrics
    total_audio_duration: float
    avg_audio_quality: float
    voice_consistency_score: float
    total_processing_time: float
    success_rate: float
    
    # Storage Information
    gcs_audio_folder: str
    audio_manifest_path: str
    
    success: bool
    error_message: str = ""
```

### Audio Manifest Format
```json
{
  "video_id": "e82c5c2a-3099-476d-937b-caf03bcc4043",
  "target_language": "es",
  "voice_id": "Friendly_Person",
  "emotion": "neutral",
  "total_segments": 33,
  "successful_synthesis": 33,
  "audio_segments": [
    {
      "segment_index": 0,
      "segment_id": "uuid-segment-001",
      "text": "Hola y bienvenidos a este tutorial en video sobre los fundamentos del machine learning.",
      "gcs_audio_path": "gs://dubbing-pipeline/e82c5c2a-3099-476d-937b-caf03bcc4043/voice_synthesis/segment_000.mp3",
      "audio_duration_seconds": 4.2,
      "audio_quality_score": 0.95,
      "processing_time_seconds": 3.1
    }
  ],
  "quality_metrics": {
    "total_audio_duration": 125.6,
    "avg_audio_quality": 0.94,
    "voice_consistency_score": 0.98,
    "success_rate": 1.0,
    "total_processing_time": 89.3,
    "cost_estimate": 0.83
  }
}
```

## Workflow Integration

### Temporal Workflow Implementation
```python
@workflow.defn
class VoiceSynthesisWorkflow:
    """Workflow for synthesizing voice from translated text using Replicate Speech-02-HD."""
    
    async def run(self, request: VoiceSynthesisRequest) -> ConsolidatedVoiceSynthesisResult:
        logger.info(f"Starting voice synthesis workflow for video {request.video_id}")
        
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=3),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=10),
            maximum_attempts=3
        )
        
        try:
            # Step 1: Load OpenAI refined translations from database
            translations = await workflow.execute_activity(
                load_translations_for_tts_activity,
                args=[request.video_id, request.target_language],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
            )
            
            if not translations:
                return ConsolidatedVoiceSynthesisResult(
                    video_id=request.video_id,
                    success=False,
                    error_message="No translations found for voice synthesis"
                )
            
            # Step 2: Parallel voice synthesis for each segment
            synthesis_tasks = []
            for translation in translations:
                task = workflow.execute_activity(
                    synthesize_voice_segment_activity,
                    args=[translation, request],
                    start_to_close_timeout=timedelta(minutes=15),
                    retry_policy=retry_policy,
                    task_queue=request.voice_synthesis_queue
                )
                synthesis_tasks.append(task)
            
            synthesis_results = await asyncio.gather(*synthesis_tasks)
            
            # Step 3: Download and store audio files in parallel
            storage_tasks = []
            for result in synthesis_results:
                if result.get('success', False):
                    task = workflow.execute_activity(
                        download_and_store_audio_activity,
                        args=[result, request.video_id],
                        start_to_close_timeout=timedelta(minutes=10),
                        retry_policy=retry_policy,
                        task_queue=request.voice_synthesis_queue
                    )
                    storage_tasks.append(task)
            
            if storage_tasks:
                await asyncio.gather(*storage_tasks)
            
            # Step 4: Consolidate results and create audio manifest
            final_result = await workflow.execute_activity(
                consolidate_voice_synthesis_activity,
                args=[request.video_id, synthesis_results],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy
            )
            
            return final_result
            
        except Exception as e:
            logger.error(f"Voice synthesis workflow failed: {e}")
            return ConsolidatedVoiceSynthesisResult(
                video_id=request.video_id,
                success=False,
                error_message=str(e)
            )
```

### Queue Configuration
- **Task Queue**: `dubbing-voice-synthesis-queue`
- **Worker Scaling**: Auto-scaling based on Replicate API rate limits
- **Timeout**: 15 minutes per TTS generation (longer for complex text)
- **Retry Policy**: 3 attempts with exponential backoff
- **Concurrency**: Limited by Replicate rate limits (~5 concurrent requests)

## Quality Assurance

### Voice Quality Validation
1. **Audio Duration**: Verify generated audio matches expected length
2. **Voice Consistency**: Ensure same voice_id across all segments
3. **Language Accuracy**: Validate pronunciation for target language
4. **Audio Quality**: Check for artifacts, distortion, or quality issues
5. **File Integrity**: Verify MP3 files are complete and playable

### Error Handling
- **Replicate API Limits**: Implement rate limiting and queue management
- **Audio Generation Failures**: Retry with different parameters or fallback voice
- **Download Errors**: Retry download with exponential backoff
- **GCS Upload Issues**: Handle storage failures with local caching
- **Quality Issues**: Flag segments for manual review or re-synthesis

### Voice Selection Strategy
- **Consistency**: Use same voice_id for entire video to maintain speaker identity
- **Language Optimization**: Select appropriate voices for target language
- **Emotion Mapping**: Apply appropriate emotions based on content context
- **Quality Preferences**: Balance processing time vs. audio quality

## Performance Considerations

### API Rate Limits (Replicate)
- **Concurrent Requests**: ~5 requests per second for Speech-02-HD
- **Queue Management**: Implement intelligent batching and scheduling
- **Retry Logic**: Exponential backoff for rate limit errors

### Cost Management
- **Character-Based Pricing**: ~$0.00025 per character processed
- **Text Optimization**: Remove unnecessary characters while preserving meaning
- **Batch Processing**: Group short segments for efficiency
- **Quality vs. Cost**: Balance HD quality with processing costs

### Performance Optimization
1. **Parallel Processing**: Process multiple segments concurrently within rate limits
2. **Audio Caching**: Cache generated audio for repeated use
3. **GCS Optimization**: Efficient upload with resumable transfers
4. **Memory Management**: Stream audio processing to handle large videos
5. **Storage Efficiency**: Compress and organize audio files effectively

## Integration Points

### Previous Stage: Translation
- **Input**: Load OpenAI refined translations from translations table
- **Data**: Culturally-appropriate text optimized for natural speech
- **Language**: Target language with cultural context

### Next Stage: Audio Alignment & Pacing
- **Output**: High-quality MP3 audio segments with timing metadata
- **Format**: Organized audio files with manifest for synchronization
- **Quality**: Consistent voice and audio quality for seamless playback

### Database Integration
- **Storage**: Complete TTS processing results with audio URLs
- **Linking**: Proper relationships to translations and segments tables
- **Monitoring**: Processing statistics and quality metrics

## Testing Strategy

### Unit Tests
- Replicate Speech-02-HD API integration and error handling
- Audio file download and GCS upload operations
- Voice synthesis configuration and parameter validation
- Quality scoring and audio metadata extraction

### Integration Tests
- End-to-end voice synthesis workflow with real translations
- Parallel processing with multiple segments and rate limiting
- Audio quality validation and consistency checks
- Database integration and audio manifest generation

### Quality Assessment Tests
- Voice consistency across segments of same video
- Audio quality metrics and subjective evaluation
- Language pronunciation accuracy for different target languages
- Processing time and cost optimization validation

## Monitoring and Metrics

### Key Performance Indicators
- **Synthesis Success Rate**: Percentage of successful TTS generations
- **Audio Quality Score**: Average quality rating across segments
- **Voice Consistency**: Similarity scores between segments
- **Processing Speed**: Average time per character/segment
- **Cost Efficiency**: Cost per video and per minute of audio

### Alerting Configuration
- **API Failures**: High error rates or service unavailability
- **Quality Degradation**: Sudden drops in audio quality scores
- **Cost Overruns**: Unexpected high API usage costs
- **Processing Delays**: Long queue times or timeout issues
- **Storage Issues**: GCS upload failures or capacity warnings

## Dependencies

### Required Packages
```txt
replicate==0.15.4
httpx==0.24.1
pydub==0.25.1
mutagen==1.46.0
```

### System Requirements
- **CPU**: Standard compute for API calls and audio processing
- **Memory**: Moderate for temporary audio file storage
- **Network**: Reliable internet for Replicate API and file downloads
- **Storage**: Temporary storage for audio processing and GCS upload

## Configuration

### Environment Variables
```bash
# Replicate Configuration
REPLICATE_API_TOKEN=r8_your_token_here

# Voice Synthesis Configuration
VOICE_SYNTHESIS_QUEUE=dubbing-voice-synthesis-queue
DEFAULT_VOICE_ID=Friendly_Person
DEFAULT_EMOTION=neutral
MAX_CONCURRENT_TTS_REQUESTS=5
TTS_TIMEOUT_MINUTES=15

# Quality Settings
AUDIO_QUALITY_THRESHOLD=0.8
VOICE_CONSISTENCY_THRESHOLD=0.9
ENABLE_QUALITY_VALIDATION=true
```

### Cost Estimation
- **Replicate Speech-02-HD**: ~$0.00025 per character
- **Example Video**: 33 segments × avg 100 chars = 3,300 chars ≈ $0.83 per video
- **Additional Costs**: GCS storage (~$0.01 per video) + processing time
- **Total Estimated Cost**: ~$0.85-1.50 per typical video depending on length

This voice synthesis stage provides high-quality, natural-sounding multilingual audio generation using Replicate's Speech-02-HD model, maintaining consistency and cultural appropriateness while optimizing for cost and processing efficiency.