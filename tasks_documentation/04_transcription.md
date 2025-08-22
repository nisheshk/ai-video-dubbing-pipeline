# Task 4: Transcription (OpenAI Whisper API)

## Overview
The Transcription stage converts segmented audio files into text using OpenAI's Whisper API. This stage processes each audio segment generated from the Speech Segmentation stage in parallel, providing accurate speech-to-text conversion with the cloud-based Whisper service.

## Input
- **Segments**: Audio segment files (`.wav`) from Speech Segmentation stage
- **Location**: GCS bucket at `gs://dubbing-pipeline/{video_id}/segments/`
- **Format**: Individual WAV files (16kHz mono audio)
- **Metadata**: Segment manifest JSON with timing and confidence data

## Processing Requirements

### Core Functionality
1. **Parallel Processing**: Each segment processed independently by workers
2. **OpenAI Whisper API**: Cloud-based transcription service
3. **Language Detection**: Automatic source language detection per segment
4. **Timestamp Alignment**: Maintain precise timing alignment with original video
5. **Quality Validation**: API response validation and error detection

### Technical Specifications
- **Service**: OpenAI Whisper API (`whisper-1` model)
- **Input Format**: Audio files (WAV, MP3, MP4, etc.) up to 25MB
- **Output Format**: Plain text transcription
- **Batch Processing**: Queue-based parallel execution
- **Rate Limiting**: Respect OpenAI API rate limits

## Architecture

### Workflow Design
```
Speech Segments → Transcription Queue → Parallel Workers → Transcription Results
     (33 files)    → (33 tasks)      → (N workers)    → (Consolidated output)
```

### Task Distribution
- **Input**: List of segment metadata from speech segmentation
- **Queue**: Individual transcription tasks per segment
- **Workers**: Multiple workers processing segments in parallel
- **Output**: Consolidated transcription results per video

### Data Flow
1. **Task Creation**: Create transcription task for each audio segment
2. **Queue Distribution**: Add tasks to Temporal task queue
3. **Parallel Execution**: Workers download segments and call OpenAI API
4. **Result Aggregation**: Collect and merge transcription results
5. **Quality Validation**: Validate transcription quality and completeness

## Implementation Details

### Database Schema
```sql
-- Transcriptions table
CREATE TABLE transcriptions (
    id UUID DEFAULT uuid_generate_v4(),
    video_id UUID NOT NULL,
    segment_id UUID NOT NULL,
    segment_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    language VARCHAR(10),
    processing_time_seconds FLOAT,
    api_request_id VARCHAR(100),
    status TEXT DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Activity Structure
- **`transcribe_segment_activity`**: Process individual audio segment via OpenAI API
- **`consolidate_transcriptions_activity`**: Merge segment transcriptions
- **`validate_transcription_quality_activity`**: Quality checks and validation

### OpenAI API Integration
```python
from openai import OpenAI

async def transcribe_audio_segment(gcs_audio_path: str, video_id: str, segment_index: int):
    """Transcribe a single audio segment using OpenAI Whisper API."""
    
    client = OpenAI(api_key=config.openai_api_key)
    
    # Download audio segment from GCS
    local_audio_path = await download_segment_from_gcs(gcs_audio_path)
    
    try:
        # Open audio file and call Whisper API
        with open(local_audio_path, "rb") as audio_file:
            transcript = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                response_format="json",  # or "text", "srt", "verbose_json", "vtt"
                language=None,  # Auto-detect or specify language code
                prompt=None,    # Optional context for better accuracy
                temperature=0   # Deterministic output
            )
        
        return {
            "video_id": video_id,
            "segment_index": segment_index,
            "text": transcript.text,
            "language": transcript.language if hasattr(transcript, 'language') else None
        }
        
    finally:
        # Cleanup local file
        os.unlink(local_audio_path)
```

### Configuration Parameters
```python
transcription_config = {
    "model": "whisper-1",           # OpenAI Whisper model
    "response_format": "json",      # text, json, srt, verbose_json, vtt
    "language": None,               # Auto-detect or specify (en, es, fr, etc.)
    "prompt": None,                 # Optional context prompt
    "temperature": 0,               # Deterministic output (0-1)
    "max_concurrent_requests": 10,  # Parallel API requests
    "request_timeout_seconds": 120, # Per-request timeout
    "retry_attempts": 3,            # Retry failed requests
    "rate_limit_buffer": 0.1        # Buffer for rate limiting
}
```

## Output Structure

### Transcription Result
```python
@dataclass
class TranscriptionResult:
    video_id: str
    segment_id: str
    segment_index: int
    text: str
    language: str
    processing_time: float
    api_request_id: str
    
@dataclass
class ConsolidatedTranscription:
    video_id: str
    total_segments: int
    primary_language: str
    transcriptions: List[TranscriptionResult]
    quality_metrics: Dict[str, Any]
```

### Consolidated Output
```json
{
  "video_id": "d82c5c2a-3099-476d-937b-caf03bcc4043",
  "total_segments": 33,
  "primary_language": "en",
  "transcriptions": [
    {
      "segment_index": 0,
      "segment_id": "seg_d82c5c2a-3099-476d-937b-caf03bcc4043_000",
      "text": "Hello and welcome to this video tutorial about machine learning fundamentals.",
      "language": "en",
      "start_time": 0.27,
      "end_time": 9.266,
      "word_count": 12,
      "api_request_id": "req_abc123"
    }
  ],
  "quality_metrics": {
    "total_words": 1250,
    "language_consistency": 0.98,
    "processing_time_total": 23.4,
    "api_requests_count": 33,
    "success_rate": 1.0
  },
  "gcs_transcript_path": "gs://dubbing-pipeline/d82c5c2a-3099-476d-937b-caf03bcc4043/transcription/full_transcript.json"
}
```

## Workflow Integration

### Temporal Workflow
```python
@workflow.defn
class TranscriptionWorkflow:
    async def run(self, request: TranscriptionRequest) -> TranscriptionResult:
        # 1. Load segment metadata from speech segmentation
        segments = await workflow.execute_activity(
            load_segments_activity,
            request.video_id,
            start_to_close_timeout=timedelta(minutes=2)
        )
        
        # 2. Create parallel transcription tasks
        transcription_tasks = []
        for segment in segments:
            task = workflow.execute_activity(
                transcribe_segment_activity,
                segment,
                start_to_close_timeout=timedelta(minutes=5)
            )
            transcription_tasks.append(task)
        
        # 3. Wait for all transcriptions to complete
        results = await asyncio.gather(*transcription_tasks)
        
        # 4. Consolidate and validate results
        final_result = await workflow.execute_activity(
            consolidate_transcriptions_activity,
            results,
            start_to_close_timeout=timedelta(minutes=2)
        )
        
        return final_result
```

### Queue Configuration
- **Task Queue**: `transcription-queue`
- **Worker Scaling**: Auto-scaling based on queue depth and API limits
- **Timeout**: 5 minutes per segment
- **Retry Policy**: 3 attempts with exponential backoff
- **Rate Limiting**: Respect OpenAI API rate limits (50 RPM for Tier 1)

## Quality Assurance

### Validation Checks
1. **Language Consistency**: Ensure all segments use same detected language
2. **Text Quality**: Check for reasonable text output (not empty, not gibberish)
3. **API Response**: Validate successful API responses
4. **Completeness**: Ensure all segments have transcriptions
5. **Error Handling**: Track and retry failed API requests

### Error Handling
- **API Rate Limits**: Implement exponential backoff and retry
- **File Size Limits**: Split large segments if needed (25MB limit)
- **Network Errors**: Retry with jitter to avoid thundering herd
- **Invalid Audio**: Handle segments with no speech or corrupted audio
- **API Quota**: Monitor usage and implement graceful degradation

## Performance Considerations

### API Rate Limits (OpenAI)
- **Tier 1**: 50 requests per minute, 500 requests per day
- **Tier 2**: 500 requests per minute, 10,000 requests per day
- **Implementation**: Queue-based rate limiting with backoff

### Optimization Strategies
1. **Concurrent Requests**: Limited by API rate limits
2. **File Size Management**: Compress audio files if needed
3. **Caching**: Cache API responses to avoid redundant calls
4. **Batch Processing**: Group small segments if beneficial
5. **Resource Management**: Efficient temporary file handling

### Cost Management
- **Audio Duration**: Monitor total audio minutes processed
- **API Usage**: Track requests and associated costs
- **Optimization**: Reduce audio quality if acceptable for transcription

## Integration Points

### Previous Stage: Speech Segmentation
- **Input**: Segment manifest JSON and audio files from GCS
- **Dependencies**: GCS audio segments and metadata

### Next Stage: Translation
- **Output**: Structured transcription data with text per segment
- **Format**: JSON with segment-level transcriptions for translation

### Database Integration
- **Logging**: Detailed processing logs per segment
- **Status Tracking**: Real-time transcription progress
- **API Tracking**: Request IDs and usage monitoring

## Testing Strategy

### Unit Tests
- OpenAI API integration and error handling
- Audio file download and cleanup
- Result formatting and serialization

### Integration Tests
- End-to-end transcription workflow
- Parallel processing with real segments
- Database integration and logging

### Performance Tests
- API rate limiting and backoff behavior
- Processing time per segment size
- Cost analysis and optimization

## Monitoring and Metrics

### Key Metrics
- **Throughput**: Segments processed per minute (limited by API)
- **Success Rate**: Percentage of successful API calls
- **Latency**: API response time per segment
- **Cost**: API usage costs per video
- **Error Rate**: Failed transcription percentage

### Alerting
- **API Errors**: High error rate or quota exhaustion
- **Queue Backup**: Alert if queue depth exceeds rate limit capacity
- **Cost Monitoring**: Unexpected high API usage
- **Worker Health**: Monitor worker availability and performance

## Dependencies

### Required Packages
```txt
openai==1.3.0
aiofiles==23.2.1
asyncio-throttle==1.0.2
```

### System Requirements
- **CPU**: Standard compute (no GPU needed for API calls)
- **Memory**: Minimal (only for temporary file storage)
- **Network**: Reliable internet for API calls
- **Storage**: Temporary storage for audio file downloads

## Configuration

### Environment Variables
```bash
OPENAI_API_KEY=sk-...
MAX_CONCURRENT_TRANSCRIPTIONS=10
TRANSCRIPTION_TIMEOUT_MINUTES=5
OPENAI_RATE_LIMIT_RPM=50
ENABLE_API_RETRY=true
MAX_RETRY_ATTEMPTS=3
```

### Cost Estimation
- **Pricing**: $0.006 per minute of audio
- **Example**: 33 segments × 5.05 minutes average = 166.65 minutes ≈ $1.00 per video

This transcription stage leverages OpenAI's cloud-based Whisper API for accurate, scalable speech-to-text conversion while managing costs and API rate limits effectively.