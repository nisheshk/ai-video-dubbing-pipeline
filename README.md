# AI Video Dubbing Project

## Project Overview
This is an end-to-end AI dubbing system that transforms foreign-language videos into dubbed versions with multiple audio tracks. The system processes videos through a 10-stage pipeline from audio extraction to final deliverable generation.

## Architecture
See `ARCHITECTURE.md` for complete technical architecture documentation including:
- Detailed pipeline stages (Audio Extraction � Final Deliverables)
- Technology stack and tool choices
- Supporting infrastructure requirements
- Data flow diagrams and performance considerations

## Key Technologies
- **Orchestration**: Temporal workflows (async)
- **Database**: Neon (Serverless PostgreSQL) with AsyncPG
- **Storage**: Google Cloud Storage (GCS) with async operations
- **Audio Processing**: FFmpeg with async subprocess, Silero VAD
- **Speech Recognition**: WhisperX/Whisper
- **Translation**: Google Translate v3, GPT-4o
- **Voice Synthesis**: ElevenLabs (Voice Cloning + TTS)
- **Data Models**: Pydantic for type safety

## Development Guidelines

### Code Style
- Follow existing patterns and conventions in the codebase
- Use established libraries and frameworks already present
- Implement proper error handling and retry logic
- Ensure idempotent operations for workflow reliability
- **Comments**: Use single-line format that is clear, concise, and professional

### Testing
- Test each pipeline stage independently
- Validate end-to-end flows with sample videos
- Check API rate limiting and error scenarios
- Verify audio/video sync accuracy

### Common Commands

#### Local Development
```bash
# Start Temporal server
temporal server start-dev

# Run local worker
python worker_local.py

# Install dependencies
pip install -r requirements.txt

# Audio extraction (direct FFmpeg)
ffmpeg -i input.mp4 -ac 1 -ar 16000 audio.wav
```

#### Temporal Cloud Operations
```bash
# Start cloud worker (processes workflows from Temporal Cloud)
source .env.cloud && python worker_cloud.py

# Trigger workflow for test video (a82c5c2a-3099-476d-937b-caf03bcc4043)
source .env.cloud && echo "1" | python client_cloud.py

# Trigger workflow and wait for completion
source .env.cloud && echo -e "1\n1" | python client_cloud.py

# Environment setup for cloud operations
cp .env.test .env.cloud  # Copy and customize cloud credentials
```

#### Temporal Workflow Operations
```bash
# Submit audio extraction workflow
python -c "import asyncio; from workflows.audio_extraction_workflow import AudioExtractionWorkflow; asyncio.run(submit_workflow())"

# Query workflow status
temporal workflow show --workflow-id <workflow-id>

# List running workflows
temporal workflow list
```

#### Testing and Development
```bash
# Install test dependencies
pip install -r requirements.txt

# Run all tests
pytest

# Run specific test categories
pytest -m unit          # Unit tests only (mocked)
pytest -m activity      # Activity tests only
pytest -m integration   # Integration tests (require FFmpeg, real files)
pytest -m "not integration"  # Skip integration tests (CI/fast testing)

# Run tests with coverage
pytest --cov=activities --cov=workflows --cov=shared --cov=config

# Run tests in parallel
pytest -n auto

# Run specific test files
pytest tests/activities/test_audio_extraction.py -v      # Unit tests (mocked)
pytest tests/activities/test_audio_extraction_real.py -v # Real file tests

# Run integration tests with specific environment
pytest tests/activities/test_audio_extraction_real.py -v --env-file .env.test

# Format code
black .

# Type checking
mypy .

# Linting
flake8 .
```

#### Test Structure
```
tests/
├── conftest.py                        # Test configuration and fixtures
├── activities/                        # Activity tests
│   ├── test_audio_extraction.py       # Unit tests (mocked GCS/DB)
│   └── test_audio_extraction_real.py  # Integration tests (real files/FFmpeg)
├── workflows/                         # Workflow tests
├── shared/                            # Shared component tests
│   └── test_config.py
└── fixtures/                          # Test data and files
```

#### Test Categories

**Unit Tests** (`test_audio_extraction.py`):
- Fast, isolated tests with mocked dependencies
- No external dependencies (GCS, FFmpeg, real files)
- Good for CI/CD and rapid development

**Integration Tests** (`test_audio_extraction_real.py`):
- Real GCS bucket operations and FFmpeg processing
- Uses existing test video: `gs://dubbing-pipeline/a82c5c2a-3099-476d-937b-caf03bcc4043/test_video1.mp4`
- Requires FFmpeg installation and GCS credentials
- Slower but tests real-world scenarios with actual cloud storage
- Environment variables: `GOOGLE_CLOUD_CREDENTIALS` (JSON) or `GOOGLE_APPLICATION_CREDENTIALS` (file path)
- Real PostgreSQL database logging (no primary key constraints for testing)

## Pipeline Architecture

### Main Orchestrator: reset_and_run_pipeline.py
- Starts 14 worker processes across 5 different worker types
- Executes 6 sequential workflow stages
- Handles database setup and teardown

### Worker Distribution
1. **worker_cloud.py** (3 instances) - Handles:
   - Audio extraction activities
   - Speech segmentation activities
   - Audio alignment activities
   - Audio stitching activities
2. **worker_transcription.py** (3 instances) - Dedicated transcription processing
3. **worker_translation.py** (3 instances) - Dedicated translation processing
4. **worker_voice_synthesis.py** (3 instances) - Dedicated voice synthesis processing
5. **worker_consolidation.py** (2 instances) - Handles workflow consolidation

### Workflow Files Used
- audio_extraction_workflow.py - Stage 1
- speech_segmentation_workflow.py - Stage 2
- transcription_workflow.py - Stage 3
- translation_workflow.py - Stage 4
- voice_synthesis_workflow.py - Stage 5
- alignment_stitching_workflow.py - Stage 6

## Pipeline Stages Summary
1. **Audio Extraction** - Extract clean audio from video
2. **Speech Segmentation** - Detect speech boundaries (VAD)
3. **Transcription** - Convert speech to text (ASR)
4. **Translation** - Translate to target language (MT)
5. **Voice Cloning** - Create speaker voice profiles
6. **Text-to-Speech** - Generate dubbed audio
7. **Alignment & Pacing** - Sync timing with original
8. **Stitch & Mix** - Assemble continuous audio track
9. **Mux** - Combine into final video with multiple tracks
10. **Subtitles** - Generate text tracks for accessibility

## Development Notes

### Async/Await Patterns
- All activities and workflows use async/await for non-blocking operations
- Database operations use AsyncPG connection pooling
- File operations use aiofiles for async I/O
- GCS operations wrapped in asyncio.to_thread() for thread-safe execution
- FFmpeg subprocess operations use asyncio.create_subprocess_exec()

### Temporal Workflow Patterns
- Activities are stateless and idempotent
- Workflows handle orchestration and state management
- Retry policies configured at activity level
- Signal handling for workflow monitoring and cancellation
- Proper timeout handling for long-running operations

### Error Handling
- Activities log errors to database with structured error details
- Workflows implement graceful failure handling with cleanup
- Retry policies with exponential backoff for transient failures
- Circuit breaker patterns for external service calls

### Performance Optimization
- Process segments in parallel for performance
- Connection pooling for database operations
- Async file operations to prevent blocking
- Resource cleanup in finally blocks
- Memory-efficient streaming for large files

### Production Considerations
- Store intermediate artifacts in GCS for debugging and reprocessing
- Use Neon database to track processing status and store asset links
- Use deterministic file naming for idempotent operations
- Handle failures gracefully with segment-level retries
- Implement health checks for all external dependencies
- Structured logging for monitoring and alerting
- Leverage Google Cloud ecosystem for seamless service integration
