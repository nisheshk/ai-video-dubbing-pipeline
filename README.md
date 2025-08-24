# AI Video Dubbing Project

## Project Overview
This is an end-to-end AI dubbing system that transforms foreign-language videos into dubbed versions with multiple audio tracks. The system processes videos through a 10-stage pipeline from audio extraction to final deliverable generation.

## Architecture
See `ARCHITECTURE.md` for complete technical architecture documentation including:
- Detailed pipeline stages (Audio Extraction � Final Deliverables)


### Quick Start

#### Run Complete Pipeline
```bash
# Run with custom parameters:
# ./run_complete_pipeline.sh [gcs_folder_uuid] [video_file_name] [target_language]
./run_complete_pipeline.sh "your-video-id" "your-video-file.mp4" "fr"

# Example with specific video
# - 0770142a-1a3a-4c65-adef-cfd98b19d0d3: GCS bucket folder name (UUID)
# - test_video1.mp4: Video file name within that folder
# - es: Target language code (Spanish)
./run_complete_pipeline.sh 0770142a-1a3a-4c65-adef-cfd98b19d0d3 test_video1.mp4 es
```

### Environment Variables
Set up the following environment variables for proper operation:

```bash
# Google Cloud Storage Configuration
GOOGLE_CLOUD_PROJECT=
GCS_BUCKET_NAME=dubbing-pipeline

# GCS Authentication - JSON content as environment variable
GOOGLE_CLOUD_CREDENTIALS=

# Temporal Cloud Configuration
TEMPORAL_CLOUD_NAMESPACE=
TEMPORAL_CLOUD_ADDRESS=
TEMPORAL_CLOUD_API_KEY=

# Task Queue
TASK_QUEUE=dubbing-prod-queue

OPENAI_API_KEY=
REPLICATE_API_TOKEN=

# PostgreSQL Database Configuration (Local)
NEON_DATABASE_URL=
NEON_HOST=
NEON_DATABASE=
NEON_USERNAME=
NEON_PASSWORD=

# Processing Configuration
TEMP_STORAGE_PATH=/tmp/dubbing_test
MAX_FILE_SIZE_MB=500
AUDIO_SAMPLE_RATE=16000
AUDIO_CHANNELS=1

# FFmpeg Configuration
FFMPEG_PATH=
FFPROBE_PATH=

# Production settings
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO
MAX_RETRIES=3
TIMEOUT_SECONDS=300

# Worker Scaling Configuration - High Concurrency
MAX_CONCURRENT_ACTIVITIES=
MAX_CONCURRENT_WORKFLOW_TASKS=
MAX_CONCURRENT_ACTIVITY_TASKS=
```