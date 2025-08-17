# AI Dubbing Pipeline - Implementation Tasks

This document outlines the specific implementation tasks for each step in the AI dubbing pipeline.

## Pipeline Steps Overview

1. ~~Input Video Upload~~ (Skip - assuming file exists in GCS)
2. **Audio Extraction** ← Current Focus
3. Speech Segmentation (VAD)
4. Transcription (ASR)
5. Translation (MT)
6. Voice Cloning
7. Text-to-Speech (TTS)
8. Alignment & Pacing
9. Stitch & Mix
10. Mux (Multiplexing)
11. Subtitles

---

## Step 2: Audio Extraction

### Overview
Extract clean audio track from source video stored in Google Cloud Storage and prepare it for speech processing.

### Prerequisites
- Video file already exists in GCS bucket: `gs://dubbing-pipeline/{videoId}/input/video.mp4`
- Access to Google Cloud Storage
- FFmpeg installed and configured

### Task Breakdown

#### 2.1 GCS Integration Setup
**Goal**: Establish connection to Google Cloud Storage
- [ ] Install and configure Google Cloud SDK
- [ ] Set up authentication (service account or ADC)
- [ ] Create GCS client for file operations
- [ ] Test bucket access and permissions

#### 2.2 Video File Download
**Goal**: Securely download video from GCS for processing
- [ ] Implement GCS file download function
- [ ] Add error handling for network failures
- [ ] Implement retry logic with exponential backoff
- [ ] Validate video file integrity after download
- [ ] Create local temporary storage for processing

#### 2.3 Audio Extraction Implementation
**Goal**: Extract mono 16kHz WAV audio optimized for ASR
- [ ] Install FFmpeg dependencies
- [ ] Implement audio extraction function using FFmpeg
- [ ] Configure extraction parameters:
  - Format: WAV
  - Channels: Mono (1 channel)
  - Sample rate: 16kHz
  - Bit depth: 16-bit
- [ ] Add error handling for corrupted video files
- [ ] Validate extracted audio quality

#### 2.4 Database Integration
**Goal**: Track processing status in Neon PostgreSQL
- [ ] Set up Neon database connection
- [ ] Create database schema for tracking:
  ```sql
  CREATE TABLE videos (
    id UUID PRIMARY KEY,
    original_filename TEXT,
    gcs_input_path TEXT,
    gcs_audio_path TEXT,
    status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
  );
  
  CREATE TABLE processing_logs (
    id UUID PRIMARY KEY,
    video_id UUID REFERENCES videos(id),
    step TEXT,
    status TEXT,
    message TEXT,
    created_at TIMESTAMP
  );
  ```
- [ ] Implement status tracking functions
- [ ] Log processing start/completion/errors

#### 2.5 Output Storage
**Goal**: Store extracted audio back to GCS in organized structure
- [ ] Upload extracted audio to GCS
- [ ] Organize files: `gs://dubbing-pipeline/{videoId}/audio/extracted.wav`
- [ ] Update database with audio file path
- [ ] Clean up local temporary files
- [ ] Implement file cleanup on errors

#### 2.6 Error Handling & Validation
**Goal**: Robust error handling and quality checks
- [ ] Validate input video format and codec support
- [ ] Check audio track existence in video
- [ ] Verify extracted audio duration matches video
- [ ] Handle edge cases:
  - Videos without audio tracks
  - Corrupted video files
  - Insufficient disk space
  - Network timeouts
- [ ] Implement comprehensive logging

#### 2.7 Performance Optimization
**Goal**: Optimize for speed and resource usage
- [ ] Stream processing for large files (avoid full download)
- [ ] Parallel processing for multiple videos
- [ ] Memory usage optimization
- [ ] Progress tracking for long operations

### Implementation Notes

#### Technology Stack
- **Language**: Python (recommended) or Node.js
- **GCS Client**: `google-cloud-storage` library
- **Audio Processing**: FFmpeg via subprocess or `ffmpeg-python`
- **Database**: `psycopg2` or `asyncpg` for PostgreSQL
- **Environment**: Docker container for consistent FFmpeg version

#### File Structure
```
gs://dubbing-pipeline/
├── {videoId}/
│   ├── input/
│   │   └── video.mp4
│   ├── audio/
│   │   └── extracted.wav
│   └── logs/
│       └── audio_extraction.log
```

#### Success Criteria
- [ ] Audio successfully extracted from GCS video
- [ ] Output audio is mono, 16kHz WAV format
- [ ] Duration matches source video ±1 second
- [ ] Files properly organized in GCS
- [ ] Database accurately tracks processing status
- [ ] Process handles errors gracefully
- [ ] Memory usage stays within reasonable limits