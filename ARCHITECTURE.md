# AI Video Dubbing System Architecture

## Overview

This document describes the architecture for an end-to-end AI dubbing pipeline that transforms foreign-language videos into dubbed versions with multiple audio tracks. The system processes raw video through 10 distinct stages, from audio extraction to final deliverable generation.

## System Flow

```
Raw Video → Audio Extraction → Speech Segmentation → Transcription → Translation → 
Voice Cloning → Text-to-Speech → Alignment & Pacing → Stitch & Mix → Mux → Final Video
```

---

## Pipeline Stages

### 1. Audio Extraction

**Purpose**: Extract audio track from source video  
**Input**: Raw video file  
**Tool**: FFmpeg  
**Process**: Extract mono, 16 kHz WAV (optimized for ASR)  
**Output**: `audio.wav`  

**Why**: Clean audio separation is essential for all downstream speech analysis tasks.

### 2. Speech Segmentation (VAD - Voice Activity Detection)

**Purpose**: Detect speech boundaries and ignore silence/music  
**Input**: `audio.wav`  
**Tool**: Silero VAD (robust, CPU-efficient)  
**Process**: Identify speech start/stop timestamps  
**Output**: JSON segment list: `{id, tStart, tEnd}`  

**Why**: Long videos must be chunked for easier processing, parallelism, and retry handling.

### 3. Transcription (ASR - Automatic Speech Recognition)

**Purpose**: Convert audio segments to source language text with timestamps  
**Input**: Audio segments from VAD  
**Tool**: Whisper/WhisperX (word-level timestamps, multilingual support)  
**Process**: Generate transcripts for each segment  
**Output**: Transcript JSON per segment: `{text, words[], speaker?, tStart, tEnd}`  

**Why**: Provides the foundation script for translation, dubbing, and subtitle generation.

### 4. Translation (MT - Machine Translation)

**Purpose**: Convert source language text to target language  
**Input**: Source language transcripts  
**Tools**: 
- **Step 1**: Google Translate v3 (bulk translation, glossary support)
- **Step 2**: GPT-4o (polish critical segments for tone & fluency)  
**Process**: Two-stage translation balancing scale with quality  
**Output**: Target-language transcript per segment  

**Why**: Combines Google's scale/cost efficiency with GPT's style quality.

### 5. Voice Cloning

**Purpose**: Create digital voice profiles from original speakers  
**Input**: Clean speech samples (60-120 seconds per speaker)  
**Tool**: ElevenLabs Voice Cloning API  
**Process**: Upload speaker samples to generate voice embeddings  
**Output**: Voice IDs (speaker embeddings)  

**Why**: Maintains each speaker's vocal identity in the dubbed version.

### 6. Text-to-Speech (TTS)

**Purpose**: Generate target-language speech from translated text  
**Input**: Target-language transcripts + Voice IDs  
**Tool**: ElevenLabs TTS (multilingual, cloned voices)  
**Process**: Generate speech for each segment using cloned voices  
**Output**: WAV files per segment (20-40s duration)  

**Why**: Produces the actual dubbed audio that replaces original speech.

### 7. Alignment & Pacing

**Purpose**: Ensure TTS clips fit exactly into original time slots  
**Input**: TTS segments + original timing constraints  
**Tools**:
- aeneas / Montreal Forced Aligner / WhisperX (forced alignment)
- FFmpeg atempo (speed adjustment ±5-8%)
- Silence padding & crossfades  
**Process**: Time-align and pace-adjust generated speech  
**Output**: Conformed per-segment WAVs  

**Why**: Prevents sync drift between audio and video.

### 8. Stitch & Mix

**Purpose**: Assemble segment WAVs into continuous dubbed track  
**Input**: Time-aligned segment WAVs  
**Tools**:
- FFmpeg concat (join clips)
- FFmpeg sidechaincompress (preserve background ambience/music)  
**Process**: Create seamless full-length audio track  
**Output**: Complete dubbed audio track (`dubbed.m4a`)  

**Why**: Final audio must run continuously for the entire video duration.

### 9. Mux (Multiplexing)

**Purpose**: Combine video, original audio, dubbed audio, and subtitles  
**Input**: Original video + original audio + dubbed audio + subtitles  
**Tool**: FFmpeg (multi-track audio, language tagging)  
**Process**: Create single container with multiple audio streams  
**Output**: `final.mp4` with switchable audio tracks  

**Why**: Delivers single file where users can choose audio language.

### 10. Subtitles

**Purpose**: Provide text tracks for accessibility and searchability  
**Input**: Source and target language transcripts  
**Tools**: Whisper output → SRT conversion  
**Process**: Generate subtitle files in both languages  
**Output**: `original.srt`, `dubbed.srt`  

**Why**: Enhances accessibility, SEO, and user experience.

---

## Supporting Infrastructure

### Database (Neon PostgreSQL)
- **Platform**: Neon (Serverless PostgreSQL)
- **Purpose**: Store video metadata, processing status, and generated asset links
- **Schema**:
  - `videos`: video metadata, source/target languages, processing status
  - `segments`: individual segment data, timestamps, processing states
  - `assets`: links to generated files (audio, subtitles, final video)
  - `speakers`: voice cloning data and ElevenLabs voice IDs
- **Benefits**: Serverless scaling, branching for dev/staging, instant provisioning

### Storage Architecture
- **Platform**: Google Cloud Storage (GCS)
- **Structure**: `gs://dubbing-pipeline/{videoId}/{segId}/...`
- **Strategy**: Store all intermediate artifacts for debugging and reprocessing
- **Integration**: Native integration with Google Translate API and other Google services

### Workflow Orchestration (Temporal)
- **Fan-out Processing**: Handle 100+ segments in parallel
- **Retry Logic**: Reprocess only failed segments (idempotent operations)
- **Progress Tracking**: Real-time status ("45/120 segments completed")
- **Rate Limiting**: Respect API quotas (ElevenLabs, Google Translate)

### Idempotency
- **Deterministic Naming**: Prevent duplicate work on retries
- **Checkpointing**: Resume from any stage on failure
- **State Management**: Track processing status per segment

---

## Technology Stack

| Stage | Primary Tool | Alternative | Rationale |
|-------|-------------|-------------|-----------|
| Audio Extraction | FFmpeg | - | Industry standard, reliable |
| VAD | Silero VAD | WebRTC VAD | Better accuracy, CPU efficient |
| ASR | WhisperX | Whisper | Word-level timestamps |
| Translation | Google Translate v3 | DeepL API | Scale + cost efficiency |
| Polish | GPT-4o | Claude | Style and fluency |
| Voice Cloning | ElevenLabs | - | Best multilingual quality |
| TTS | ElevenLabs | Azure Speech | Natural voice synthesis |
| Alignment | aeneas | Montreal FA | Reliable forced alignment |
| Audio Processing | FFmpeg | - | Complete audio toolkit |

---

## Final Deliverables

### Primary Output: `final.mp4`
- **Video Stream**: Original (unchanged)
- **Audio Track 1**: Original language
- **Audio Track 2**: Dubbed track  
- **Subtitle Tracks**: Source + target language

### Additional Assets
- `original.srt` - Source language subtitles
- `dubbed.srt` - Target language subtitles  
- `metadata.json` - QA data (timings, texts, quality scores)

### Quality Assurance Data
- Segment-level processing metadata (stored in Neon DB)
- Source/target text pairs (with revision history)
- Timing alignment accuracy scores
- Voice quality metrics
- Asset URLs and processing logs

---

## Performance Considerations

- **Parallel Processing**: Segment-level parallelization for 10x+ speed improvement
- **API Rate Limiting**: Intelligent queuing for third-party services
- **Memory Management**: Stream processing for large video files
- **Error Recovery**: Granular retry mechanisms at segment level
- **Scalability**: Horizontal scaling via containerized workers

---

## Data Flow Summary

```
Input Video (.mp4)
    ↓
Audio Extraction (FFmpeg)
    ↓
Speech Segmentation (Silero VAD)
    ↓
Transcription (WhisperX) → Translation (Google + GPT)
    ↓                          ↓
Voice Analysis              Target Text
    ↓                          ↓
Voice Cloning (ElevenLabs) → TTS Generation
    ↓
Timing Alignment (aeneas)
    ↓
Audio Assembly (FFmpeg)
    ↓
Final Multiplexing
    ↓
Multi-track Video Output
```

This architecture provides a robust, scalable foundation for high-quality AI dubbing with support for multiple languages, speakers, and output formats.