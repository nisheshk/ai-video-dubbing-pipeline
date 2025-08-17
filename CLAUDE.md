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
- **Database**: Neon (Serverless PostgreSQL)
- **Storage**: Google Cloud Storage (GCS)
- **Audio Processing**: FFmpeg, Silero VAD
- **Speech Recognition**: WhisperX/Whisper
- **Translation**: Google Translate v3, GPT-4o
- **Voice Synthesis**: ElevenLabs (Voice Cloning + TTS)
- **Orchestration**: Temporal workflows

## Development Guidelines

### Code Style
- Follow existing patterns and conventions in the codebase
- Use established libraries and frameworks already present
- Implement proper error handling and retry logic
- Ensure idempotent operations for workflow reliability

### Testing
- Test each pipeline stage independently
- Validate end-to-end flows with sample videos
- Check API rate limiting and error scenarios
- Verify audio/video sync accuracy

### Common Commands
```bash
# Audio extraction
ffmpeg -i input.mp4 -ac 1 -ar 16000 audio.wav

# Run tests (when implemented)
# Add specific test commands here

# Build and lint (when implemented)  
# Add specific build/lint commands here
```

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
- Process segments in parallel for performance
- Implement proper rate limiting for external APIs
- Store intermediate artifacts in GCS for debugging and reprocessing
- Use Neon database to track processing status and store asset links
- Use deterministic file naming for idempotent operations
- Handle failures gracefully with segment-level retries
- Leverage Google Cloud ecosystem for seamless service integration