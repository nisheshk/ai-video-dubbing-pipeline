# Stage 7: Alignment & Pacing + Stage 8: Audio Stitching & Video Muxing

## Overview
This combined stage handles both audio alignment/pacing (Stage 7) and audio stitching/video muxing (Stage 8). It ensures that synthesized audio segments are precisely time-aligned with the original video timing, then stitched together and muxed with the original video to create the final dubbed output.

## CRITICAL TIMING REQUIREMENTS
⚠️ **Input and output segments MUST have identical timing**:
- Original video segment: `start_time=10.5s, end_time=15.2s` → Duration: `4.7s`
- Synthesized audio segment: `actual_duration=3.8s` → **MUST BE STRETCHED** to `4.7s`
- Final dubbed segment: `start_time=10.5s, end_time=15.2s` → Duration: `4.7s` ✅

## Core Functionality

### Stage 7: Audio Alignment Process
1. **Load Timing Data**: Extract original segment boundaries from `segments` table
2. **Load Synthesized Audio**: Retrieve TTS audio files from `voice_synthesis` table
3. **Calculate Stretch Ratio**: `stretch_ratio = original_duration / synthesized_duration`
4. **Time-Stretch Audio**: Use FFmpeg to match exact original timing
5. **Validate Timing**: Ensure aligned audio duration matches original ±50ms
6. **Store Aligned Audio**: Save time-aligned segments to GCS

### Stage 8: Audio Stitching & Video Muxing Process
1. **Concatenate Segments**: Combine aligned audio segments in chronological order
2. **Insert Silence Gaps**: Add exact silence between segments from original timing
3. **Generate Final Audio**: Create continuous dubbed audio track
4. **Extract Video Stream**: Remove original audio from video, keep video only
5. **Mux Audio + Video**: Combine dubbed audio with original video stream
6. **Quality Validation**: Verify final video duration matches original exactly

### Key Technologies

| Component | Tool/Service | Alternative | Purpose |
|-----------|-------------|-------------|----------|
| Time-Stretching | FFmpeg `atempo` | SoX `tempo` | Stretch audio without pitch change |
| Audio Concatenation | FFmpeg `concat` | SoX `concatenate` | Combine segments seamlessly |
| Video Muxing | FFmpeg | - | Combine video + audio streams |
| Timing Validation | Custom Logic | - | Ensure precise alignment |
| Silence Generation | FFmpeg `anullsrc` | SoX `pad` | Create exact silence gaps |

## Technical Implementation

### Input Data Sources
- **Database Tables**:
  - `segments`: Original timing boundaries (`start_time`, `end_time`, `duration`)
  - `voice_synthesis`: Synthesized audio file paths (`gcs_audio_path`, `audio_duration_seconds`)
  - `videos`: Original video file location (`gcs_input_path`)
- **GCS Storage**:
  - Original video file
  - Individual synthesized audio segments (from TTS)
- **Computed Data**:
  - Stretch ratios for each segment
  - Silence gap durations between segments

### Processing Steps

#### 1. Data Loading & Validation
```python
# Load segments ordered by start_time from database
segments = await db.get_segments_for_video(video_id, order_by='start_time')
voice_data = await db.get_voice_synthesis_for_video(video_id)

# Validate data completeness
assert len(segments) == len(voice_data), "Segment count mismatch"
for segment, voice in zip(segments, voice_data):
    assert segment['id'] == voice['segment_id'], "Segment ID mismatch"
```

#### 2. Timing Analysis & Stretch Calculation
```python
alignment_data = []
for segment, voice in zip(segments, voice_data):
    original_duration = segment['end_time'] - segment['start_time']
    synthesized_duration = voice['audio_duration_seconds']
    stretch_ratio = original_duration / synthesized_duration
    
    # Validate stretch ratio is within acceptable bounds
    if not 0.5 <= stretch_ratio <= 2.0:
        raise ValueError(f"Stretch ratio {stretch_ratio} outside safe range")
    
    alignment_data.append({
        'segment_id': segment['id'],
        'original_duration': original_duration,
        'synthesized_duration': synthesized_duration,
        'stretch_ratio': stretch_ratio,
        'start_time': segment['start_time'],
        'end_time': segment['end_time']
    })
```

#### 3. Audio Time-Stretching (Per Segment)
```bash
# FFmpeg time-stretching with quality preservation
ffmpeg -i synthesized_segment.wav \
       -filter:a "atempo=${stretch_ratio}" \
       -acodec pcm_s16le \
       -ar 44100 \
       aligned_segment.wav

# Validate output duration matches target
aligned_duration=$(ffprobe -i aligned_segment.wav -show_entries format=duration -v quiet -of csv="p=0")
target_duration=${original_duration}
# Assert: |aligned_duration - target_duration| < 0.05  # 50ms tolerance
```

#### 4. Silence Gap Calculation & Generation
```python
# Calculate silence gaps between consecutive segments
silence_segments = []
for i in range(len(segments) - 1):
    current_end = segments[i]['end_time']
    next_start = segments[i+1]['start_time'] 
    gap_duration = next_start - current_end
    
    if gap_duration > 0.01:  # Only add silence if gap > 10ms
        silence_segments.append({
            'start_time': current_end,
            'duration': gap_duration,
            'type': 'silence'
        })
```

#### 5. Audio Stitching & Concatenation
```bash
# Create FFmpeg concat file with aligned segments and silence
cat > concat_list.txt << EOF
file 'aligned_segment_001.wav'
file 'silence_gap_001.wav'
file 'aligned_segment_002.wav'
file 'silence_gap_002.wav'
...
EOF

# Concatenate all segments into final dubbed audio
ffmpeg -f concat -safe 0 -i concat_list.txt \
       -acodec pcm_s16le \
       -ar 44100 \
       final_dubbed_audio.wav
```

#### 6. Video Muxing
```bash
# Extract video stream without audio
ffmpeg -i original_video.mp4 -an -vcodec copy video_only.mp4

# Combine video with dubbed audio (replace original audio)
ffmpeg -i video_only.mp4 -i final_dubbed_audio.wav \
       -c:v copy -c:a aac -b:a 192k \
       -map 0:v:0 -map 1:a:0 \
       final_dubbed_video.mp4

# Optional: Create multi-track version (original + dubbed)
ffmpeg -i original_video.mp4 -i final_dubbed_audio.wav \
       -c:v copy -c:a aac -b:a 192k \
       -map 0:v:0 -map 0:a:0 -map 1:a:0 \
       -metadata:s:a:0 language=eng -metadata:s:a:1 language=spa \
       final_video_multitrack.mp4
```

### Output Artifacts
- **Aligned Audio Segments**: Time-synchronized individual segments in GCS
- **Final Dubbed Audio**: Complete continuous audio track
- **Final Dubbed Video**: Video with synchronized dubbed audio
- **Multi-track Video**: Optional version with both original and dubbed audio
- **Alignment Metadata**: Timing validation and quality metrics stored in database

## Database Schema Updates

### Audio Alignments Table
```sql
CREATE TABLE IF NOT EXISTS audio_alignments (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    video_id UUID NOT NULL,
    segment_id UUID NOT NULL REFERENCES segments(id),
    voice_synthesis_id UUID NOT NULL REFERENCES voice_synthesis(id),
    
    -- Timing Data
    original_start_time REAL NOT NULL,
    original_end_time REAL NOT NULL, 
    original_duration REAL NOT NULL,
    synthesized_duration REAL NOT NULL,
    stretch_ratio REAL NOT NULL,
    
    -- Alignment Results
    aligned_audio_gcs_path TEXT NOT NULL,
    aligned_duration REAL NOT NULL,
    timing_accuracy_ms REAL,  -- Deviation from target timing
    
    -- Quality Metrics
    alignment_quality_score REAL CHECK (alignment_quality_score >= 0 AND alignment_quality_score <= 1),
    audio_distortion_score REAL,  -- Measure of time-stretching artifacts
    
    status TEXT DEFAULT 'pending',
    error_message TEXT,
    processing_time_seconds REAL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Final Dubbed Videos Table
CREATE TABLE IF NOT EXISTS dubbed_videos (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id),
    
    -- Output Files
    final_audio_gcs_path TEXT NOT NULL,
    final_video_gcs_path TEXT NOT NULL,
    multitrack_video_gcs_path TEXT,  -- Optional multi-track version
    
    -- Timing Validation
    original_video_duration REAL NOT NULL,
    dubbed_video_duration REAL NOT NULL,
    duration_accuracy_ms REAL,  -- Should be near 0
    
    -- Quality Metrics
    overall_sync_score REAL CHECK (overall_sync_score >= 0 AND overall_sync_score <= 1),
    segments_processed INTEGER NOT NULL,
    segments_successful INTEGER NOT NULL,
    avg_stretch_ratio REAL,
    
    processing_time_seconds REAL,
    status TEXT DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Performance Considerations

### Optimization Strategies
- **Parallel Processing**: Process segments concurrently
- **Chunk-based Processing**: Handle long segments in smaller chunks
- **Quality Thresholds**: Skip alignment for segments with minimal timing differences
- **Caching**: Store aligned segments for reuse

### Resource Management
- **Memory Usage**: Stream large audio files to prevent memory exhaustion
- **CPU Utilization**: Balance time-stretching quality vs processing speed
- **Storage Efficiency**: Clean up intermediate alignment files

## Quality Assurance

### Alignment Accuracy
- **Timing Precision**: ±50ms alignment tolerance
- **Audio Quality**: Minimize artifacts from time-stretching
- **Sync Validation**: Verify audio-visual synchronization

### Error Handling
- **Stretch Ratio Limits**: Prevent excessive time-stretching (0.5x - 2.0x range)
- **Quality Degradation**: Fallback strategies for poor alignment
- **Segment Recovery**: Handle individual segment alignment failures

## Integration Points

### Input Dependencies (Required for Processing)
- **Stage 2 (Speech Segmentation)**: Original segment timing boundaries in `segments` table
- **Stage 6 (Voice Synthesis)**: Synthesized audio files stored in GCS with paths in `voice_synthesis` table  
- **Original Video**: Source video file for final muxing (from `videos.gcs_input_path`)

### Output Deliverables
- **Final Dubbed Video**: Complete video with synchronized dubbed audio track
- **Optional Multi-track Video**: Video with both original and dubbed audio tracks
- **Alignment Metadata**: Quality metrics and timing validation data in database
- **Pipeline Completion**: Ready for Stage 9 (Subtitles) or final delivery

## Monitoring & Metrics

### Key Performance Indicators
- **Timing Accuracy**: Average deviation from target timing (target: <50ms)
- **Duration Match**: Final video duration vs original (target: <100ms difference)
- **Stretch Ratio Distribution**: Percentage of segments within safe range (0.5x - 2.0x)
- **Processing Speed**: Segments processed per minute
- **Success Rate**: Percentage of segments successfully aligned and stitched

### Alerting Thresholds  
- **CRITICAL**: Timing accuracy >100ms (audio-video sync issues)
- **CRITICAL**: Final video duration difference >500ms
- **WARNING**: >10% of segments exceed safe stretch ratio limits
- **WARNING**: Processing time >3x real-time duration
- **ERROR**: Segment failure rate >5%

### Quality Validation Checklist
✅ **Timing Validation**: Each aligned segment duration matches original ±50ms  
✅ **Continuity Check**: No audio gaps or overlaps between segments  
✅ **Duration Match**: Final video duration equals original ±100ms  
✅ **Audio Quality**: No audible artifacts from time-stretching  
✅ **Sync Verification**: Audio-visual synchronization maintained  
✅ **File Integrity**: Final video file is playable and uncorrupted