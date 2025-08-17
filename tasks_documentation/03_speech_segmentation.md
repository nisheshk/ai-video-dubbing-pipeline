# AI Dubbing Pipeline - Speech Segmentation (VAD)

## Step 3: Speech Segmentation

### Overview
Detect speech boundaries and ignore silence/music using Voice Activity Detection (VAD). This step is crucial for breaking long audio files into manageable segments for parallel processing, improved retry handling, and efficient resource usage.

### Prerequisites
- Extracted audio file exists in GCS: `gs://dubbing-pipeline/{videoId}/audio/extracted.wav`
- Audio is in mono, 16kHz WAV format (from previous step)
- Temporal workflow orchestration setup
- Database schema for segment tracking

### Task Breakdown

#### 3.1 VAD Tool Selection and Setup
**Goal**: Choose and integrate Voice Activity Detection system
- [ ] Evaluate VAD options:
  - **Silero VAD** (recommended): Robust, CPU-efficient, multilingual
  - WebRTC VAD: Lightweight, basic functionality
  - py-webrtcvad: Python wrapper for WebRTC
  - speechbrain VAD: Deep learning based
- [ ] Install Silero VAD dependencies: `pip install silero-vad`
- [ ] Download pre-trained Silero models
- [ ] Test VAD on sample audio files
- [ ] Benchmark performance on different audio qualities

#### 3.2 Audio Preprocessing
**Goal**: Prepare audio for optimal VAD performance
- [ ] Implement audio loading and validation
- [ ] Add resampling if needed (ensure 16kHz)
- [ ] Apply pre-filtering for noise reduction (optional)
- [ ] Handle stereo-to-mono conversion edge cases
- [ ] Validate audio file integrity and format

#### 3.3 Speech Segmentation Implementation
**Goal**: Generate accurate speech/silence boundaries
- [ ] Implement VAD processing function
- [ ] Configure VAD parameters:
  ```python
  vad_config = {
      'threshold': 0.5,          # Speech probability threshold
      'min_speech_duration_ms': 250,    # Minimum speech segment
      'min_silence_duration_ms': 100,   # Minimum silence gap
      'window_size_samples': 1024,      # Processing window
      'speech_pad_ms': 30       # Padding around speech
  }
  ```
- [ ] Process audio in chunks for memory efficiency
- [ ] Generate segment timestamps with high precision
- [ ] Filter out very short segments (< 1 second)
- [ ] Merge nearby segments with small gaps

#### 3.4 Segment Post-Processing
**Goal**: Optimize segments for downstream processing
- [ ] Implement segment merging logic:
  - Combine segments separated by < 500ms
  - Split overly long segments (> 30 seconds)
  - Ensure minimum segment duration (2 seconds)
- [ ] Add segment boundary refinement
- [ ] Calculate segment statistics (duration, speech confidence)
- [ ] Generate segment IDs and metadata
- [ ] Validate segment boundaries don't exceed audio duration

#### 3.5 Database Schema and Storage
**Goal**: Track segment data for processing pipeline
- [ ] Extend database schema:
  ```sql
  CREATE TABLE segments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    video_id UUID NOT NULL,
    segment_index INTEGER NOT NULL,
    start_time FLOAT NOT NULL,
    end_time FLOAT NOT NULL,
    duration FLOAT NOT NULL,
    confidence_score FLOAT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(video_id, segment_index)
  );
  
  CREATE INDEX idx_segments_video_id ON segments(video_id);
  CREATE INDEX idx_segments_status ON segments(status);
  ```
- [ ] Implement segment CRUD operations
- [ ] Add batch insert for performance
- [ ] Track segment processing status

#### 3.6 Output Generation
**Goal**: Generate segment data for downstream processing
- [ ] Create JSON segment manifest:
  ```json
  {
    "video_id": "uuid",
    "total_duration": 3600.5,
    "total_segments": 45,
    "segments": [
      {
        "id": "uuid",
        "index": 0,
        "start_time": 0.5,
        "end_time": 8.2,
        "duration": 7.7,
        "confidence": 0.95,
        "audio_path": "gs://bucket/video_id/segments/000.wav"
      }
    ]
  }
  ```
- [ ] Extract individual segment audio files
- [ ] Upload segment files to GCS: `gs://dubbing-pipeline/{videoId}/segments/{index:03d}.wav`
- [ ] Generate segment manifest file
- [ ] Update database with segment records

#### 3.7 Quality Assurance and Validation
**Goal**: Ensure accurate and complete segmentation
- [ ] Implement segment validation checks:
  - No overlapping segments
  - No gaps in coverage (optional music/silence is OK)
  - Segment timing accuracy
  - Audio file integrity per segment
- [ ] Add quality metrics calculation:
  - Speech-to-silence ratio
  - Average segment duration
  - Confidence score distribution
- [ ] Implement visual debugging tools (optional):
  - Waveform visualization with segment boundaries
  - Confidence score plotting
- [ ] Generate QA report

#### 3.8 Error Handling and Edge Cases
**Goal**: Robust handling of problematic audio
- [ ] Handle audio files with no speech detected
- [ ] Manage extremely long silence periods
- [ ] Process low-quality or noisy audio gracefully
- [ ] Handle corrupted audio segments
- [ ] Implement recovery from partial processing failures
- [ ] Add comprehensive error logging

#### 3.9 Performance Optimization
**Goal**: Optimize for speed and scalability
- [ ] Implement streaming processing for large files
- [ ] Add GPU acceleration support (if available)
- [ ] Optimize memory usage for long audio files
- [ ] Implement progress tracking and reporting
- [ ] Add parallel processing for multiple audio files
- [ ] Cache VAD models for reuse

#### 3.10 Temporal Workflow Integration
**Goal**: Integrate with workflow orchestration system
- [ ] Implement VAD activity for Temporal workflow
- [ ] Add proper retry policies and timeouts
- [ ] Implement activity heartbeats for long-running operations
- [ ] Add workflow signals for progress updates
- [ ] Handle activity cancellation gracefully

### Implementation Notes

#### Technology Stack
- **VAD Engine**: Silero VAD (recommended)
- **Audio Processing**: librosa, soundfile, or pydub
- **Database**: AsyncPG for PostgreSQL integration
- **Storage**: Google Cloud Storage client
- **Workflow**: Temporal Python SDK

#### File Organization
```
gs://dubbing-pipeline/{videoId}/
├── audio/
│   └── extracted.wav          # Input from previous step
├── segments/
│   ├── manifest.json          # Segment metadata
│   ├── 000.wav               # Individual segments
│   ├── 001.wav
│   └── ...
└── logs/
    └── segmentation.log       # Processing logs
```

#### Configuration Parameters
```python
VAD_CONFIG = {
    # Silero VAD specific
    'model_name': 'silero_vad',
    'threshold': 0.5,
    'sampling_rate': 16000,
    
    # Segmentation logic
    'min_speech_duration_ms': 1000,   # 1 second minimum
    'max_segment_duration_s': 30,     # 30 seconds maximum
    'min_silence_gap_ms': 500,        # 500ms to separate segments
    'speech_padding_ms': 50,          # Padding around speech
    
    # Processing
    'chunk_size_ms': 10000,           # Process in 10-second chunks
    'overlap_ms': 1000,               # 1-second overlap between chunks
    'confidence_threshold': 0.3       # Minimum confidence for speech
}
```

### Success Criteria
- [ ] VAD accurately detects speech boundaries in test audio
- [ ] Segments are properly sized (1-30 seconds each)
- [ ] No speech content is lost in segmentation
- [ ] Segment files are correctly stored in GCS
- [ ] Database accurately tracks all segments
- [ ] Processing handles various audio qualities gracefully
- [ ] Memory usage remains efficient for long audio files
- [ ] Integration with Temporal workflow is seamless
- [ ] Error handling covers edge cases
- [ ] Performance meets requirements (< 2x real-time processing)

### Dependencies
- silero-vad
- torch (for Silero VAD)
- librosa or soundfile (audio processing)
- asyncpg (database)
- google-cloud-storage
- temporalio (workflow integration)

### Testing Strategy
1. **Unit Tests**: VAD function accuracy, segment validation
2. **Integration Tests**: End-to-end processing with sample videos
3. **Performance Tests**: Large file processing, memory usage
4. **Edge Case Tests**: Silent audio, noisy audio, very short/long files
5. **Quality Tests**: Speech boundary accuracy, segment completeness