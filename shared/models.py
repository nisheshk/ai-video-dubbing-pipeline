"""Shared data models for AI dubbing pipeline."""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field
from enum import Enum
import uuid


class ProcessingStatus(str, Enum):
    """Video processing status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AudioExtractionRequest(BaseModel):
    """Request for audio extraction from video."""
    
    video_id: str = Field(description="Unique video identifier")
    gcs_input_path: str = Field(description="GCS path to input video file")
    original_filename: Optional[str] = Field(default=None, description="Original video filename")
    source_language: Optional[str] = Field(default=None, description="Source video language")
    target_language: Optional[str] = Field(default=None, description="Target dubbing language")
    
    @classmethod
    def create(cls, gcs_input_path: str, **kwargs) -> "AudioExtractionRequest":
        """Create request with auto-generated video ID."""
        return cls(
            video_id=str(uuid.uuid4()),
            gcs_input_path=gcs_input_path,
            **kwargs
        )


class AudioExtractionResult(BaseModel):
    """Result from audio extraction process."""
    
    video_id: str = Field(description="Video identifier")
    success: bool = Field(description="Whether extraction succeeded")
    gcs_audio_url: str = Field(description="GCS URL of extracted audio")
    processing_time_seconds: float = Field(description="Total processing time")
    video_metadata: Dict[str, Any] = Field(default_factory=dict, description="Video file metadata")
    audio_metadata: Dict[str, Any] = Field(default_factory=dict, description="Extracted audio metadata")
    error_message: str = Field(default="", description="Error message if failed")
    
    # Processing step timings
    download_time_seconds: Optional[float] = Field(default=None)
    validation_time_seconds: Optional[float] = Field(default=None)
    extraction_time_seconds: Optional[float] = Field(default=None)
    upload_time_seconds: Optional[float] = Field(default=None)


class VideoMetadata(BaseModel):
    """Video file metadata."""
    
    duration: float = Field(description="Video duration in seconds")
    size_bytes: int = Field(description="File size in bytes")
    format_name: str = Field(description="Video format")
    video_codec: str = Field(description="Video codec")
    video_width: int = Field(description="Video width in pixels")
    video_height: int = Field(description="Video height in pixels")
    video_fps: float = Field(description="Video frame rate")
    audio_codec: str = Field(description="Audio codec")
    audio_sample_rate: int = Field(description="Audio sample rate")
    audio_channels: int = Field(description="Audio channel count")
    audio_bitrate: int = Field(description="Audio bitrate")


class AudioMetadata(BaseModel):
    """Extracted audio file metadata."""
    
    duration: float = Field(description="Audio duration in seconds")
    size_bytes: int = Field(description="File size in bytes")
    codec: str = Field(description="Audio codec")
    sample_rate: int = Field(description="Sample rate in Hz")
    channels: int = Field(description="Number of audio channels")
    bitrate: int = Field(description="Bitrate in bps")


class ProcessingLog(BaseModel):
    """Processing step log entry."""
    
    id: str = Field(description="Log entry ID")
    video_id: str = Field(description="Video identifier")
    step: str = Field(description="Processing step name")
    status: ProcessingStatus = Field(description="Step status")
    message: Optional[str] = Field(default=None, description="Log message")
    error_details: Optional[Dict[str, Any]] = Field(default=None, description="Error details")
    execution_time_ms: Optional[int] = Field(default=None, description="Execution time in milliseconds")
    created_at: str = Field(description="Timestamp when log was created")


class VideoRecord(BaseModel):
    """Video database record."""
    
    id: str = Field(description="Video identifier")
    original_filename: str = Field(description="Original filename")
    gcs_input_path: str = Field(description="GCS input path")
    gcs_audio_path: Optional[str] = Field(default=None, description="GCS audio output path")
    status: ProcessingStatus = Field(description="Processing status")
    source_language: Optional[str] = Field(default=None, description="Source language")
    target_language: Optional[str] = Field(default=None, description="Target language")
    duration_seconds: Optional[float] = Field(default=None, description="Video duration")
    file_size_bytes: Optional[int] = Field(default=None, description="File size")
    created_at: str = Field(description="Creation timestamp")
    updated_at: str = Field(description="Last update timestamp")


class WorkflowStatus(BaseModel):
    """Workflow execution status."""
    
    workflow_id: str = Field(description="Workflow identifier")
    video_id: str = Field(description="Video being processed")
    status: ProcessingStatus = Field(description="Current status")
    current_step: str = Field(description="Current processing step")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    started_at: str = Field(description="Workflow start time")
    updated_at: str = Field(description="Last status update")


class SegmentMetadata(BaseModel):
    """Audio segment metadata for future pipeline stages."""
    
    segment_id: str = Field(description="Segment identifier")
    video_id: str = Field(description="Parent video identifier")
    start_time: float = Field(description="Segment start time in seconds")
    end_time: float = Field(description="Segment end time in seconds")
    duration: float = Field(description="Segment duration")
    speaker_id: Optional[str] = Field(default=None, description="Speaker identifier")
    confidence_score: Optional[float] = Field(default=None, description="Segmentation confidence")


class SpeechSegmentationRequest(BaseModel):
    """Request for speech segmentation (VAD) processing."""
    
    video_id: str = Field(description="Video identifier")
    gcs_audio_path: str = Field(description="GCS path to extracted audio file")
    
    # VAD Configuration
    vad_threshold: float = Field(default=0.5, description="VAD speech detection threshold")
    min_speech_duration_ms: int = Field(default=1500, description="Minimum speech segment duration")
    max_segment_duration_s: int = Field(default=90, description="Maximum segment duration")
    min_silence_gap_ms: int = Field(default=1000, description="Minimum silence gap between segments")
    speech_padding_ms: int = Field(default=50, description="Padding around speech segments")


class SpeechSegmentationResult(BaseModel):
    """Result from speech segmentation processing."""
    
    video_id: str = Field(description="Video identifier")
    success: bool = Field(description="Whether segmentation succeeded")
    total_segments: int = Field(description="Number of segments created")
    total_speech_duration: float = Field(description="Total speech duration in seconds")
    processing_time_seconds: float = Field(description="Processing time")
    
    # Segment data
    segments: List[Dict[str, Any]] = Field(default_factory=list, description="Segment metadata list")
    gcs_manifest_path: str = Field(default="", description="GCS path to segment manifest JSON")
    gcs_segments_folder: str = Field(default="", description="GCS folder containing segment audio files")
    
    # Quality metrics
    speech_to_silence_ratio: float = Field(description="Ratio of speech to total duration")
    avg_segment_duration: float = Field(description="Average segment duration")
    confidence_scores: List[float] = Field(default_factory=list, description="VAD confidence scores")
    
    error_message: str = Field(default="", description="Error message if failed")


class AudioSegment(BaseModel):
    """Individual audio segment data."""
    
    id: str = Field(description="Segment UUID")
    segment_index: int = Field(description="Sequential segment index")
    start_time: float = Field(description="Start time in seconds")
    end_time: float = Field(description="End time in seconds") 
    duration: float = Field(description="Segment duration in seconds")
    confidence_score: float = Field(description="VAD confidence score")
    gcs_audio_path: str = Field(description="GCS path to segment audio file")
    status: ProcessingStatus = Field(default=ProcessingStatus.PENDING, description="Processing status")


class TranscriptionRequest(BaseModel):
    """Request for transcribing audio segments."""
    
    video_id: str = Field(description="Video identifier")
    segments: List[Dict[str, Any]] = Field(description="Audio segments to transcribe")
    
    # OpenAI Whisper API Configuration
    model: str = Field(default="whisper-1", description="OpenAI Whisper model")
    response_format: str = Field(default="json", description="Response format (text, json, srt, verbose_json, vtt)")
    language: Optional[str] = Field(default=None, description="Language code or auto-detect")
    prompt: Optional[str] = Field(default=None, description="Optional context prompt")
    temperature: float = Field(default=0, description="Sampling temperature (0-1)")
    
    # Queue Configuration (to avoid loading config inside workflow)
    transcription_queue: str = Field(description="Queue name for transcription tasks")


class TranscriptionResult(BaseModel):
    """Result from transcribing a single audio segment."""
    
    video_id: str = Field(description="Video identifier")
    segment_id: str = Field(description="Segment identifier")
    segment_index: int = Field(description="Sequential segment index")
    text: str = Field(description="Transcribed text")
    language: Optional[str] = Field(default=None, description="Detected language")
    processing_time_seconds: float = Field(description="Processing time")
    api_request_id: Optional[str] = Field(default=None, description="OpenAI API request ID")
    success: bool = Field(description="Whether transcription succeeded")
    error_message: str = Field(default="", description="Error message if failed")


class ConsolidatedTranscriptionResult(BaseModel):
    """Result from transcribing all segments of a video."""
    
    video_id: str = Field(description="Video identifier")
    success: bool = Field(description="Whether all transcriptions succeeded")
    total_segments: int = Field(description="Total number of segments processed")
    successful_transcriptions: int = Field(description="Number of successful transcriptions")
    primary_language: str = Field(description="Most common detected language")
    
    # Transcription data
    transcriptions: List[TranscriptionResult] = Field(default_factory=list, description="Individual transcription results")
    full_transcript: str = Field(description="Complete concatenated transcript")
    gcs_transcript_path: str = Field(default="", description="GCS path to transcript JSON file")
    
    # Quality metrics
    total_words: int = Field(description="Total word count")
    language_consistency_score: float = Field(description="Language consistency across segments (0-1)")
    processing_time_seconds: float = Field(description="Total processing time")
    api_requests_count: int = Field(description="Number of API requests made")
    success_rate: float = Field(description="Percentage of successful transcriptions")
    
    error_message: str = Field(default="", description="Error message if failed")


class TranslationRequest(BaseModel):
    """Request for translating transcribed segments with cultural refinement."""
    
    video_id: str = Field(description="Video identifier")
    target_language: str = Field(description="Target language code (e.g., 'es', 'fr', 'de')")
    source_language: Optional[str] = Field(default=None, description="Source language code (auto-detect if None)")
    cultural_context: str = Field(default="general", description="Cultural context for refinement (general, educational, business, entertainment)")
    enable_refinement: bool = Field(default=True, description="Enable OpenAI cultural refinement")
    
    # Queue Configuration (to avoid loading config inside workflow)
    translation_queue: str = Field(description="Queue name for translation tasks")


class SegmentTranslationResult(BaseModel):
    """Result from translating a single segment with both Google and OpenAI versions."""
    
    video_id: str = Field(description="Video identifier")
    segment_id: str = Field(description="Segment identifier (UUID)")
    segment_index: int = Field(description="Sequential segment index")
    
    # Source data
    source_text: str = Field(description="Original transcribed text")
    source_language: str = Field(description="Source language code")
    target_language: str = Field(description="Target language code")
    
    # Google Translate Results (for audit/logging)
    google_translation: str = Field(description="Google Translate v3 result")
    google_confidence_score: Optional[float] = Field(default=None, description="Google translation confidence (0-1)")
    google_detected_language: Optional[str] = Field(default=None, description="Google detected source language")
    google_processing_time_seconds: float = Field(description="Google API processing time")
    google_api_request_id: Optional[str] = Field(default=None, description="Google API request identifier")
    
    # OpenAI Refinement Results (FINAL OUTPUT)
    openai_refined_translation: str = Field(description="OpenAI culturally refined translation")
    openai_processing_time_seconds: float = Field(description="OpenAI processing time")
    openai_model_used: str = Field(default="gpt-4", description="OpenAI model used for refinement")
    openai_api_request_id: Optional[str] = Field(default=None, description="OpenAI API request identifier")
    cultural_context: str = Field(description="Cultural context used for refinement")
    
    # Quality and Processing Metrics
    translation_quality_score: Optional[float] = Field(default=None, description="Overall translation quality score (0-1)")
    processing_time_total_seconds: float = Field(description="Total processing time for segment")
    
    success: bool = Field(description="Whether translation succeeded")
    error_message: str = Field(default="", description="Error message if failed")


class ConsolidatedTranslationResult(BaseModel):
    """Result from translating all segments of a video with cultural refinement."""
    
    video_id: str = Field(description="Video identifier")
    source_language: str = Field(description="Source language code")
    target_language: str = Field(description="Target language code")
    cultural_context: str = Field(description="Cultural context used")
    
    # Processing summary
    total_segments: int = Field(description="Total number of segments processed")
    successful_translations: int = Field(description="Number of successful translations")
    success_rate: float = Field(description="Percentage of successful translations")
    
    # Translation results
    translations: List[SegmentTranslationResult] = Field(default_factory=list, description="Individual segment translation results")
    
    # Quality metrics
    avg_google_confidence: float = Field(description="Average Google Translate confidence score")
    avg_translation_quality: float = Field(description="Average overall translation quality score")
    
    # Processing time breakdown
    total_processing_time_seconds: float = Field(description="Total processing time for all segments")
    google_processing_time_seconds: float = Field(description="Total Google API processing time")
    openai_processing_time_seconds: float = Field(description="Total OpenAI processing time")
    
    # Output storage
    gcs_translation_path: str = Field(default="", description="GCS path to consolidated translation JSON")
    
    success: bool = Field(description="Whether overall translation succeeded")
    error_message: str = Field(default="", description="Error message if failed")


class VoiceSynthesisRequest(BaseModel):
    """Request for synthesizing voice from translated text using Replicate Speech-02-HD."""
    
    video_id: str = Field(description="Video identifier")
    target_language: str = Field(description="Target language code")
    voice_id: str = Field(default="Friendly_Person", description="Voice ID for synthesis")
    emotion: str = Field(default="neutral", description="Emotion for voice synthesis (neutral, happy, sad, excited, calm)")
    language_boost: str = Field(default="Automatic", description="Language boost setting")
    english_normalization: bool = Field(default=True, description="Enable English text normalization")
    
    # Queue Configuration (to avoid loading config inside workflow)
    voice_synthesis_queue: str = Field(description="Queue name for voice synthesis tasks")


class SegmentVoiceSynthesisResult(BaseModel):
    """Result from synthesizing voice for a single segment."""
    
    video_id: str = Field(description="Video identifier")
    translation_id: str = Field(description="Translation identifier (UUID)")
    segment_id: str = Field(description="Segment identifier (UUID)")
    segment_index: int = Field(description="Sequential segment index")
    
    # Input data
    source_text: str = Field(description="OpenAI refined translation text")
    target_language: str = Field(description="Target language code")
    
    # TTS Configuration
    voice_id: str = Field(description="Voice ID used for synthesis")
    emotion: str = Field(description="Emotion used for synthesis")
    language_boost: str = Field(description="Language boost setting used")
    english_normalization: bool = Field(description="Text normalization setting")
    
    # Replicate Results
    replicate_audio_url: str = Field(description="Replicate delivery URL for generated audio")
    gcs_audio_path: str = Field(default="", description="GCS path where audio is stored")
    audio_duration_seconds: Optional[float] = Field(default=None, description="Audio duration in seconds")
    processing_time_seconds: float = Field(description="Processing time for TTS generation")
    replicate_request_id: Optional[str] = Field(default=None, description="Replicate API request identifier")
    
    # Quality Metrics
    audio_quality_score: Optional[float] = Field(default=None, description="Audio quality score (0-1)")
    voice_similarity_score: Optional[float] = Field(default=None, description="Voice consistency score (0-1)")
    
    success: bool = Field(description="Whether voice synthesis succeeded")
    error_message: str = Field(default="", description="Error message if failed")


class ConsolidatedVoiceSynthesisResult(BaseModel):
    """Result from synthesizing voice for all segments of a video."""
    
    video_id: str = Field(description="Video identifier")
    target_language: str = Field(description="Target language code")
    voice_id: str = Field(description="Voice ID used for synthesis")
    emotion: str = Field(description="Primary emotion used")
    
    # Processing summary
    total_segments: int = Field(description="Total number of segments processed")
    successful_synthesis: int = Field(description="Number of successful voice syntheses")
    success_rate: float = Field(description="Percentage of successful syntheses")
    
    # Audio Results
    voice_segments: List[SegmentVoiceSynthesisResult] = Field(default_factory=list, description="Individual segment synthesis results")
    
    # Quality Metrics
    total_audio_duration: float = Field(description="Total duration of all audio segments")
    avg_audio_quality: float = Field(description="Average audio quality score")
    voice_consistency_score: float = Field(description="Voice consistency across segments")
    
    # Processing Time
    total_processing_time_seconds: float = Field(description="Total processing time for all segments")
    avg_processing_time_per_segment: float = Field(description="Average processing time per segment")
    
    # Storage Information
    gcs_audio_folder: str = Field(default="", description="GCS folder containing audio files")
    audio_manifest_path: str = Field(default="", description="GCS path to audio manifest JSON")
    
    # Cost Estimation
    estimated_cost: float = Field(description="Estimated cost for voice synthesis")
    character_count: int = Field(description="Total characters processed")
    
    success: bool = Field(description="Whether overall voice synthesis succeeded")
    error_message: str = Field(default="", description="Error message if failed")


class DubbingPipelineRequest(BaseModel):
    """Complete dubbing pipeline request."""
    
    video_id: str = Field(description="Video identifier")
    gcs_input_path: str = Field(description="Input video GCS path")
    source_language: str = Field(description="Source language code")
    target_language: str = Field(description="Target language code")
    voice_cloning_enabled: bool = Field(default=True, description="Enable voice cloning")
    quality_level: str = Field(default="standard", description="Processing quality level")
    
    # Optional parameters
    original_filename: Optional[str] = Field(default=None)
    priority: int = Field(default=0, description="Processing priority")
    callback_url: Optional[str] = Field(default=None, description="Completion callback URL")


class DubbingPipelineResult(BaseModel):
    """Complete dubbing pipeline result."""
    
    video_id: str = Field(description="Video identifier")
    success: bool = Field(description="Whether pipeline succeeded")
    final_video_url: str = Field(description="GCS URL of final dubbed video")
    processing_time_seconds: float = Field(description="Total processing time")
    
    # Stage results
    audio_extraction: Optional[AudioExtractionResult] = Field(default=None)
    # Future: Add other pipeline stage results
    
    error_message: str = Field(default="", description="Error message if failed")


class ApiResponse(BaseModel):
    """Standard API response format."""
    
    success: bool = Field(description="Whether request succeeded")
    message: str = Field(description="Response message")
    data: Optional[Dict[str, Any]] = Field(default=None, description="Response data")
    error_code: Optional[str] = Field(default=None, description="Error code if failed")
    timestamp: str = Field(description="Response timestamp")


class HealthCheckResponse(BaseModel):
    """Health check response."""
    
    status: str = Field(description="Service status")
    version: str = Field(description="Service version")
    dependencies: Dict[str, str] = Field(description="Dependency status")
    timestamp: str = Field(description="Check timestamp")