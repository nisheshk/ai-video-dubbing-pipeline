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