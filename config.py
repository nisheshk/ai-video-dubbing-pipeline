"""Configuration management for AI dubbing pipeline."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass
class DubbingConfig:
    """Configuration for AI dubbing pipeline."""
    
    # Google Cloud Storage
    google_cloud_project: str
    google_application_credentials: Optional[str]  # File path or JSON content
    gcs_bucket_name: str
    
    # Neon PostgreSQL Database
    neon_database_url: str
    neon_host: str
    neon_database: str
    neon_username: str
    neon_password: str
    
    # Temporal Configuration
    temporal_cloud_namespace: str
    temporal_cloud_address: str
    temporal_cloud_api_key: str
    task_queue: str
    
    # Dedicated Task Queues
    audio_extraction_queue: str
    speech_segmentation_queue: str
    transcription_queue: str
    translation_queue: str
    voice_synthesis_queue: str
    consolidation_queue: str
    
    # Processing Configuration
    temp_storage_path: str
    max_file_size_mb: int
    audio_sample_rate: int
    audio_channels: int
    
    # Speech Segmentation Configuration
    speech_vad_threshold: float
    speech_min_duration_ms: int
    speech_max_segment_duration_s: int
    speech_min_silence_gap_ms: int
    speech_padding_ms: int
    
    # FFmpeg Configuration
    ffmpeg_path: str
    ffprobe_path: str
    
    # OpenAI API Configuration
    openai_api_key: str
    openai_rate_limit_rpm: int
    max_concurrent_transcriptions: int
    
    # Replicate API Configuration
    replicate_api_token: str
    
    # Logging Configuration
    log_level: str
    log_file: str
    
    # Error Handling Configuration
    max_retries: int
    retry_delay_seconds: int
    timeout_seconds: int
    
    # Worker Scaling Configuration
    max_concurrent_activities: int
    max_concurrent_workflow_tasks: int
    max_concurrent_activity_tasks: int
    
    # Environment
    environment: str
    debug: bool
    
    @classmethod
    def from_env(cls, env_file: str = ".env.local") -> "DubbingConfig":
        """Load configuration from environment variables."""
        # Load environment file if it exists
        if os.path.exists(env_file):
            load_dotenv(env_file)
        
        # Determine environment
        environment = os.getenv("ENVIRONMENT", "local")
        debug = os.getenv("DEBUG", "false").lower() == "true"
        
        return cls(
            # Google Cloud Storage
            google_cloud_project=os.getenv("GOOGLE_CLOUD_PROJECT", ""),
            google_application_credentials=(
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or  # File path (standard)
                os.getenv("GOOGLE_CLOUD_CREDENTIALS")           # JSON content (new option)
            ),
            gcs_bucket_name=os.getenv("GCS_BUCKET_NAME", "dubbing-pipeline"),
            
            # Neon PostgreSQL Database
            neon_database_url=os.getenv("NEON_DATABASE_URL", ""),
            neon_host=os.getenv("NEON_HOST", ""),
            neon_database=os.getenv("NEON_DATABASE", ""),
            neon_username=os.getenv("NEON_USERNAME", ""),
            neon_password=os.getenv("NEON_PASSWORD", ""),
            
            # Temporal Configuration
            temporal_cloud_namespace=os.getenv("TEMPORAL_CLOUD_NAMESPACE", ""),
            temporal_cloud_address=os.getenv("TEMPORAL_CLOUD_ADDRESS", ""),
            temporal_cloud_api_key=os.getenv("TEMPORAL_CLOUD_API_KEY", ""),
            task_queue=os.getenv("TASK_QUEUE", "dubbing-prod-queue"),
            
            # Dedicated Task Queues
            audio_extraction_queue=os.getenv("AUDIO_EXTRACTION_QUEUE", "dubbing-audio-queue"),
            speech_segmentation_queue=os.getenv("SPEECH_SEGMENTATION_QUEUE", "dubbing-segmentation-queue"),
            transcription_queue=os.getenv("TRANSCRIPTION_QUEUE", "dubbing-transcription-queue"),
            translation_queue=os.getenv("TRANSLATION_QUEUE", "dubbing-translation-queue"),
            voice_synthesis_queue=os.getenv("VOICE_SYNTHESIS_QUEUE", "dubbing-voice-synthesis-queue"),
            consolidation_queue=os.getenv("CONSOLIDATION_QUEUE", "dubbing-consolidation-queue"),
            
            # Processing Configuration
            temp_storage_path=os.getenv("TEMP_STORAGE_PATH", "/tmp/dubbing_pipeline"),
            max_file_size_mb=int(os.getenv("MAX_FILE_SIZE_MB", "1000")),
            audio_sample_rate=int(os.getenv("AUDIO_SAMPLE_RATE", "16000")),
            audio_channels=int(os.getenv("AUDIO_CHANNELS", "1")),
            
            # Speech Segmentation Configuration
            speech_vad_threshold=float(os.getenv("SPEECH_VAD_THRESHOLD", "0.5")),
            speech_min_duration_ms=int(os.getenv("SPEECH_MIN_DURATION_MS", "1500")),
            speech_max_segment_duration_s=int(os.getenv("SPEECH_MAX_SEGMENT_DURATION_S", "90")),
            speech_min_silence_gap_ms=int(os.getenv("SPEECH_MIN_SILENCE_GAP_MS", "1000")),
            speech_padding_ms=int(os.getenv("SPEECH_PADDING_MS", "50")),
            
            # FFmpeg Configuration
            ffmpeg_path=os.getenv("FFMPEG_PATH", "/usr/local/bin/ffmpeg"),
            ffprobe_path=os.getenv("FFPROBE_PATH", "/usr/local/bin/ffprobe"),
            
            # OpenAI API Configuration
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            openai_rate_limit_rpm=int(os.getenv("OPENAI_RATE_LIMIT_RPM", "50")),
            max_concurrent_transcriptions=int(os.getenv("MAX_CONCURRENT_TRANSCRIPTIONS", "6")),
            
            # Replicate API Configuration
            replicate_api_token=os.getenv("REPLICATE_API_TOKEN", ""),
            
            # Logging Configuration
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE", "logs/dubbing_pipeline.log"),
            
            # Error Handling Configuration
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_delay_seconds=int(os.getenv("RETRY_DELAY_SECONDS", "5")),
            timeout_seconds=int(os.getenv("TIMEOUT_SECONDS", "300")),
            
            # Worker Scaling Configuration
            max_concurrent_activities=int(os.getenv("MAX_CONCURRENT_ACTIVITIES", "25")),
            max_concurrent_workflow_tasks=int(os.getenv("MAX_CONCURRENT_WORKFLOW_TASKS", "5")),
            max_concurrent_activity_tasks=int(os.getenv("MAX_CONCURRENT_ACTIVITY_TASKS", "20")),
            
            # Environment
            environment=environment,
            debug=debug,
        )
    
    def ensure_temp_directory(self) -> None:
        """Create temp directory if it doesn't exist."""
        Path(self.temp_storage_path).mkdir(parents=True, exist_ok=True)
        
    def ensure_log_directory(self) -> None:
        """Create log directory if it doesn't exist."""
        log_path = Path(self.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    def validate_required_config(self) -> list[str]:
        """Validate required configuration and return missing fields."""
        missing_fields = []
        
        # Required for all environments
        required_fields = [
            ("google_cloud_project", self.google_cloud_project),
            ("gcs_bucket_name", self.gcs_bucket_name),
            ("neon_database_url", self.neon_database_url),
        ]
        
        # Required for cloud environment
        if self.environment == "cloud":
            required_fields.extend([
                ("temporal_cloud_namespace", self.temporal_cloud_namespace),
                ("temporal_cloud_address", self.temporal_cloud_address),
                ("temporal_cloud_api_key", self.temporal_cloud_api_key),
            ])
        
        for field_name, field_value in required_fields:
            if not field_value:
                missing_fields.append(field_name.upper())
        
        return missing_fields
    
    def get_database_config(self) -> dict:
        """Get database configuration dict."""
        return {
            "url": self.neon_database_url,
            "host": self.neon_host,
            "database": self.neon_database,
            "username": self.neon_username,
            "password": self.neon_password,
        }
    
    def get_gcs_config(self) -> dict:
        """Get GCS configuration dict."""
        return {
            "project": self.google_cloud_project,
            "bucket": self.gcs_bucket_name,
            "credentials": self.google_application_credentials,
        }
    
    def get_temporal_config(self) -> dict:
        """Get Temporal configuration dict."""
        if self.environment == "cloud":
            return {
                "namespace": self.temporal_cloud_namespace,
                "address": self.temporal_cloud_address,
                "api_key": self.temporal_cloud_api_key,
                "task_queue": self.task_queue,
            }
        else:
            return {
                "host": "localhost",
                "port": 7233,
                "task_queue": self.task_queue,
            }
    
    def get_worker_config(self) -> dict:
        """Get worker scaling configuration dict."""
        return {
            "max_concurrent_activities": self.max_concurrent_activities,
            "max_concurrent_workflow_tasks": self.max_concurrent_workflow_tasks,
            "max_concurrent_activity_tasks": self.max_concurrent_activity_tasks,
        }
    
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment in ["local", "development"] or self.debug
    
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment == "production" and not self.debug


# Global configuration instance
# This will be loaded when the module is imported
_config_cache: Optional[DubbingConfig] = None


def get_config(env_file: str = ".env.local") -> DubbingConfig:
    """Get cached configuration instance."""
    global _config_cache
    if _config_cache is None:
        _config_cache = DubbingConfig.from_env(env_file)
    return _config_cache


def reload_config(env_file: str = ".env.local") -> DubbingConfig:
    """Reload configuration from environment."""
    global _config_cache
    _config_cache = DubbingConfig.from_env(env_file)
    return _config_cache