"""Async database client for AI dubbing pipeline."""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

import asyncpg
from asyncpg import Connection, Pool

from .models import VideoRecord, ProcessingLog, ProcessingStatus

logger = logging.getLogger(__name__)


class AsyncDatabaseClient:
    """Async Neon PostgreSQL client for dubbing pipeline."""
    
    def __init__(self, database_url: str):
        """Initialize database client with connection URL."""
        self.database_url = database_url
        self.pool: Optional[Pool] = None
    
    async def connect(self) -> None:
        """Establish connection pool to database."""
        try:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=10,
                command_timeout=60,
                server_settings={'jit': 'off'}  # Disable JIT for better performance
            )
            
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            logger.info("Successfully connected to database")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection closed")
    
    async def create_schema(self) -> None:
        """Create database schema for video processing."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        schema_sql = """
        -- Enable UUID extension
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        
        -- Videos table
        CREATE TABLE IF NOT EXISTS videos (
            id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
            original_filename TEXT NOT NULL,
            gcs_input_path TEXT NOT NULL,
            gcs_audio_path TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            source_language TEXT,
            target_language TEXT,
            duration_seconds FLOAT,
            file_size_bytes BIGINT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Processing logs table
        CREATE TABLE IF NOT EXISTS processing_logs (
            id UUID DEFAULT uuid_generate_v4(),
            video_id UUID NOT NULL,
            step TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            error_details JSONB,
            execution_time_ms INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Segments table for future pipeline stages
        CREATE TABLE IF NOT EXISTS segments (
            id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
            video_id UUID NOT NULL,
            segment_index INTEGER NOT NULL,
            start_time FLOAT NOT NULL,
            end_time FLOAT NOT NULL,
            duration FLOAT NOT NULL,
            speaker_id TEXT,
            confidence_score FLOAT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Transcriptions table for AI dubbing pipeline
        CREATE TABLE IF NOT EXISTS transcriptions (
            id UUID DEFAULT uuid_generate_v4(),
            video_id UUID NOT NULL,
            segment_id UUID NOT NULL,
            segment_index INTEGER NOT NULL,
            text TEXT NOT NULL,
            language VARCHAR(10),
            processing_time_seconds FLOAT,
            api_request_id VARCHAR(100),
            status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Translations table for AI dubbing pipeline with dual storage
        CREATE TABLE IF NOT EXISTS translations (
            id UUID DEFAULT uuid_generate_v4(),
            video_id UUID NOT NULL,
            segment_id UUID NOT NULL,
            segment_index INTEGER NOT NULL,
            
            -- Source data from transcriptions
            source_text TEXT NOT NULL,
            source_language VARCHAR(10),
            target_language VARCHAR(10),
            
            -- Google Translate v3 Results (for audit/logging)
            google_translation TEXT NOT NULL,
            google_confidence_score FLOAT,
            google_detected_language VARCHAR(10),
            google_processing_time_seconds FLOAT,
            google_api_request_id VARCHAR(100),
            
            -- OpenAI Refinement Results (FINAL OUTPUT)
            openai_refined_translation TEXT NOT NULL,
            openai_processing_time_seconds FLOAT,
            openai_model_used VARCHAR(50) DEFAULT 'gpt-4',
            openai_api_request_id VARCHAR(100),
            cultural_context TEXT,
            
            -- Quality and Processing Metrics
            translation_quality_score FLOAT,
            processing_time_total_seconds FLOAT,
            
            status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Voice synthesis table for TTS results
        CREATE TABLE IF NOT EXISTS voice_synthesis (
            id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
            video_id UUID NOT NULL,
            translation_id UUID NOT NULL,  -- Links to translations.id
            segment_id UUID NOT NULL,
            segment_index INTEGER NOT NULL,
            
            -- Input data from translations
            source_text TEXT NOT NULL,
            target_language VARCHAR(10),
            
            -- Replicate TTS Configuration
            voice_id VARCHAR(50) DEFAULT 'Friendly_Person',
            emotion VARCHAR(20) DEFAULT 'neutral',
            language_boost VARCHAR(20) DEFAULT 'Automatic',
            english_normalization BOOLEAN DEFAULT true,
            
            -- TTS Results
            replicate_audio_url TEXT NOT NULL,
            gcs_audio_path TEXT,
            audio_duration_seconds FLOAT,
            processing_time_seconds FLOAT,
            replicate_request_id VARCHAR(100),
            
            -- Audio Quality Metrics
            audio_quality_score FLOAT,
            voice_similarity_score FLOAT,
            
            status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        -- Workflow executions table
        CREATE TABLE IF NOT EXISTS workflow_executions (
            id UUID DEFAULT uuid_generate_v4(),
            workflow_id TEXT NOT NULL,
            video_id UUID NOT NULL,
            workflow_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            current_step TEXT,
            error_message TEXT,
            started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            completed_at TIMESTAMP WITH TIME ZONE
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
        CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at);
        CREATE INDEX IF NOT EXISTS idx_processing_logs_video_id ON processing_logs(video_id);
        CREATE INDEX IF NOT EXISTS idx_processing_logs_step_status ON processing_logs(step, status);
        CREATE INDEX IF NOT EXISTS idx_segments_video_id ON segments(video_id);
        CREATE INDEX IF NOT EXISTS idx_transcriptions_video_id ON transcriptions(video_id);
        CREATE INDEX IF NOT EXISTS idx_transcriptions_segment_index ON transcriptions(video_id, segment_index);
        CREATE INDEX IF NOT EXISTS idx_transcriptions_status ON transcriptions(status);
        CREATE INDEX IF NOT EXISTS idx_translations_video_id ON translations(video_id);
        CREATE INDEX IF NOT EXISTS idx_translations_segment_index ON translations(video_id, segment_index);
        CREATE INDEX IF NOT EXISTS idx_translations_status ON translations(status);
        CREATE INDEX IF NOT EXISTS idx_translations_target_language ON translations(target_language);
        CREATE INDEX IF NOT EXISTS idx_voice_synthesis_video_id ON voice_synthesis(video_id);
        CREATE INDEX IF NOT EXISTS idx_voice_synthesis_segment_index ON voice_synthesis(video_id, segment_index);
        CREATE INDEX IF NOT EXISTS idx_voice_synthesis_status ON voice_synthesis(status);
        CREATE INDEX IF NOT EXISTS idx_voice_synthesis_translation_id ON voice_synthesis(translation_id);
        CREATE INDEX IF NOT EXISTS idx_workflow_executions_video_id ON workflow_executions(video_id);
        CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions(status);
        
        -- Audio alignments table for Stage 7: Alignment & Pacing
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

        -- Final dubbed videos table for Stage 8: Audio Stitching & Video Muxing
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
        
        -- Additional indexes for alignment tables
        CREATE INDEX IF NOT EXISTS idx_audio_alignments_video_id ON audio_alignments(video_id);
        CREATE INDEX IF NOT EXISTS idx_audio_alignments_segment_id ON audio_alignments(segment_id);
        CREATE INDEX IF NOT EXISTS idx_audio_alignments_status ON audio_alignments(status);
        CREATE INDEX IF NOT EXISTS idx_dubbed_videos_video_id ON dubbed_videos(video_id);
        CREATE INDEX IF NOT EXISTS idx_dubbed_videos_status ON dubbed_videos(status);
        
        -- Update trigger for videos table
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        DROP TRIGGER IF EXISTS update_videos_updated_at ON videos;
        CREATE TRIGGER update_videos_updated_at
            BEFORE UPDATE ON videos
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
            
        DROP TRIGGER IF EXISTS update_segments_updated_at ON segments;
        CREATE TRIGGER update_segments_updated_at
            BEFORE UPDATE ON segments
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
            
        DROP TRIGGER IF EXISTS update_transcriptions_updated_at ON transcriptions;
        CREATE TRIGGER update_transcriptions_updated_at
            BEFORE UPDATE ON transcriptions
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
            
        DROP TRIGGER IF EXISTS update_translations_updated_at ON translations;
        CREATE TRIGGER update_translations_updated_at
            BEFORE UPDATE ON translations
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(schema_sql)
            logger.info("Database schema created successfully")
        except Exception as e:
            # Check if it's a deadlock or relation already exists error (which is OK)
            error_msg = str(e).lower()
            if "deadlock detected" in error_msg or "already exists" in error_msg:
                logger.info(f"Schema creation skipped (already exists or concurrent creation): {e}")
            else:
                logger.error(f"Failed to create schema: {e}")
                raise
    
    async def create_video_record(self, 
                                 video_id: str,
                                 original_filename: str, 
                                 gcs_input_path: str,
                                 file_size_bytes: Optional[int] = None,
                                 source_language: Optional[str] = None,
                                 target_language: Optional[str] = None) -> str:
        """Create a new video record."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO videos (id, original_filename, gcs_input_path, file_size_bytes, 
                                       source_language, target_language, status)
                    VALUES ($1, $2, $3, $4, $5, $6, 'pending')
                    """,
                    video_id, original_filename, gcs_input_path, file_size_bytes,
                    source_language, target_language
                )
            
            logger.info(f"Created/updated video record: {video_id}")
            return video_id
            
        except Exception as e:
            logger.error(f"Failed to create video record: {e}")
            raise
    
    async def update_video_status(self, 
                                 video_id: str, 
                                 status: str, 
                                 gcs_audio_path: Optional[str] = None,
                                 duration_seconds: Optional[float] = None) -> None:
        """Update video processing status."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                if gcs_audio_path:
                    await conn.execute(
                        """
                        UPDATE videos 
                        SET status = $1, gcs_audio_path = $2, duration_seconds = $3
                        WHERE id = $4
                        """,
                        status, gcs_audio_path, duration_seconds, video_id
                    )
                else:
                    await conn.execute(
                        "UPDATE videos SET status = $1 WHERE id = $2",
                        status, video_id
                    )
            
            logger.info(f"Updated video {video_id} status to {status}")
            
        except Exception as e:
            logger.error(f"Failed to update video status: {e}")
            raise
    
    async def log_processing_step(self, 
                                 video_id: str, 
                                 step: str, 
                                 status: str, 
                                 message: Optional[str] = None,
                                 error_details: Optional[Dict[str, Any]] = None,
                                 execution_time_ms: Optional[int] = None) -> None:
        """Log a processing step."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            # Convert error_details dict to JSON string for PostgreSQL JSONB
            error_details_json = json.dumps(error_details) if error_details else None
            
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO processing_logs (video_id, step, status, message, error_details, execution_time_ms)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    video_id, step, status, message, error_details_json, execution_time_ms
                )
            
            logger.debug(f"Logged {step} {status} for video {video_id}")
            
        except Exception as e:
            logger.error(f"Failed to log processing step: {e}")
            raise
    
    async def get_video_info(self, video_id: str) -> Optional[Dict[str, Any]]:
        """Get video information by ID."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM videos WHERE id = $1",
                    video_id
                )
                
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            logger.error(f"Failed to get video info: {e}")
            raise
    
    async def get_processing_logs(self, video_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get processing logs for a video."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT * FROM processing_logs 
                    WHERE video_id = $1 
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    video_id, limit
                )
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get processing logs: {e}")
            raise
    
    async def get_videos_by_status(self, status: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get videos by status."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT * FROM videos 
                    WHERE status = $1 
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    status, limit
                )
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get videos by status: {e}")
            raise
    
    async def record_workflow_execution(self, 
                                      workflow_id: str,
                                      video_id: str,
                                      workflow_type: str) -> None:
        """Record workflow execution start."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO workflow_executions (workflow_id, video_id, workflow_type, status)
                    VALUES ($1, $2, $3, 'running')
                    ON CONFLICT (workflow_id) DO UPDATE SET
                        status = 'running',
                        started_at = NOW()
                    """,
                    workflow_id, video_id, workflow_type
                )
            
            logger.info(f"Recorded workflow execution: {workflow_id}")
            
        except Exception as e:
            logger.error(f"Failed to record workflow execution: {e}")
            raise
    
    async def create_segments(self, 
                            video_id: str, 
                            segments_data: List[Dict[str, Any]]) -> List[str]:
        """Create multiple segments for a video."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            segment_ids = []
            async with self.pool.acquire() as conn:
                for segment in segments_data:
                    segment_id = await conn.fetchval(
                        """
                        INSERT INTO segments (video_id, segment_index, start_time, end_time, 
                                            duration, confidence_score, status)
                        VALUES ($1, $2, $3, $4, $5, $6, 'pending')
                        RETURNING id
                        """,
                        video_id,
                        segment['segment_index'],
                        segment['start_time'],
                        segment['end_time'], 
                        segment['duration'],
                        segment.get('confidence_score', 0.0)
                    )
                    segment_ids.append(str(segment_id))
            
            logger.info(f"Created {len(segment_ids)} segments for video {video_id}")
            return segment_ids
            
        except Exception as e:
            logger.error(f"Failed to create segments: {e}")
            raise
    
    async def update_segment_status(self, 
                                  segment_id: str, 
                                  status: str,
                                  error_message: Optional[str] = None) -> None:
        """Update segment processing status."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE segments SET status = $1 WHERE id = $2",
                    status, segment_id
                )
            
            logger.info(f"Updated segment {segment_id} status to {status}")
            
        except Exception as e:
            logger.error(f"Failed to update segment status: {e}")
            raise
    
    async def get_segments_for_video(self, video_id: str) -> List[Dict[str, Any]]:
        """Get all segments for a video."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, segment_index, start_time, end_time, duration, 
                           confidence_score, status, created_at
                    FROM segments 
                    WHERE video_id = $1 
                    ORDER BY segment_index
                    """,
                    video_id
                )
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get segments: {e}")
            raise
    
    async def update_workflow_status(self, 
                                   workflow_id: str,
                                   status: str,
                                   current_step: Optional[str] = None,
                                   error_message: Optional[str] = None) -> None:
        """Update workflow execution status."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        try:
            async with self.pool.acquire() as conn:
                completed_at = "NOW()" if status in ['completed', 'failed', 'cancelled'] else None
                
                if completed_at:
                    await conn.execute(
                        """
                        UPDATE workflow_executions 
                        SET status = $1, current_step = $2, error_message = $3, completed_at = NOW()
                        WHERE workflow_id = $4
                        """,
                        status, current_step, error_message, workflow_id
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE workflow_executions 
                        SET status = $1, current_step = $2, error_message = $3
                        WHERE workflow_id = $4
                        """,
                        status, current_step, error_message, workflow_id
                    )
            
            logger.info(f"Updated workflow {workflow_id} status to {status}")
            
        except Exception as e:
            logger.error(f"Failed to update workflow status: {e}")
            raise


@asynccontextmanager
async def get_database_client(config):
    """Async context manager for database client."""
    client = AsyncDatabaseClient(config.neon_database_url)
    try:
        await client.connect()
        # Skip schema creation during activity execution to avoid deadlocks
        # Schema is created once during pipeline initialization
        yield client
    finally:
        await client.disconnect()