"""Async database client for AI dubbing pipeline."""

import asyncio
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
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            original_filename TEXT NOT NULL,
            gcs_input_path TEXT NOT NULL,
            gcs_audio_path TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            source_language TEXT,
            target_language TEXT,
            duration_seconds FLOAT,
            file_size_bytes BIGINT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'cancelled'))
        );
        
        -- Processing logs table
        CREATE TABLE IF NOT EXISTS processing_logs (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
            step TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            error_details JSONB,
            execution_time_ms INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            CONSTRAINT valid_log_status CHECK (status IN ('started', 'completed', 'failed'))
        );
        
        -- Segments table for future pipeline stages
        CREATE TABLE IF NOT EXISTS segments (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
            segment_index INTEGER NOT NULL,
            start_time FLOAT NOT NULL,
            end_time FLOAT NOT NULL,
            duration FLOAT NOT NULL,
            speaker_id TEXT,
            confidence_score FLOAT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            CONSTRAINT valid_segment_status CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
            UNIQUE(video_id, segment_index)
        );
        
        -- Workflow executions table
        CREATE TABLE IF NOT EXISTS workflow_executions (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            workflow_id TEXT NOT NULL UNIQUE,
            video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
            workflow_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            current_step TEXT,
            error_message TEXT,
            started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            completed_at TIMESTAMP WITH TIME ZONE,
            CONSTRAINT valid_workflow_status CHECK (status IN ('running', 'completed', 'failed', 'cancelled'))
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
        CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at);
        CREATE INDEX IF NOT EXISTS idx_processing_logs_video_id ON processing_logs(video_id);
        CREATE INDEX IF NOT EXISTS idx_processing_logs_step_status ON processing_logs(step, status);
        CREATE INDEX IF NOT EXISTS idx_segments_video_id ON segments(video_id);
        CREATE INDEX IF NOT EXISTS idx_workflow_executions_video_id ON workflow_executions(video_id);
        CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions(status);
        
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
        """
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(schema_sql)
            logger.info("Database schema created successfully")
        except Exception as e:
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
                    ON CONFLICT (id) DO UPDATE SET
                        original_filename = EXCLUDED.original_filename,
                        gcs_input_path = EXCLUDED.gcs_input_path,
                        file_size_bytes = EXCLUDED.file_size_bytes,
                        source_language = EXCLUDED.source_language,
                        target_language = EXCLUDED.target_language,
                        updated_at = NOW()
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
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO processing_logs (video_id, step, status, message, error_details, execution_time_ms)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    video_id, step, status, message, error_details, execution_time_ms
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
        await client.create_schema()
        yield client
    finally:
        await client.disconnect()