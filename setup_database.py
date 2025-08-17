#!/usr/bin/env python3
"""Setup database schema for AI dubbing pipeline."""

import asyncio
import sys
from config import DubbingConfig
from shared.database import AsyncDatabaseClient

async def setup_database():
    """Setup database schema."""
    try:
        # Load configuration
        config = DubbingConfig.from_env()
        print(f"Connecting to database: {config.neon_host}:{config.neon_database}")
        
        # Connect to database using the context manager
        from shared.database import get_database_client
        async with get_database_client(config) as db:
            print("✅ Successfully connected to PostgreSQL!")
            print("✅ Database schema created successfully!")
            
            # Test basic operations
            print("Testing database operations...")
            
            # Create a test video record
            import uuid
            video_id = str(uuid.uuid4())
            await db.create_video_record(
                video_id=video_id,
                gcs_input_path="test/test_setup.mp4",
                original_filename="test_setup.mp4",
                source_language="en",
                target_language="es"
            )
            print("✅ Created test video record")
            
            # Update status
            await db.update_video_status(video_id, "processing")
            print("✅ Updated video status")
            
            # Log processing step
            await db.log_processing_step(
                video_id=video_id,
                step="setup_test",
                status="completed",
                message="Database setup test",
                execution_time_ms=1000
            )
            print("✅ Logged processing step")
            
            # Get video info
            video_info = await db.get_video_info(video_id)
            print(f"✅ Retrieved video info: {video_info}")
            
            print("\n🎉 Database setup completed successfully!")
            print(f"Database URL: {config.neon_database_url}")
            
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(setup_database())