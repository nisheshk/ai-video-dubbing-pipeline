#!/usr/bin/env python3
"""Script to test database logging functionality after speech segmentation."""

import asyncio
import os
from config import DubbingConfig
from shared.database import get_database_client

async def main():
    """Test database logging by querying processing logs."""
    
    # Load config
    config = DubbingConfig.from_env()
    
    # Test video ID from our segmentation test
    video_id = "d82c5c2a-3099-476d-937b-caf03bcc4043"
    
    print("🔍 Querying database for speech segmentation logs...")
    print("=" * 60)
    
    try:
        async with get_database_client(config) as db_client:
            
            # Get all processing logs for the video
            logs = await db_client.get_processing_logs(video_id, limit=50)
            
            if not logs:
                print("❌ No processing logs found for this video")
                return
            
            print(f"📊 Found {len(logs)} processing log entries:")
            print()
            
            # Group logs by step
            step_logs = {}
            for log in logs:
                step = log['step']
                if step not in step_logs:
                    step_logs[step] = []
                step_logs[step].append(log)
            
            # Display logs by step
            for step, step_log_entries in step_logs.items():
                print(f"🔧 {step.upper().replace('_', ' ')}")
                for log in sorted(step_log_entries, key=lambda x: x['created_at']):
                    status_emoji = {
                        'processing': '🔄',
                        'completed': '✅', 
                        'failed': '❌'
                    }.get(log['status'], '⚪')
                    
                    execution_time = ""
                    if log['execution_time_ms']:
                        execution_time = f" ({log['execution_time_ms']}ms)"
                    
                    print(f"   {status_emoji} {log['status'].upper()}: {log['message']}{execution_time}")
                    print(f"      ⏰ {log['created_at']}")
                    
                    if log['error_details']:
                        print(f"      🚨 Error: {log['error_details']}")
                print()
            
            # Get segment data
            print("🎞️ Checking segment data...")
            segments = await db_client.get_segments_for_video(video_id)
            
            if segments:
                print(f"📦 Found {len(segments)} segments in database:")
                for i, segment in enumerate(segments[:5]):  # Show first 5
                    print(f"   Segment {i:2d}: {segment['start_time']:6.2f}s - {segment['end_time']:6.2f}s "
                          f"({segment['duration']:5.2f}s, status: {segment['status']})")
                
                if len(segments) > 5:
                    print(f"   ... and {len(segments) - 5} more segments")
            else:
                print("❌ No segments found in database")
                
    except Exception as e:
        print(f"❌ Database query failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())