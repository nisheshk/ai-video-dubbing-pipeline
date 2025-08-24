#!/bin/bash

# AI Dubbing Pipeline - Complete Reset & Run Script
# This script drops all tables, recreates schema, and runs the entire pipeline
# Usage: ./run_complete_pipeline.sh [video_id] [file_name] [target_language]

set -e  # Exit on any error

# Parse command line arguments
VIDEO_ID=${1:-"a82c5c2a-3099-476d-937b-caf03bcc4043"}
FILE_NAME=${2:-"test_video1.mp4"}
TARGET_LANGUAGE=${3:-"es"}

echo "=========================================="
echo "   AI DUBBING PIPELINE - COMPLETE RUN"  
echo "=========================================="

# Check if we're in the right directory
if [ ! -f "reset_and_run_pipeline.py" ]; then
    echo "❌ Error: Please run this script from the AI dubbing project root directory"
    exit 1
fi

# Check if Python environment is set up
if ! command -v python &> /dev/null; then
    echo "❌ Error: Python not found. Please install Python 3.8+"
    exit 1
fi

# Check if required environment file exists
if [ ! -f ".env.cloud" ]; then
    echo "❌ Error: .env.cloud file not found"
    echo "💡 Please copy .env.test to .env.cloud and configure your credentials"
    exit 1
fi

# Source environment variables
echo "🔧 Loading environment variables..."
source .env.cloud

# Check if Temporal server is running
echo "🔍 Checking Temporal server..."
if ! curl -s http://localhost:7233 > /dev/null 2>&1; then
    echo "❌ Temporal server not running. Starting Temporal server..."
    echo "💡 Run this in another terminal: temporal server start-dev"
    echo "❓ Continue anyway? (y/n)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Install/check dependencies
echo "📦 Checking Python dependencies..."
pip install -r requirements.txt --quiet

# Make the Python script executable
chmod +x reset_and_run_pipeline.py

echo ""
echo "🚀 Starting complete AI dubbing pipeline..."
echo "📺 Video ID: $VIDEO_ID"
echo "📄 File Name: $FILE_NAME"
echo "🌍 Target Language: $TARGET_LANGUAGE"
echo ""
echo "Pipeline stages:"
echo "  1. 🗑️  Drop all database tables"
echo "  2. 🏗️  Recreate database schema"  
echo "  3. 🎵 Audio Extraction (FFmpeg)"
echo "  4. 🗣️  Speech Segmentation (Silero VAD)"
echo "  5. 📝 Transcription (OpenAI Whisper)"
echo "  6. 🌍 Translation (Google + OpenAI)"
echo "  7. 🎤 Voice Synthesis (Replicate TTS)"
echo "  8. 🎯 Audio Alignment & Video Stitching"
echo ""
echo "⏱️  Estimated time: 15-30 minutes"
echo ""

# Ask for confirmation
echo "❓ Proceed with complete pipeline execution? (y/n)"
read -r response
if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "⏹️  Cancelled by user"
    exit 0
fi

echo ""
echo "▶️  Starting pipeline execution..."
echo "========================================"

# Run the Python pipeline script with video parameters
python reset_and_run_pipeline.py "$VIDEO_ID" "$FILE_NAME" "$TARGET_LANGUAGE"

# Check exit code
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "🎉 PIPELINE COMPLETED SUCCESSFULLY! 🎉"
    echo "=========================================="
    echo ""
    echo "✅ Your dubbed video is ready!"
    echo "📁 Check the GCS bucket for final outputs:"
    echo "   - final_dubbed_video_*.mp4 (single audio track)"
    echo "   - multitrack_video_*.mp4 (original + dubbed audio)"
    echo ""
    echo "📊 Check the database for detailed metrics and paths"
    echo ""
else
    echo ""
    echo "=========================================="
    echo "❌ PIPELINE FAILED"
    echo "=========================================="
    echo ""
    echo "💡 Check the logs above for error details"
    echo "🔧 Common issues:"
    echo "   - Missing environment variables in .env.cloud"
    echo "   - Temporal server not running"
    echo "   - Database connection issues"
    echo "   - API key configuration problems"
    echo ""
fi

exit $exit_code