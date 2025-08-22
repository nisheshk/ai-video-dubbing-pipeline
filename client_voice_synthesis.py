#!/usr/bin/env python3
"""Test client for Voice Synthesis workflow using Replicate Speech-02-HD."""

import asyncio
import logging
import sys
import os
from temporalio.client import Client, TLSConfig

from workflows.voice_synthesis_workflow import start_voice_synthesis_workflow, get_voice_synthesis_result
from config import DubbingConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Test the voice synthesis workflow with various scenarios."""
    
    logger.info("🎵 Starting AI Dubbing Voice Synthesis Workflow Test Client")
    
    # Load configuration
    config = DubbingConfig.from_env()
    
    # Validate required environment variables
    required_vars = [
        ("REPLICATE_API_TOKEN", os.getenv("REPLICATE_API_TOKEN")),
        ("TEMPORAL_CLOUD_NAMESPACE", config.temporal_cloud_namespace),
        ("TEMPORAL_CLOUD_ADDRESS", config.temporal_cloud_address),
        ("TEMPORAL_CLOUD_API_KEY", config.temporal_cloud_api_key),
    ]
    
    missing_vars = []
    for var_name, var_value in required_vars:
        if not var_value:
            missing_vars.append(var_name)
    
    if missing_vars:
        logger.error(f"✗ Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set them in your .env.cloud file")
        return 1
    
    try:
        # Connect to Temporal Cloud
        logger.info("Connecting to Temporal Cloud...")
        
        client = await Client.connect(
            config.temporal_cloud_address,
            namespace=config.temporal_cloud_namespace,
            tls=TLSConfig(
                server_root_ca_cert=None,
                client_cert=None,
                client_private_key=None,
            ),
            rpc_metadata={
                "temporal-namespace": config.temporal_cloud_namespace,
                "authorization": f"Bearer {config.temporal_cloud_api_key}"
            }
        )
        
        logger.info("✓ Connected to Temporal Cloud")
        
        # Test scenarios
        test_scenarios = [
            {
                "name": "Spanish TTS Generation",
                "video_id": "e82c5c2a-3099-476d-937b-caf03bcc4043",  # Test video with translations
                "target_language": "es",
                "voice_id": "Friendly_Person",
                "emotion": "neutral"
            },
            {
                "name": "French TTS Generation",
                "video_id": "e82c5c2a-3099-476d-937b-caf03bcc4043",
                "target_language": "fr", 
                "voice_id": "Friendly_Person",
                "emotion": "happy"
            },
            {
                "name": "Nepali TTS Generation",
                "video_id": "e82c5c2a-3099-476d-937b-caf03bcc4043",
                "target_language": "ne",
                "voice_id": "Friendly_Person", 
                "emotion": "neutral"
            }
        ]
        
        print("\n" + "=" * 80)
        print("🎵 VOICE SYNTHESIS WORKFLOW TEST SCENARIOS")
        print("=" * 80)
        
        for i, scenario in enumerate(test_scenarios, 1):
            print(f"{i}. {scenario['name']}")
        
        print("\n0. Run all scenarios")
        print("q. Quit")
        
        choice = input("\nSelect test scenario: ").strip().lower()
        
        if choice == 'q':
            logger.info("Exiting...")
            return 0
        elif choice == '0':
            selected_scenarios = test_scenarios
        else:
            try:
                scenario_index = int(choice) - 1
                if 0 <= scenario_index < len(test_scenarios):
                    selected_scenarios = [test_scenarios[scenario_index]]
                else:
                    logger.error("Invalid selection")
                    return 1
            except ValueError:
                logger.error("Invalid input")
                return 1
        
        # Run selected scenarios
        for scenario in selected_scenarios:
            logger.info(f"\n🚀 Running scenario: {scenario['name']}")
            logger.info(f"   Video ID: {scenario['video_id']}")
            logger.info(f"   Target Language: {scenario['target_language']}")
            logger.info(f"   Voice ID: {scenario['voice_id']}")
            logger.info(f"   Emotion: {scenario['emotion']}")
            
            try:
                # Start voice synthesis workflow
                workflow_id = await start_voice_synthesis_workflow(
                    client,
                    video_id=scenario["video_id"],
                    target_language=scenario["target_language"],
                    voice_config={
                        "voice_id": scenario["voice_id"],
                        "emotion": scenario["emotion"],
                        "language_boost": "Automatic",
                        "english_normalization": True
                    }
                )
                
                logger.info(f"✓ Started workflow: {workflow_id}")
                
                # Ask user if they want to wait for completion
                wait_choice = input(f"\nWait for completion of '{scenario['name']}'? (y/n): ").strip().lower()
                
                if wait_choice == 'y':
                    logger.info("⏳ Waiting for voice synthesis workflow to complete...")
                    
                    try:
                        result = await get_voice_synthesis_result(client, workflow_id)
                        
                        logger.info("=" * 80)
                        logger.info("🎵 VOICE SYNTHESIS WORKFLOW COMPLETED")
                        logger.info("=" * 80)
                        logger.info(f"Success: {result.success}")
                        logger.info(f"Total Segments: {result.total_segments}")
                        logger.info(f"Successful Synthesis: {result.successful_synthesis}")
                        logger.info(f"Success Rate: {result.success_rate:.1%}")
                        logger.info(f"Total Audio Duration: {result.total_audio_duration:.1f}s")
                        logger.info(f"Voice Consistency Score: {result.voice_consistency_score:.2f}")
                        logger.info(f"Audio Quality Score: {result.avg_audio_quality:.2f}")
                        logger.info(f"Processing Time: {result.total_processing_time_seconds:.1f}s")
                        logger.info(f"Estimated Cost: ${result.estimated_cost:.2f}")
                        logger.info(f"Character Count: {result.character_count}")
                        
                        if result.gcs_audio_folder:
                            logger.info(f"Audio Files: {result.gcs_audio_folder}")
                        if result.audio_manifest_path:
                            logger.info(f"Audio Manifest: {result.audio_manifest_path}")
                        
                        if result.error_message:
                            logger.warning(f"Error: {result.error_message}")
                        
                        logger.info("=" * 80)
                        
                        if result.success:
                            logger.info("🎉 Voice synthesis completed successfully!")
                        else:
                            logger.error("❌ Voice synthesis failed")
                        
                    except Exception as workflow_error:
                        logger.error(f"Workflow execution failed: {workflow_error}")
                else:
                    logger.info("Workflow started. You can check its status in Temporal Cloud UI.")
                    
            except Exception as e:
                logger.error(f"Failed to start workflow for scenario '{scenario['name']}': {e}")
                continue
        
        logger.info("\n🎵 Voice synthesis test client finished")
        return 0
        
    except Exception as e:
        logger.error(f"Test client failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))