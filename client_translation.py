#!/usr/bin/env python3
"""Test client for Translation workflow using Google Translate v3 + OpenAI refinement."""

import asyncio
import logging
import sys
import os
from temporalio.client import Client, TLSConfig

from workflows.translation_workflow import start_translation_workflow, get_translation_result
from config import DubbingConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Test the translation workflow with various scenarios."""
    
    logger.info("🌍 Starting AI Dubbing Translation Workflow Test Client")
    
    # Load configuration
    config = DubbingConfig.from_env()
    
    # Validate required environment variables
    required_vars = [
        ("GOOGLE_CLOUD_PROJECT", config.google_cloud_project),
        ("GOOGLE_CLOUD_CREDENTIALS", config.google_application_credentials),
        ("OPENAI_API_KEY", config.openai_api_key),
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
                "name": "English to Spanish (Educational)",
                "video_id": "e82c5c2a-3099-476d-937b-caf03bcc4043",  # Test video with transcriptions
                "target_language": "es",
                "source_language": "en",
                "cultural_context": "educational",
                "enable_refinement": True
            },
            {
                "name": "English to French (General)",
                "video_id": "e82c5c2a-3099-476d-937b-caf03bcc4043",
                "target_language": "fr", 
                "source_language": "en",
                "cultural_context": "general",
                "enable_refinement": True
            },
            {
                "name": "English to Nepaliese (No Refinement - Google Only)",
                "video_id": "e82c5c2a-3099-476d-937b-caf03bcc4043",
                "target_language": "ne",
                "source_language": "en", 
                "cultural_context": "general",
                "enable_refinement": True
            }
        ]
        
        print("\n" + "=" * 80)
        print("🌍 TRANSLATION WORKFLOW TEST SCENARIOS")
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
            logger.info(f"   Cultural Context: {scenario['cultural_context']}")
            logger.info(f"   OpenAI Refinement: {'Enabled' if scenario['enable_refinement'] else 'Disabled'}")
            
            try:
                # Start translation workflow
                workflow_id = await start_translation_workflow(
                    client,
                    video_id=scenario["video_id"],
                    target_language=scenario["target_language"],
                    source_language=scenario["source_language"],
                    cultural_context=scenario["cultural_context"],
                    enable_refinement=scenario["enable_refinement"]
                )
                
                logger.info(f"✓ Started workflow: {workflow_id}")
                
                # Ask user if they want to wait for completion
                wait_choice = input(f"\nWait for completion of '{scenario['name']}'? (y/n): ").strip().lower()
                
                if wait_choice in ['y', 'yes', '1']:
                    logger.info("⏳ Waiting for workflow completion...")
                    
                    result = await get_translation_result(client, workflow_id)
                    
                    # Display results
                    print(f"\n{'=' * 60}")
                    print(f"🌍 TRANSLATION RESULTS - {scenario['name']}")
                    print(f"{'=' * 60}")
                    print(f"Video ID: {result.video_id}")
                    print(f"Source Language: {result.source_language}")
                    print(f"Target Language: {result.target_language}")
                    print(f"Cultural Context: {result.cultural_context}")
                    print(f"Success: {'✓' if result.success else '✗'}")
                    print(f"Total Segments: {result.total_segments}")
                    print(f"Successful Translations: {result.successful_translations}")
                    print(f"Success Rate: {result.success_rate:.1%}")
                    
                    if result.success:
                        print(f"Average Google Confidence: {result.avg_google_confidence:.2f}")
                        print(f"Average Translation Quality: {result.avg_translation_quality:.2f}")
                        print(f"Total Processing Time: {result.total_processing_time_seconds:.1f}s")
                        print(f"  - Google Translate: {result.google_processing_time_seconds:.1f}s")
                        print(f"  - OpenAI Refinement: {result.openai_processing_time_seconds:.1f}s")
                        
                        if result.gcs_translation_path:
                            print(f"Translation File: {result.gcs_translation_path}")
                        
                        # Show sample translations
                        if result.translations:
                            print(f"\n📝 Sample Translations (first 3 segments):")
                            for i, translation in enumerate(result.translations[:3]):
                                print(f"\n  Segment {translation.segment_index}:")
                                print(f"    Source: {translation.source_text[:100]}...")
                                print(f"    Google: {translation.google_translation[:100]}...")
                                print(f"    OpenAI: {translation.openai_refined_translation[:100]}...")
                                if translation.google_confidence_score:
                                    print(f"    Confidence: {translation.google_confidence_score:.2f}")
                    else:
                        print(f"Error: {result.error_message}")
                    
                    print(f"{'=' * 60}")
                
                else:
                    logger.info(f"⏭️  Workflow started, check Temporal UI for progress: {workflow_id}")
                
            except Exception as e:
                logger.error(f"✗ Failed to run scenario '{scenario['name']}': {e}")
                continue
            
            # Add delay between scenarios if running multiple
            if len(selected_scenarios) > 1:
                await asyncio.sleep(2)
        
        logger.info("\n✓ Translation workflow test completed")
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Test interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"✗ Translation test failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    # Environment setup reminder
    env_file = ".env.cloud"
    if not os.path.exists(env_file):
        print(f"⚠️  Warning: {env_file} not found")
        print("Make sure to source your environment variables:")
        print(f"  source {env_file}")
        print()
    
    # Run the test client
    sys.exit(asyncio.run(main()))