#!/usr/bin/env python3
"""Simple Google Translate API test with environment credentials."""

import os
import json
import tempfile
from google.cloud import translate_v3 as translate
from google.oauth2 import service_account
from config import DubbingConfig

def test_translation():
    """Test Google Translate API with environment credentials."""
    
    # Use the same config pattern as the working activities
    config = DubbingConfig.from_env()
    
    print(f"Project ID: {config.google_cloud_project}")
    print(f"Credentials available: {bool(config.google_application_credentials)}")
    
    if not config.google_cloud_project or not config.google_application_credentials:
        print("Missing Google Cloud project or credentials configuration")
        return
    
    try:
        # Use the same authentication pattern as the GCS client
        credentials_value = config.google_application_credentials
        
        # Check if it's a file path or JSON content
        if os.path.isfile(credentials_value):
            credentials = service_account.Credentials.from_service_account_file(credentials_value)
        else:
            # Parse JSON credentials
            credentials_info = json.loads(credentials_value)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Initialize client
        client = translate.TranslationServiceClient(credentials=credentials)
        parent = f"projects/{config.google_cloud_project}/locations/global"
        
        # Test translation
        test_text = """Maya's fingers traced the spine of a leather-bound volume, its gold lettering worn smooth by countless hands before hers. The silence in the old Carnegie library was absolute—no whispered conversations, no rustling pages, no soft footfalls on hardwood floors. Just her and ten thousand forgotten books.
Outside, the city hummed with digital life. Everyone carried the world's knowledge in their pockets now, accessing any text with a thought-speed search. Physical books had become curiosities, then burdens, then finally, obsolete.
She was supposed to catalog everything before the demolition crew arrived next week. The city council had been generous, giving her a full month to document the collection for the digital archive. Most of it was already online anyway.
But Maya couldn't bring herself to reduce Moby Dick to mere metadata, or compress Dickinson's carefully chosen line breaks into database fields. How do you digitize the particular way afternoon light fell across page 237 of someone's favorite novel? How do you preserve the coffee stain on page 15 that marked where a reader had laughed so hard they nearly spilled their cup?"""
        target_language = "ne"
        
        print(f"\nTranslating: '{test_text}' to {target_language}")
        
        response = client.translate_text(
            request={
                "parent": parent,
                "contents": [test_text],
                "mime_type": "text/plain",
                "source_language_code": "en",
                "target_language_code": target_language,
            }
        )
        
        for translation in response.translations:
            print(f"Translation: {translation.translated_text}")
            print(f"Detected language: {translation.detected_language_code}")
        
        print("\n✓ Google Translate API test successful!")
        
    except Exception as e:
        print(f"✗ Translation test failed: {e}")

if __name__ == "__main__":
    test_translation()