#!/usr/bin/env python3
"""
Google Cloud Translation API v3 Advanced - Complete Implementation
Features: Basic translation, batch translation, document translation, adaptive translation
"""

from google.cloud import translate_v3 as translate
from google.oauth2 import service_account
import json
import os
from typing import List, Dict, Optional

class AdvancedTranslator:
    def __init__(self, project_id: str, location: str = "global", credentials_path: Optional[str] = None):
        """
        Initialize the Advanced Translator
        
        Args:
            project_id: Your Google Cloud Project ID
            location: Location for translation (default: "global")
            credentials_path: Path to service account JSON file (optional if using default credentials)
        """
        self.project_id = project_id
        self.location = location
        self.parent = f"projects/{project_id}/locations/{location}"
        
        # Initialize client with credentials if provided
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = translate.TranslationServiceClient(credentials=credentials)
        elif os.getenv('GOOGLE_CLOUD_CREDENTIALS'):
            # Use JSON credentials from environment variable
            credentials_json = os.getenv('GOOGLE_CLOUD_CREDENTIALS')
            credentials_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self.client = translate.TranslationServiceClient(credentials=credentials)
        else:
            # Uses default credentials (GOOGLE_APPLICATION_CREDENTIALS env var)
            self.client = translate.TranslationServiceClient()
    
    def translate_text(self, text: str, target_language: str, source_language: str = None, 
                      model: str = None, glossary_config: Dict = None) -> Dict:
        """
        Translate text using Cloud Translation API v3 Advanced
        
        Args:
            text: Text to translate
            target_language: Target language code (e.g., 'es', 'fr', 'de')
            source_language: Source language code (optional, auto-detected if None)
            model: Translation model to use (optional)
            glossary_config: Glossary configuration (optional)
        
        Returns:
            Dictionary with translation results
        """
        try:
            # Prepare the request
            request = {
                "parent": self.parent,
                "contents": [text],
                "target_language_code": target_language,
            }
            
            if source_language:
                request["source_language_code"] = source_language
            
            if model:
                request["model"] = f"{self.parent}/models/{model}"
            
            if glossary_config:
                request["glossary_config"] = glossary_config
            
            # Make the translation request
            response = self.client.translate_text(request=request)
            
            result = {
                "translated_text": response.translations[0].translated_text,
                "detected_language": response.translations[0].detected_language_code,
                "glossary_translations": getattr(response.translations[0], 'glossary_translation', None)
            }
            
            return result
            
        except Exception as e:
            return {"error": str(e)}
    
    def translate_with_llm(self, text: str, target_language: str, source_language: str = None,
                          temperature: float = 0.0, examples: List[Dict] = None) -> Dict:
        """
        Translate using Translation LLM model
        
        Args:
            text: Text to translate
            target_language: Target language code
            source_language: Source language code (optional)
            temperature: Controls randomness (0.0 = deterministic, 1.0 = creative)
            examples: List of example translation pairs for adaptive translation
        
        Returns:
            Dictionary with translation results
        """
        try:
            request = {
                "parent": self.parent,
                "contents": [text],
                "target_language_code": target_language,
                "model": f"{self.parent}/models/translation-llm"
            }
            
            if source_language:
                request["source_language_code"] = source_language
            
            # Add adaptive translation examples if provided
            if examples:
                adaptive_request = translate.AdaptiveMtTranslateRequest(
                    parent=self.parent,
                    content=[text],
                    reference_sentence_pairs=examples
                )
                response = self.client.adaptive_mt_translate(request=adaptive_request)
                return {
                    "translated_text": response.translations[0].translated_text,
                    "adaptive_translation": True
                }
            
            response = self.client.translate_text(request=request)
            
            return {
                "translated_text": response.translations[0].translated_text,
                "detected_language": response.translations[0].detected_language_code,
                "model": "translation-llm"
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def batch_translate_text(self, input_configs: List[Dict], output_config: Dict,
                           target_languages: List[str], source_language: str = None,
                           models: Dict = None, glossaries: Dict = None) -> str:
        """
        Perform batch translation (asynchronous)
        
        Args:
            input_configs: List of input configurations (Cloud Storage)
            output_config: Output configuration (Cloud Storage)
            target_languages: List of target language codes
            source_language: Source language code (optional)
            models: Dictionary mapping language pairs to models
            glossaries: Dictionary mapping language pairs to glossaries
        
        Returns:
            Operation name for tracking progress
        """
        try:
            request = {
                "parent": self.parent,
                "source_language_code": source_language or "",
                "target_language_codes": target_languages,
                "input_configs": input_configs,
                "output_config": output_config,
            }
            
            if models:
                request["models"] = models
            
            if glossaries:
                request["glossaries"] = glossaries
            
            operation = self.client.batch_translate_text(request=request)
            
            return operation.name
            
        except Exception as e:
            return f"Error: {str(e)}"
    
    def translate_document(self, document_input_config: Dict, target_language: str,
                          source_language: str = None, model: str = None,
                          glossary_config: Dict = None, document_output_config: Dict = None) -> Dict:
        """
        Translate a document while preserving formatting
        
        Args:
            document_input_config: Document input configuration
            target_language: Target language code
            source_language: Source language code (optional)
            model: Translation model to use (optional)
            glossary_config: Glossary configuration (optional)
            document_output_config: Output configuration (optional)
        
        Returns:
            Dictionary with translation results
        """
        try:
            request = {
                "parent": self.parent,
                "target_language_code": target_language,
                "document_input_config": document_input_config,
            }
            
            if source_language:
                request["source_language_code"] = source_language
            
            if model:
                request["model"] = f"{self.parent}/models/{model}"
            
            if glossary_config:
                request["glossary_config"] = glossary_config
            
            if document_output_config:
                request["document_output_config"] = document_output_config
            
            response = self.client.translate_document(request=request)
            
            return {
                "translated_document": response.document_translation,
                "glossary_document_translation": getattr(response, 'glossary_document_translation', None),
                "model": response.model
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def detect_language(self, text: str) -> Dict:
        """
        Detect the language of the input text
        
        Args:
            text: Text to analyze
        
        Returns:
            Dictionary with detection results
        """
        try:
            request = {
                "parent": self.parent,
                "content": text,
            }
            
            response = self.client.detect_language(request=request)
            
            languages = []
            for language in response.languages:
                languages.append({
                    "language_code": language.language_code,
                    "confidence": language.confidence
                })
            
            return {"languages": languages}
            
        except Exception as e:
            return {"error": str(e)}
    
    def get_supported_languages(self, display_language_code: str = "en") -> Dict:
        """
        Get list of supported languages
        
        Args:
            display_language_code: Language for displaying language names
        
        Returns:
            Dictionary with supported languages
        """
        try:
            request = {
                "parent": self.parent,
                "display_language_code": display_language_code,
            }
            
            response = self.client.get_supported_languages(request=request)
            
            languages = []
            for language in response.languages:
                languages.append({
                    "language_code": language.language_code,
                    "display_name": language.display_name,
                    "support_source": language.support_source,
                    "support_target": language.support_target
                })
            
            return {"languages": languages}
            
        except Exception as e:
            return {"error": str(e)}


# Example usage and test functions
def main():
    """
    Example usage of the Advanced Translator
    """
    # Initialize the translator
    PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'your-project-id')
    LOCATION = "global"  # or "us-central1", "europe-west1", etc.
    
    print(f"Using Project ID: {PROJECT_ID}")
    print(f"GOOGLE_CLOUD_CREDENTIALS available: {bool(os.getenv('GOOGLE_CLOUD_CREDENTIALS'))}")
    
    translator = AdvancedTranslator(
        project_id=PROJECT_ID,
        location=LOCATION
    )
    
    # Example 1: Basic text translation
    print("=== Basic Translation ===")
    result = translator.translate_text(
        text="Hello, how are you today?",
        target_language="es",
        source_language="en"
    )
    print(f"Translation: {result}")
    
    # Example 2: Translation with LLM model
    print("\n=== LLM Translation ===")
    result = translator.translate_with_llm(
        text="The quick brown fox jumps over the lazy dog.",
        target_language="fr",
        source_language="en",
        temperature=0.1
    )
    print(f"LLM Translation: {result}")
    
    # Example 3: Language detection
    print("\n=== Language Detection ===")
    result = translator.detect_language("Bonjour, comment allez-vous?")
    print(f"Detected language: {result}")
    
    # Example 4: Get supported languages
    print("\n=== Supported Languages (first 5) ===")
    result = translator.get_supported_languages()
    if "languages" in result:
        for lang in result["languages"][:5]:
            print(f"{lang['language_code']}: {lang['display_name']}")
    
    # Example 5: Document translation configuration
    print("\n=== Document Translation Example ===")
    # This is a configuration example - you'll need actual Cloud Storage paths
    document_config = {
        "gcs_source": {
            "input_uri": "gs://your-bucket/input-document.pdf"
        },
        "mime_type": "application/pdf"
    }
    
    output_config = {
        "gcs_destination": {
            "output_uri_prefix": "gs://your-bucket/translated-documents/"
        }
    }
    
    print("Document translation config prepared (requires actual Cloud Storage setup)")
    
    # Example 6: Batch translation configuration
    print("\n=== Batch Translation Example ===")
    input_configs = [
        {
            "gcs_source": {
                "input_uri": "gs://your-bucket/input-files/*.txt"
            },
            "mime_type": "text/plain"
        }
    ]
    
    batch_output_config = {
        "gcs_destination": {
            "output_uri_prefix": "gs://your-bucket/batch-translated/"
        }
    }
    
    print("Batch translation config prepared (requires actual Cloud Storage setup)")


if __name__ == "__main__":
    main()


# Additional utility functions
def create_glossary_example():
    """
    Example of how to create and use a glossary
    """
    # This would require additional setup and is shown for reference
    glossary_config = {
        "glossary": "projects/your-project/locations/global/glossaries/your-glossary",
        "ignore_case": True
    }
    return glossary_config


def adaptive_translation_examples():
    """
    Example sentence pairs for adaptive translation
    """
    examples = [
        {
            "source_sentence": "Hello",
            "target_sentence": "Hola"
        },
        {
            "source_sentence": "Good morning",
            "target_sentence": "Buenos días"
        },
        {
            "source_sentence": "Thank you",
            "target_sentence": "Gracias"
        }
    ]
    return examples


# Environment setup helper
def setup_environment():
    """
    Helper function to set up environment variables
    """
    setup_instructions = """
    To use this code, you need to:
    
    1. Install the required library:
       pip install google-cloud-translate
    
    2. Set up authentication (choose one):
       a) Set environment variable:
          export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
       
       b) Use Application Default Credentials:
          gcloud auth application-default login
       
       c) Pass credentials_path to the AdvancedTranslator constructor
    
    3. Enable the Cloud Translation API in your Google Cloud Project:
       gcloud services enable translate.googleapis.com
    
    4. Replace 'your-project-id' with your actual Google Cloud Project ID
    
    5. For document and batch translation, set up Cloud Storage buckets
    """
    print(setup_instructions)


if __name__ == "__main__":
    print("Google Cloud Translation API v3 Advanced - Setup Instructions")
    setup_environment()
    print("\n" + "="*50)
    print("Running examples...")
    main()