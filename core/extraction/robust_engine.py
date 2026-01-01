from google import generativeai as genai
import time
import functools
from typing import Optional, Any
from google.api_core import exceptions

class RobustGenerationEngine:
    """
    Wrapper for Gemini API calls with automatic model fallback and retry logic.
    Primary: Gemini 3 Flash
    Fallback: Gemini 2.0 Flash
    """
    def __init__(self, 
                 primary_model: str = "gemini-3-flash-preview", 
                 fallback_model: str = "gemini-2.0-flash-exp"):
        self.primary_model_name = primary_model
        # Use updated 2.0 Flash model name
        self.fallback_model_name = fallback_model
        
        self.primary_model = genai.GenerativeModel(self.primary_model_name)
        self.fallback_model = genai.GenerativeModel(self.fallback_model_name)

    def generate_content_with_fallback(self, contents: Any, **kwargs) -> Any:
        """
        Attempts generation with primary model. If it fails (503/ResourceExhausted),
        retries with exponential backoff, then falls back to secondary model.
        """
        try:
            print(f"🤖 Attempting generation with {self.primary_model_name}...")
            return self._generate_with_retry(self.primary_model, contents, **kwargs)
        
        except Exception as e:
            print(f"⚠️ Primary model failed: {e}")
            print(f"🔄 Falling back to {self.fallback_model_name}...")
            
            try:
                return self._generate_with_retry(self.fallback_model, contents, **kwargs)
            except Exception as e2:
                print(f"❌ Fallback model also failed: {e2}")
                raise e2

    def _generate_with_retry(self, model, contents, max_retries=3, **kwargs):
        """Helper to retry generation with exponential backoff."""
        for attempt in range(max_retries):
            try:
                return model.generate_content(contents, **kwargs)
            except exceptions.ServiceUnavailable as e:
                wait_time = 2 ** attempt
                print(f"   Server busy. Retrying in {wait_time}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            except Exception as e:
                # For non-retryable errors (like 400 Bad Request), fail immediately
                raise e
        
        raise exceptions.ServiceUnavailable("Max retries exceeded")

if __name__ == "__main__":
    import os
    if not os.environ.get("GOOGLE_API_KEY"):
        print("Set GOOGLE_API_KEY to test.")
    else:
        genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))
        engine = RobustGenerationEngine()
        try:
            response = engine.generate_content_with_fallback("Hello, are you online?")
            print(f"✅ Success! Response: {response.text}")
        except Exception as e:
            print(f"Test failed: {e}")
