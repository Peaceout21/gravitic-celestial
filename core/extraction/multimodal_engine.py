import os
import sys
import json
import base64
from typing import List
from PIL import Image

# Patch for Python 3.9 importlib.metadata issues
if sys.version_info < (3, 10):
    try:
        import importlib_metadata
        import importlib.metadata
        importlib.metadata.packages_distributions = importlib_metadata.packages_distributions
    except ImportError:
        pass

from google import generativeai as genai
from core.models import EarningsReport
from core.extraction.slidedeck_parser import SlidedeckParser

# Configure Gemini
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))

class MultiModalExtractor:
    \"\"\"
    Extracts structured financial data from slidedecks using Gemini 3 Flash Vision.
    \"\"\"
    def __init__(self, model_name: str = \"gemini-3-flash-preview\"):
        self.model_name = model_name
        self.model = genai.GenerativeModel(model_name)
        self.parser = SlidedeckParser()


    def extract_from_slidedeck(self, pdf_path: str, ticker: str) -> EarningsReport:
        """
        Extracts structured data from a slidedeck PDF using Gemini 2.5 Pro Vision.
        """
        # Convert PDF to images
        image_paths = self.parser.pdf_to_images(pdf_path)
        
        if not image_paths:
            raise ValueError(f"No images extracted from PDF: {pdf_path}")
        
        # Load images for Gemini
        images = [Image.open(path) for path in image_paths[:10]]  # Limit to first 10 pages for MVP
        
        prompt = f"""
You are a senior equity research analyst. Analyze the following investor slidedeck for {ticker}.

Extract all key financial information and return a JSON object with this exact structure:
{{
    "ticker": "{ticker}",
    "company_name": "string",
    "fiscal_period": "string (e.g., Q3 FY2024)",
    "kpis": [
        {{
            "name": "string (e.g., Revenue, GAAP EPS)",
            "value_actual": "string (e.g., $18.12B)",
            "value_consensus": "string or null",
            "period": "string",
            "is_beat": boolean or null,
            "context": "string (brief explanation)"
        }}
    ],
    "guidance": [
        {{
            "metric": "string",
            "midpoint": 20.0,
            "unit": "string (e.g., billion, %)",
            "commentary": "string"
        }}
    ],
    "key_themes": ["string"],
    "source_urls": []
}}

Pay special attention to:
1. Financial tables with revenue, margins, and EPS
2. Charts showing growth trends
3. Forward-looking guidance statements
4. Management commentary on key themes

Return ONLY the JSON object, no other text.
"""
        
        try:
            # Use native structured output
            generation_config = genai.types.GenerationConfig(
                response_mime_type="application/json",
                # response_schema=EarningsReport # Ideal, but for now enforcing MIME type + Pydantic validation
            )

            response = self.model.generate_content(
                [prompt] + images,
                generation_config=generation_config
            )
            
            # The response is guaranteed to be valid JSON by the model
            json_str = response.text
            data = json.loads(json_str)
            return EarningsReport(**data)
            
        except Exception as e:
            print(f"Error during vision extraction: {e}")
            raise

    def extract_from_images(self, image_paths: List[str], ticker: str) -> EarningsReport:
        """
        Alternative method to extract from pre-converted images.
        """
        images = [Image.open(path) for path in image_paths]
        return self._run_vision_extraction(images, ticker)

if __name__ == "__main__":
    if not os.environ.get("GOOGLE_API_KEY"):
        print("ERROR: GOOGLE_API_KEY not set.")
        sys.exit(1)
    
    print("MultiModalExtractor initialized with Gemini 2.5 Pro Vision.")
    print(f"Model: gemini-2.5-pro-preview-06-05")
    print("Ready for slidedeck analysis.")
