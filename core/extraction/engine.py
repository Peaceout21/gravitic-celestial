import os
import json
import sys

# Patch for Python 3.9 importlib.metadata issues
if sys.version_info < (3, 10):
    import importlib_metadata
    import importlib.metadata
    importlib.metadata.packages_distributions = importlib_metadata.packages_distributions

from typing import List
from google import generativeai as genai
from core.models import EarningsReport

# Configure Google Generative AI
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))

class ExtractionEngine:
    def __init__(self, model_name: str = "gemini-2.0-flash-exp"):
        self.model_name = model_name
        self.model = genai.GenerativeModel(self.model_name)

    def extract_from_text(self, text: str, ticker: str) -> EarningsReport:
        """
        Extracts structured earnings data from raw text using Gemini 2.0.
        """
        prompt = f"""
        Extract the earnings details for {ticker} from the following press release.
        Return the result EXCLUSIVELY as a JSON object matching the following structure:
        
        Structure:
        {{
            "ticker": "string",
            "company_name": "string",
            "fiscal_period": "string",
            "kpis": [{{ "name": "string", "value_actual": "string", "value_consensus": "string", "period": "string", "is_beat": boolean, "context": "string" }}],
            "guidance": [{{ "metric": "string", "midpoint": float, "unit": "string", "commentary": "string" }}],
            "summary": {{ "bull_case": ["string"], "bear_case": ["string"], "key_themes": ["string"] }},
            "source_urls": ["string"]
        }}

        TEXT:
        {text}
        """

        try:
            response = self.model.generate_content(prompt)
            json_str = response.text
            # Clean up JSON formatting if present
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0].strip()
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0].strip()
            
            data = json.loads(json_str)
            return EarningsReport(**data)
        except Exception as e:
            print(f"An error occurred during extraction: {e}")
            raise

if __name__ == "__main__":
    # Ensure GOOGLE_API_KEY is in environment
    if not os.environ.get("GOOGLE_API_KEY"):
        print("ERROR: GOOGLE_API_KEY not found in environment.")
        sys.exit(1)

    engine = ExtractionEngine()
    
    with open("data/raw/nvda_q3_2024_pr.txt", "r") as f:
        mock_pr = f.read()
    
    print(f"Running live extraction for NVDA...")
    report = engine.extract_from_text(mock_pr, "NVDA")
    print("\n--- EXTRACTED REPORT ---")
    print(report.model_dump_json(indent=2))
