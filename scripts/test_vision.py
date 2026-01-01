import sys
import os
sys.path.append(os.getcwd())

# Patch for Python 3.9
if sys.version_info < (3, 10):
    try:
        import importlib_metadata
        import importlib.metadata
        importlib.metadata.packages_distributions = importlib_metadata.packages_distributions
    except ImportError:
        pass

from PIL import Image, ImageDraw, ImageFont
from google import generativeai as genai
import json

def create_test_slide():
    """Creates a mock investor slide image for testing."""
    img = Image.new('RGB', (800, 600), color='white')
    draw = ImageDraw.Draw(img)
    
    # Title
    draw.rectangle([0, 0, 800, 80], fill='#1a1a2e')
    draw.text((50, 25), "NVIDIA Q3 FY2024 - Financial Highlights", fill='white')
    
    # Content
    draw.text((50, 120), "Record Revenue: $18.12 Billion", fill='black')
    draw.text((50, 160), "Data Center Revenue: $14.51 Billion (+279% YoY)", fill='black')
    draw.text((50, 200), "GAAP Gross Margin: 74.0%", fill='black')
    draw.text((50, 240), "Non-GAAP EPS: $4.02", fill='black')
    
    draw.text((50, 320), "Q4 FY2024 Guidance:", fill='#1a1a2e')
    draw.text((50, 360), "Revenue: $20.0 Billion (+/- 2%)", fill='black')
    draw.text((50, 400), "Gross Margins: ~74.5%", fill='black')
    
    draw.text((50, 480), "Key Theme: AI Revolution & Accelerated Computing", fill='#667eea')
    
    img_path = "data/processed/test_slide.png"
    img.save(img_path)
    return img_path

def test_vision_with_image():
    print("\n--- Testing Gemini 2.5 Pro Vision with Image ---")
    
    if not os.environ.get("GOOGLE_API_KEY"):
        print("ERROR: GOOGLE_API_KEY not set.")
        sys.exit(1)
    
    genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))
    
    # Create test slide
    img_path = create_test_slide()
    print(f"Created test slide: {img_path}")
    
    # Load image
    img = Image.open(img_path)
    
    # Use Gemini 3 Flash
    model = genai.GenerativeModel("gemini-3-flash-preview")
    print(f"Model: gemini-3-flash-preview")
    
    prompt = """
You are a senior equity research analyst. Analyze this investor slide for NVDA.

Extract all key financial information and return a JSON object with this structure:
{
    "ticker": "NVDA",
    "company_name": "string",
    "fiscal_period": "string",
    "kpis": [{"name": "string", "value_actual": "string", "context": "string"}],
    "guidance": [{"metric": "string", "midpoint": number, "unit": "string"}],
    "key_themes": ["string"]
}

Return ONLY valid JSON, no other text.
"""
    
    try:
        response = model.generate_content([prompt, img])
        
        json_str = response.text
        if "```json" in json_str:
            json_str = json_str.split("```json")[1].split("```")[0].strip()
        elif "```" in json_str:
            json_str = json_str.split("```")[1].split("```")[0].strip()
        
        print("\n--- RAW RESPONSE ---")
        print(json_str[:500])
        
        data = json.loads(json_str)
        print("\n--- EXTRACTED DATA ---")
        print(json.dumps(data, indent=2))
        print("\n✅ Gemini 2.5 Pro Vision extraction successful!")
        
    except Exception as e:
        print(f"\n❌ Vision extraction failed: {e}")
        raise

if __name__ == "__main__":
    test_vision_with_image()
