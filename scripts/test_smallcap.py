from core.ingestion.polling_engine import PollingEngine
import os

def test_smallcap():
    print("--- Running Small-Cap International Test with Report Generation ---")
    
    # Small/Mid-cap mix
    tickers = [
        "RKLB", "LUNR",  # USA (Rocket Lab, Intuitive Machines)
        "ZENTEC.NS", "MTARTECH.NS" # INDIA (Zen Technologies, MTAR)
    ]
    
    # 1. Clear DB to force processing
    if os.path.exists("data/celestial.db"):
        os.remove("data/celestial.db")
        print("🧹 Cleared state database.")
        
    engine = PollingEngine(tickers)
    engine.run_once()

if __name__ == "__main__":
    # Ensure env var is set (using default for test if missing, but it should be set)
    if "GOOGLE_API_KEY" not in os.environ:
        print("⚠️ Warning: GOOGLE_API_KEY not set. Extraction might fail.")
        
    test_smallcap()
