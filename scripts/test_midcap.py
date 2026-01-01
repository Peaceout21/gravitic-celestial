from core.ingestion.polling_engine import PollingEngine
import os

def test_midcap():
    print("--- Running Mid-Cap International Test ---")
    
    # Mid-cap / Growth mix
    tickers = [
        "PLTR", "SOFI", "UBER",  # USA
        "TATAELXSI.NS", "KPITTECH.NS" # INDIA
    ]
    
    # 1. Clear DB to force processing
    if os.path.exists("data/celestial.db"):
        os.remove("data/celestial.db")
        print("🧹 Cleared state database.")
        
    engine = PollingEngine(tickers)
    engine.run_once()

if __name__ == "__main__":
    test_midcap()
