from core.ingestion.polling_engine import PollingEngine

def test_international_polling():
    print("--- Testing International Polling Engine ---")
    
    # Mix of US and Indian tickers
    # Note: For NSE test to pass, the tickers need to have recent announcements in the RSS feed
    # otherwise it will just poll properly and process 0 new items.
    tickers = ["NVDA", "INTC", "RELIANCE.NS", "INFY.NS"]
    
    engine = PollingEngine(tickers)
    engine.run_once()

if __name__ == "__main__":
    test_international_polling()
