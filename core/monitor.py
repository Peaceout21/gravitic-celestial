import time
import os
from core.ingestion.sec_polling import SECPoller
from core.extraction.engine import ExtractionEngine

def run_loop(ticker_watchlist: list, user_agent: str):
    """
    The main monitoring loop.
    """
    poller = SECPoller(user_agent=user_agent)
    engine = ExtractionEngine()

    print(f"Monitoring SEC for tickers: {ticker_watchlist}...")

    while True:
        try:
            filings = poller.fetch_latest_filings()
            for filing in filings:
                # In a real app, we'd check if filing['ticker'] is in watchlist
                # For V0, we'll just log any 8-K found
                print(f"New 8-K found: {filing['title']}")
                print(f"Link: {filing['link']}")
                
                # simulate extraction
                # text = poller.get_filing_text(filing['link'])
                # report = engine.extract_from_text(text, filing['ticker'])
                # print(f"Report Generated for {filing['ticker']}")
                
            time.sleep(60) # Poll every minute
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error in loop: {e}")
            time.sleep(10)

if __name__ == "__main__":
    # SEC requires a real user agent
    UA = "FinancialAgentMVP/0.1 (contact@example.com)"
    WATCHLIST = ["NVDA", "AAPL", "TSLA"]
    
    # run_loop(WATCHLIST, UA)
    print("Monitor initialized. Run with 'python core/monitor.py' after setting API keys.")
