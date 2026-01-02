import time
import os
from typing import List
from core.ingestion.international.market_registry import MarketRegistry
from core.ingestion.state_manager import StateManager
from core.extraction.engine import ExtractionEngine
from core.synthesis.hybrid_rag import HybridRAGEngine
from core.notifications.client import NotificationClient

class PollingEngine:
    """
    Robust polling engine that:
    1. Uses MarketRegistry to fetch filings from correct source (SEC vs NSE).
    2. Checks SQLite state to avoid duplicates.
    3. Triggers Extraction + RAG Indexing.
    """
    def __init__(self, tickers: List[str]):
        self.tickers = tickers
        self.registry = MarketRegistry()
        self.state = StateManager()
        self.extractor = ExtractionEngine()
        self.rag: HybridRAGEngine | None = None
        self.notifier: NotificationClient | None = None

    def run_once(self):
        """Runs a single polling cycle across all markets."""
        print(f"--- Polling for: {self.tickers} ---")
        
        # Group tickers by market tobatch requests efficiently
        # Actually, our clients take a list of tickers, but they are market-specific.
        # Efficient approach: Group tickers -> Get Client -> Batch Fetch
        
        # Simple simulation: Iterate tickers or batched by market
        # Let's use the registry's grouping logic
        groups = self.registry.group_tickers_by_market(self.tickers)
        
        for market, market_tickers in groups.items():
            if not market_tickers:
                continue
                
            print(f"🌍 querying {market.upper()} for {market_tickers}...")
            # We can grab any client instance from the registry for that market
            # Since our registry logic is per-ticker, let's just grab the first one
            # Ideally Registry gives us the client for the group
            client = self.registry.get_client(market_tickers[0]) # Valid since they are grouped
            
            filings = client.get_latest_filings(market_tickers)

            for filing in filings:
                accession = filing['accession_number']
                ticker = filing['ticker']
                
                if self.state.is_processed(accession):
                    print(f"Skipping {ticker} {accession} (Already processed)")
                    continue
                
                print(f"🚨 NEW FILING FOUND: {ticker} {accession}")
                
                try:
                    filing_obj = filing.get('filing_obj')
                    if filing_obj:
                        print(f"📥 Fetching text for {ticker}...")
                        text_content = client.get_filing_text(filing_obj)
                        
                        if text_content:
                            preview_len = min(200, len(text_content))
                            print(f"📄 Content Preview: {text_content[:preview_len]}...")
                            
                            # Trigger Extraction
                            print(f"🤖 Generating Earnings Note for {ticker}...")
                            report = self.extractor.extract_from_text(text_content, ticker)
                            
                            # Save Report to Disk
                            self._save_report(report, ticker, accession)
                            print(f"📝 Report saved to data/reports/{ticker}_{accession}.md")
                        else:
                            print("⚠️ No content extracted.")
                    else:
                        print("⚠️ No filing object available.")
                    
                    self.state.mark_processed(accession, ticker, filing['filing_date'])
                    print(f"✅ Processed {ticker} {accession}")
                    
                except Exception as e:
                    print(f"❌ Error processing {ticker}: {e}")

    def start_loop(self, interval_seconds: int = 60):
        print("🚀 Starting Polling Engine...")
        while True:
            try:
                self.run_once()
                print(f"Sleeping for {interval_seconds}s...")
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                print("Stopping poller.")
                break
    def _save_report(self, report, ticker, accession):
        """Saves the extracted report as a Markdown file."""
        output_dir = "data/reports"
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{output_dir}/{ticker}_{accession}.md"
        
        with open(filename, "w") as f:
            f.write(f"# 📊 Earnings Note: {report.company_name} ({report.ticker})\n")
            f.write(f"**Fiscal Period**: {report.fiscal_period}\n")
            f.write(f"**ID**: {accession}\n\n")
            
            f.write("## 🟢 Key KPIs\n")
            for kpi in report.kpis:
                f.write(f"- **{kpi.name}**: {kpi.value_actual} (Context: {kpi.context})\n")
            
            f.write("\n## 🧭 Guidance\n")
            for guide in report.guidance:
                f.write(f"- **{guide.metric}**: {guide.midpoint} {guide.unit} ({guide.commentary})\n")
            
            if report.summary:
                f.write("\n## 📝 Summary\n")
                if report.summary.bull_case:
                    f.write("**Bull Case**:\n" + "\n".join([f"- {i}" for i in report.summary.bull_case]) + "\n")
                if report.summary.bear_case:
                    f.write("**Bear Case**:\n" + "\n".join([f"- {i}" for i in report.summary.bear_case]) + "\n")
        
        # Send Notification
        notifier = self._get_notifier()
        notifier.send_report_alert(report, filename)

    def _get_notifier(self) -> NotificationClient:
        if self.notifier is None:
            self.notifier = NotificationClient()
        return self.notifier

    def _get_rag(self) -> HybridRAGEngine:
        if self.rag is None:
            self.rag = HybridRAGEngine()
        return self.rag

if __name__ == "__main__":
    poller = PollingEngine(tickers=["NVDA", "AMD", "INTC"])
    poller.run_once()
