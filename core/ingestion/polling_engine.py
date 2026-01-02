import time
import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    def __init__(self, tickers: List[str], max_workers: int | None = None):
        self.tickers = tickers
        self.registry = MarketRegistry()
        self.extractor = ExtractionEngine()
        self.rag: HybridRAGEngine | None = None
        self.notifier: NotificationClient | None = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._state_local = threading.local()
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)

    def _get_state_manager(self) -> StateManager:
        if not hasattr(self._state_local, "state"):
            self._state_local.state = StateManager()
        return self._state_local.state

    def _process_filing(self, filing: dict, client) -> None:
        accession = filing.get('accession_number')
        ticker = filing.get('ticker')

        if not accession or not ticker:
            self.logger.warning("Skipping malformed filing: %s", filing)
            return

        self.logger.info("➡️ Starting task for %s %s", ticker, accession)
        try:
            state = self._get_state_manager()
            if state.is_processed(accession):
                self.logger.info("Skipping %s %s (Already processed)", ticker, accession)
                return

            self.logger.info("🚨 NEW FILING FOUND: %s %s", ticker, accession)

            filing_obj = filing.get('filing_obj')
            if filing_obj:
                self.logger.info("📥 Fetching text for %s...", ticker)
                try:
                    text_content = client.get_filing_text(filing_obj)
                except Exception:
                    self.logger.exception("Failed to fetch filing text for %s", ticker)
                    text_content = None

                if text_content:
                    preview_len = min(200, len(text_content))
                    self.logger.debug("📄 Content Preview: %s...", text_content[:preview_len])

                    # Trigger Extraction
                    self.logger.info("🤖 Generating Earnings Note for %s...", ticker)
                    report = self.extractor.extract_from_text(text_content, ticker)

                    # Save Report to Disk
                    self._save_report(report, ticker, accession)
                    self.logger.info("📝 Report saved to data/reports/%s_%s.md", ticker, accession)
                else:
                    self.logger.warning("⚠️ No content extracted.")
            else:
                self.logger.warning("⚠️ No filing object available.")

            filing_date = filing.get('filing_date')
            if filing_date is None:
                self.logger.warning("Missing filing_date for %s %s", ticker, accession)
                filing_date = ""
            state.mark_processed(accession, ticker, filing_date)
            self.logger.info("✅ Processed %s %s", ticker, accession)

        except Exception:
            self.logger.exception("❌ Error processing %s %s", ticker, accession)
        finally:
            self.logger.info("⬅️ Finished task for %s %s", ticker, accession)

    def run_once(self):
        """Runs a single polling cycle across all markets."""
        if not self.tickers:
            self.logger.warning("No tickers provided; skipping polling cycle.")
            return

        self.logger.info("--- Polling for: %s ---", self.tickers)

        # Group tickers by market tobatch requests efficiently
        # Actually, our clients take a list of tickers, but they are market-specific.
        # Efficient approach: Group tickers -> Get Client -> Batch Fetch

        # Simple simulation: Iterate tickers or batched by market
        # Let's use the registry's grouping logic
        groups = self.registry.group_tickers_by_market(self.tickers)

        for market, market_tickers in groups.items():
            if not market_tickers:
                continue

            self.logger.info("🌍 querying %s for %s...", market.upper(), market_tickers)
            # We can grab any client instance from the registry for that market
            # Since our registry logic is per-ticker, let's just grab the first one
            # Ideally Registry gives us the client for the group
            client = self.registry.get_client(market_tickers[0]) # Valid since they are grouped

            try:
                filings = client.get_latest_filings(market_tickers)
            except Exception:
                self.logger.exception("Failed fetching filings for %s", market_tickers)
                continue

            if not filings:
                continue

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._process_filing, filing, client): filing
                    for filing in filings
                }
                for future in as_completed(futures):
                    filing = futures[future]
                    accession = filing.get('accession_number')
                    ticker = filing.get('ticker')
                    try:
                        future.result()
                    except Exception:
                        self.logger.exception("❌ Unhandled exception for %s %s", ticker, accession)

    def start_loop(self, interval_seconds: int = 60):
        """Legacy simple loop. Use start_scheduled() for production."""
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.logger.info("🚀 Starting Polling Engine (Simple Loop)...")
        while True:
            try:
                self.run_once()
                self.logger.info("Sleeping for %ss...", interval_seconds)
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                self.logger.info("Stopping poller.")
                break

    def start_scheduled(
        self,
        cron_expression: str = None,
        interval_minutes: int = 5,
        misfire_grace_seconds: int | None = None,
    ):
        """
        Production scheduler using APScheduler.
        Args:
            cron_expression: Optional cron string (e.g., "*/5 * * * *" for every 5 mins).
                             If None, uses interval_minutes.
            interval_minutes: Fallback interval if cron not provided.
        """
        try:
            from apscheduler.schedulers.blocking import BlockingScheduler
            from apscheduler.triggers.cron import CronTrigger
            from apscheduler.triggers.interval import IntervalTrigger
            from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
        except ImportError:
            self.logger.error("❌ APScheduler not installed. Run: pip install apscheduler")
            self.logger.info("Falling back to simple loop...")
            self.start_loop(interval_seconds=interval_minutes * 60)
            return

        scheduler = BlockingScheduler()

        def _log_scheduler_event(event):
            scheduled_run_time = getattr(event, "scheduled_run_time", None)
            scheduled_run_time_str = (
                scheduled_run_time.isoformat() if scheduled_run_time else "unknown"
            )
            job_id = getattr(event, "job_id", "unknown")
            if event.exception:
                traceback_str = getattr(event, "traceback", None)
                self.logger.error(
                    "Scheduled job %s failed at %s: %s\n%s",
                    job_id,
                    scheduled_run_time_str,
                    event.exception,
                    traceback_str or "No traceback available.",
                )
            else:
                self.logger.warning(
                    "Scheduled job %s missed its run time at %s.",
                    job_id,
                    scheduled_run_time_str,
                )
                self.state.record_scheduler_event(
                    event_type="misfire",
                    job_id=job_id,
                    scheduled_run_time=scheduled_run_time_str,
                    exception=None,
                    traceback=None,
                )

        scheduler.add_listener(_log_scheduler_event, EVENT_JOB_ERROR | EVENT_JOB_MISSED)

        if cron_expression:
            try:
                trigger = CronTrigger.from_crontab(cron_expression)
            except ValueError:
                self.logger.exception("Invalid cron expression: %s", cron_expression)
                trigger = IntervalTrigger(minutes=interval_minutes)
                self.logger.info("🕐 Scheduled every %s minutes", interval_minutes)
            else:
                self.logger.info("🕐 Scheduled with cron: %s", cron_expression)
        else:
            if interval_minutes <= 0:
                raise ValueError("interval_minutes must be positive")
            trigger = IntervalTrigger(minutes=interval_minutes)
            self.logger.info("🕐 Scheduled every %s minutes", interval_minutes)

        if misfire_grace_seconds is not None and misfire_grace_seconds <= 0:
            raise ValueError("misfire_grace_seconds must be positive")
        scheduler.add_job(
            self.run_once,
            trigger,
            id="polling_job",
            max_instances=1,
            misfire_grace_time=misfire_grace_seconds,
        )

        self.logger.info("🚀 Starting Polling Engine (Scheduled)...")
        self.logger.info("Press Ctrl+C to stop.")

        try:
            # Run once immediately at startup
            self.run_once()
            scheduler.start()
        except KeyboardInterrupt:
            self.logger.info("⛔ Shutting down scheduler...")
            scheduler.shutdown()
            self.logger.info("Stopped.")

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
