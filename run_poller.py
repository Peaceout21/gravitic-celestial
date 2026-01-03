#!/usr/bin/env python3
"""
CLI entry point for the Financial Analyst Polling Engine.

Usage:
    # Run with default interval (every 5 minutes)
    python run_poller.py NVDA AMD INTC

    # Run with custom interval
    python run_poller.py --interval 10 NVDA AMD

    # Run with cron expression
    python run_poller.py --cron "*/15 * * * *" NVDA AMD
"""

import argparse
import logging
import os
import sys

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.ingestion.polling_engine import PollingEngine
from core.logging_config import configure_logging


def main():
    parser = argparse.ArgumentParser(
        description="Run the Financial Analyst Polling Engine"
    )
    parser.add_argument(
        "tickers",
        nargs="+",
        help="Ticker symbols to monitor (e.g., NVDA AMD RELIANCE.NS)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Polling interval in minutes (default: 5)"
    )
    parser.add_argument(
        "--cron",
        type=str,
        default=None,
        help="Cron expression for scheduling (e.g., '*/15 * * * *')"
    )
    parser.add_argument(
        "--simple",
        action="store_true",
        help="Use simple loop instead of APScheduler"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum worker threads for concurrent filing processing"
    )
    
    args = parser.parse_args()

    configure_logging(
        log_level=args.log_level,
        log_format=args.log_format,
        log_file=args.log_file,
        console=args.console,
    )
    logger = logging.getLogger("run_poller")

    logger.info("📊 Gravitic Financial Analyst")
    logger.info("   Monitoring: %s", args.tickers)
    
    engine = PollingEngine(tickers=args.tickers, max_workers=args.max_workers)
    
    if args.simple:
        engine.start_loop(interval_seconds=args.interval * 60)
    else:
        engine.start_scheduled(
            cron_expression=args.cron,
            interval_minutes=args.interval,
            misfire_grace_seconds=args.misfire_grace_seconds,
        )


if __name__ == "__main__":
    main()
