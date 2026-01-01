import os
from typing import Optional
import requests

class FinancialDataProvider:
    """
    Base class for financial data providers (SEC, FMP, etc.)
    """
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FINANCIAL_API_KEY")

    def get_latest_press_release(self, ticker: str) -> str:
        """
        Fetches the latest press release text for a given ticker.
        Placeholder implementation.
        """
        raise NotImplementedError("Subclasses must implement this")

    def get_consensus_estimates(self, ticker: str, period: str) -> dict:
        """
        Fetches analyst estimates for comparison.
        """
        raise NotImplementedError("Subclasses must implement this")
