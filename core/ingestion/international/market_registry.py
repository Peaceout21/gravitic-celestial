from typing import Dict
from core.ingestion.edgar_client import EdgarClient
from core.ingestion.international.nse_client import NseClient
from core.ingestion.international.base_client import BaseIngestionClient

class MarketRegistry:
    """
    Factory that returns the correct IngestionClient based on the ticker suffix.
    """
    def __init__(self):
        self.edgar_client = EdgarClient()
        self.nse_client = NseClient()

    def get_client(self, ticker: str) -> BaseIngestionClient:
        """
        Returns a client instance for the given ticker.
        Rules:
        - Ends with '.NS' -> NseClient
        - Default -> EdgarClient
        """
        if ticker.endswith('.NS'):
            return self.nse_client
        return self.edgar_client

    def group_tickers_by_market(self, tickers: list[str]) -> Dict[str, list[str]]:
        """
        Groups tickers by their market/client for batched processing.
        Returns: { 'edgar': ['NVDA'], 'nse': ['RELIANCE.NS'] }
        """
        groups = {'edgar': [], 'nse': []}
        for t in tickers:
            if t.endswith('.NS'):
                groups['nse'].append(t)
            else:
                groups['edgar'].append(t)
        return groups
