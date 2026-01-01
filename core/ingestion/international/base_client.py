from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any

class BaseIngestionClient(ABC):
    """
    Abstract Base Class for all market ingestion clients (SEC, NSE, LSE, etc.).
    Enforces a consistent interface for the PollingEngine.
    """
    
    @abstractmethod
    def get_latest_filings(self, tickers: List[str], limit: int = 5) -> List[Dict]:
        """
        Fetches the latest filings for a list of tickers.
        
        Returns a list of dictionaries with standard keys:
        - ticker: str
        - accession_number: str (unique ID)
        - filing_date: str
        - form: str (e.g., '8-K', 'Corporate Announcement')
        - url: str
        - filing_obj: Any (original object for text extraction)
        """
        pass

    @abstractmethod
    def get_filing_text(self, filing_obj: Any) -> Optional[str]:
        """
        Extracts the full text content from the filing object (or via URL).
        Should prioritize Markdown > HTML > Text.
        """
        pass
