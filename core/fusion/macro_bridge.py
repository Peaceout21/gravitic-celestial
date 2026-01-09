import sqlite3
import pandas as pd
import logging
import os
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class MacroBridge:
    """
    Bridges the gap between Celestial (Dashboard) and Gravitic-Macro (Truth Layer).
    Reads directly from the Macro Signals SQLite database.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        # Default to the known relative path if not provided
        if not db_path:
            # Assumes standard folder structure: .../gravitic-celestial/../gravitic-macro/macro_signals.db
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(base_dir, "..", "gravitic-macro", "macro_signals.db")
            
        self.db_path = os.path.abspath(db_path)
        
        if not os.path.exists(self.db_path):
            logger.warning(f"Macro DB not found at: {self.db_path}. Dashboard will show empty macro data.")

    def get_ticker_signals(self, ticker: str, limit: int = 5) -> pd.DataFrame:
        """
        Fetches latest prediction market signals specifically for a ticker.
        """
        query = """
            SELECT event_title, probability_yes, volume_usd, category, timestamp
            FROM macro_probabilities
            WHERE related_ticker = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        return self._execute_query(query, (ticker, limit))

    def get_sector_signals(self, sector: str, limit: int = 5) -> pd.DataFrame:
        """
        Fetches latest signals for a broader sector (e.g., "AI/Chips", "Macro").
        """
        query = """
            SELECT event_title, probability_yes, volume_usd, related_ticker, timestamp
            FROM macro_probabilities
            WHERE sector = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        return self._execute_query(query, (sector, limit))

    def get_macro_risk_signals(self, limit: int = 5) -> pd.DataFrame:
        """
        Convenience method to get top global macro risks.
        """
        return self.get_sector_signals("Macro", limit)

    def run_live_scan(self, ticker: str, limit: int = 10) -> List[Dict]:
        """
        Performs a full search + hydration loop across workspace boundaries.
        Returns a list of hydrated MacroEvent-like dicts.
        """
        try:
            # 1. Import Macro components (requires them to be in python path)
            from macro_core.scrapers.polymarket import PolymarketScraper
            from macro_core.scrapers.discovery import DiscoveryEngine
            
            scraper = PolymarketScraper(use_llm_filter=False) # No LLM in live scan for speed
            discovery = DiscoveryEngine()
            
            # 2. Search Local Index
            metadata = discovery.search_ticker(ticker, limit=limit)
            
            # 3. Hydrate live data
            hydrated = scraper.hydrate_metadata(metadata)
            
            # 4. Convert to dict for UI consumption
            return [e.dict() for e in hydrated]
            
        except ImportError:
            logger.error("Macro dependencies not available for live scan.")
            return []
        except Exception as e:
            logger.error(f"Live scan failed: {e}")
            return []

    def _execute_query(self, query: str, params: tuple) -> pd.DataFrame:
        """
        Executes SQL and returns a Pandas DataFrame.
        """
        try:
            if not os.path.exists(self.db_path):
                return pd.DataFrame()

            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(query, conn, params=params)
                
                # Format columns if data exists
                if not df.empty:
                    # Convert probability to percentage string for display (optional, or keep float)
                    # df['Odds'] = (df['probability_yes'] * 100).round(1).astype(str) + '%'
                    pass
                return df
                
        except Exception as e:
            logger.error(f"MacroBridge Query Error: {e}")
            return pd.DataFrame()
