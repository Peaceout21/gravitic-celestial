import os
import sqlite3
import pandas as pd
import logging
from typing import Dict, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)

class NebulaBridge:
    """
    SOTA Bridge: Connects Gravitic Celestial to the Nebula Signal Store.
    Ingests alternative data signals (Hiring, Shipping, Digital) for fusion.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        # Default to the known relative path in the scratch setup
        if not db_path:
            self.db_path = Path("/Users/arjun/.gemini/antigravity/scratch/gravitic-nebula/nebula_signals.db")
        else:
            self.db_path = Path(db_path)
            
        if not self.db_path.exists():
            logger.warning(f"Nebula Signal Store not found at {self.db_path}. Fusion metrics will be unavailable.")

    def get_company_signals(self, ticker: str) -> Dict[str, any]:
        """
        Retrieves all cached signals for a ticker from Nebula.
        """
        if not self.db_path.exists():
            return {}

        try:
            conn = sqlite3.connect(self.db_path)
            # We query the 'signals' table which stores JSON blobs of our SOTA metrics
            query = "SELECT signal_type, data_json, timestamp FROM signals WHERE ticker = ? ORDER BY timestamp DESC"
            df = pd.read_sql_query(query, conn, params=(ticker,))
            conn.close()

            if df.empty:
                return {}

            # Pivot to latest unique signal per type
            latest_signals = {}
            import json
            for _, row in df.drop_duplicates('signal_type').iterrows():
                latest_signals[row['signal_type']] = json.loads(row['data_json'])
            
            return latest_signals

        except Exception as e:
            logger.error(f"Failed to bridge Nebula signals: {e}")
            return {}

    def get_alpha_context(self, ticker: str) -> str:
        """
        Generates a text summary of alternative data for Gemini prompt injection.
        """
        signals = self.get_company_signals(ticker)
        if not signals:
            return "No alternative data signals available from Nebula."

        context = "### Nebula Alternative Data Signals:\n"
        
        if 'hiring' in signals:
            h = signals['hiring']
            context += f"- **Hiring Velocity**: {h.get('expansion_velocity', 'N/A')} ({h.get('total_open_roles_macro', 'N/A')} active roles). {h.get('interpretation', '')}\n"
            
        if 'shipping' in signals:
            s = signals['shipping']
            context += f"- **Shipping Volume**: {s.get('total_inventory_incoming_teu', 'N/A')} TEU. {s.get('interpretation', '')}\n"
            
        if 'digital' in signals:
            d = signals['digital']
            context += f"- **Digital Footprint**: Rank {d.get('current_value', 'N/A')}. {d.get('interpretation', '')}\n"

        if 'social' in signals:
            sc = signals['social']
            context += f"- **Social Sentiment**: {sc.get('signal', 'Neutral')} ({sc.get('sentiment_score', 'N/A')}). {sc.get('interpretation', '')}\n"

        return context

if __name__ == "__main__":
    # Quick Test
    bridge = NebulaBridge()
    print(bridge.get_alpha_context("Nvidia"))
