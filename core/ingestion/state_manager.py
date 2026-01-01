import os
import sqlite3
from typing import List, Optional

class StateManager:
    """
    Manages simple state persistence using SQLite to track processed filings.
    Prevents duplicate processing of the same 8-K.
    """
    def __init__(self, db_path: str = "data/celestial.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initializes the SQLite database with necessary tables."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS processed_filings (
                accession_number TEXT PRIMARY KEY,
                ticker TEXT,
                filing_date TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def is_processed(self, accession_number: str) -> bool:
        """Checks if a filing has already been processed."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT 1 FROM processed_filings WHERE accession_number = ?', (accession_number,))
        result = c.fetchone()
        conn.close()
        return result is not None

    def mark_processed(self, accession_number: str, ticker: str, filing_date: str):
        """Marks a filing as processed."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        try:
            c.execute(
                'INSERT OR IGNORE INTO processed_filings (accession_number, ticker, filing_date) VALUES (?, ?, ?)',
                (accession_number, ticker, filing_date)
            )
            conn.commit()
        except Exception as e:
            print(f"Error marking state: {e}")
        finally:
            conn.close()

    def get_processed_count(self) -> int:
        """Returns the total number of processed filings."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM processed_filings')
        count = c.fetchone()[0]
        conn.close()
        return count
