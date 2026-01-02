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
        self.conn = None
        self._init_db()

    def _init_db(self):
        """Initializes the SQLite database with necessary tables."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        c = self.conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS processed_filings (
                accession_number TEXT PRIMARY KEY,
                ticker TEXT,
                filing_date TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS scheduler_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                job_id TEXT,
                scheduled_run_time TEXT,
                exception TEXT,
                traceback TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()

    def close(self):
        """Closes the SQLite connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def is_processed(self, accession_number: str) -> bool:
        """Checks if a filing has already been processed."""
        c = self.conn.cursor()
        c.execute('SELECT 1 FROM processed_filings WHERE accession_number = ?', (accession_number,))
        result = c.fetchone()
        return result is not None

    def mark_processed(self, accession_number: str, ticker: str, filing_date: str):
        """Marks a filing as processed."""
        c = self.conn.cursor()
        try:
            c.execute(
                'INSERT OR IGNORE INTO processed_filings (accession_number, ticker, filing_date) VALUES (?, ?, ?)',
                (accession_number, ticker, filing_date)
            )
            self.conn.commit()
        except Exception as e:
            print(f"Error marking state: {e}")

    def get_processed_count(self) -> int:
        """Returns the total number of processed filings."""
        c = self.conn.cursor()
        c.execute('SELECT COUNT(*) FROM processed_filings')
        count = c.fetchone()[0]
        return count

    def record_scheduler_event(
        self,
        event_type: str,
        job_id: Optional[str],
        scheduled_run_time: Optional[str],
        exception: Optional[str],
        traceback: Optional[str],
    ) -> None:
        """Records scheduler event metadata for later inspection."""
        c = self.conn.cursor()
        try:
            c.execute(
                '''
                INSERT INTO scheduler_events (
                    event_type,
                    job_id,
                    scheduled_run_time,
                    exception,
                    traceback
                ) VALUES (?, ?, ?, ?, ?)
                ''',
                (event_type, job_id, scheduled_run_time, exception, traceback),
            )
            self.conn.commit()
        except Exception as e:
            print(f"Error recording scheduler event: {e}")
