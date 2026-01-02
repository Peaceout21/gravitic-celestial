import os
import sqlite3
from typing import List, Optional, Tuple

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
            CREATE TABLE IF NOT EXISTS failed_filings (
                accession_number TEXT PRIMARY KEY,
                ticker TEXT,
                filing_date TEXT,
                error_type TEXT,
                error_message TEXT,
                failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                retry_count INTEGER DEFAULT 0
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

    def mark_failed(
        self,
        accession_number: str,
        ticker: str,
        filing_date: str,
        error_type: str,
        error_message: str,
        retention_days: int = 30,
    ):
        """Records a failed filing attempt for diagnostics and retries."""
        c = self.conn.cursor()
        try:
            c.execute(
                '''
                INSERT INTO failed_filings (
                    accession_number,
                    ticker,
                    filing_date,
                    error_type,
                    error_message,
                    failed_at
                ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(accession_number) DO UPDATE SET
                    ticker = excluded.ticker,
                    filing_date = excluded.filing_date,
                    error_type = excluded.error_type,
                    error_message = excluded.error_message,
                    failed_at = CURRENT_TIMESTAMP
                ''',
                (accession_number, ticker, filing_date, error_type, error_message),
            )
            if retention_days > 0:
                self.cleanup_failures(retention_days)
            self.conn.commit()
        except Exception as e:
            print(f"Error marking failure: {e}")

    def increment_retry(self, accession_number: str):
        """Increments the retry counter for a failed filing."""
        c = self.conn.cursor()
        try:
            c.execute(
                '''
                UPDATE failed_filings
                SET retry_count = retry_count + 1,
                    failed_at = CURRENT_TIMESTAMP
                WHERE accession_number = ?
                ''',
                (accession_number,),
            )
            self.conn.commit()
        except Exception as e:
            print(f"Error incrementing retry count: {e}")

    def get_failed(self, limit: int = 100) -> List[Tuple[str, str, str, str, str, str, int]]:
        """Returns recent failed filings for diagnostics."""
        c = self.conn.cursor()
        c.execute(
            '''
            SELECT accession_number,
                   ticker,
                   filing_date,
                   error_type,
                   error_message,
                   failed_at,
                   retry_count
            FROM failed_filings
            ORDER BY failed_at DESC
            LIMIT ?
            ''',
            (limit,),
        )
        return c.fetchall()

    def cleanup_failures(self, retention_days: int = 30):
        """Deletes failed filings older than the retention window."""
        if retention_days <= 0:
            return
        c = self.conn.cursor()
        c.execute(
            'DELETE FROM failed_filings WHERE failed_at < datetime("now", ?)',
            (f"-{retention_days} days",),
        )
        self.conn.commit()
