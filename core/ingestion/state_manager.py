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
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'processed',
                status_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_id INTEGER
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS filing_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                accession_number TEXT,
                ticker TEXT,
                error_type TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self._ensure_column("processed_filings", "status", "TEXT DEFAULT 'processed'")
        self._ensure_column("processed_filings", "status_updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        self._ensure_column("processed_filings", "error_id", "INTEGER")
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        c = self.conn.cursor()
        c.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in c.fetchall()}
        if column not in existing:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def close(self):
        """Closes the SQLite connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def is_processed(self, accession_number: str) -> bool:
        """Checks if a filing has already been processed."""
        c = self.conn.cursor()
        c.execute(
            'SELECT 1 FROM processed_filings WHERE accession_number = ? AND status IN (?, ?)',
            (accession_number, "processed", "in_progress")
        )
        result = c.fetchone()
        return result is not None

    def mark_in_progress(self, accession_number: str, ticker: str, filing_date: str):
        """Marks a filing as in progress."""
        c = self.conn.cursor()
        try:
            c.execute(
                '''
                INSERT INTO processed_filings (accession_number, ticker, filing_date, status, status_updated_at, error_id)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL)
                ON CONFLICT(accession_number) DO UPDATE SET
                    ticker=excluded.ticker,
                    filing_date=excluded.filing_date,
                    status=excluded.status,
                    status_updated_at=CURRENT_TIMESTAMP,
                    error_id=NULL
                ''',
                (accession_number, ticker, filing_date, "in_progress")
            )
            self.conn.commit()
        except Exception as e:
            print(f"Error marking state: {e}")

    def mark_processed(self, accession_number: str, ticker: str, filing_date: str):
        """Marks a filing as processed."""
        c = self.conn.cursor()
        try:
            c.execute(
                '''
                INSERT INTO processed_filings (accession_number, ticker, filing_date, status, status_updated_at, error_id)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL)
                ON CONFLICT(accession_number) DO UPDATE SET
                    ticker=excluded.ticker,
                    filing_date=excluded.filing_date,
                    status=excluded.status,
                    status_updated_at=CURRENT_TIMESTAMP,
                    error_id=NULL
                ''',
                (accession_number, ticker, filing_date, "processed")
            )
            self.conn.commit()
        except Exception as e:
            print(f"Error marking state: {e}")

    def mark_failed(self, accession_number: str, ticker: str, filing_date: str, error_type: str, error_message: str):
        """Marks a filing as failed and stores the error."""
        c = self.conn.cursor()
        try:
            c.execute(
                '''
                INSERT INTO filing_errors (accession_number, ticker, error_type, error_message)
                VALUES (?, ?, ?, ?)
                ''',
                (accession_number, ticker, error_type, error_message)
            )
            error_id = c.lastrowid
            c.execute(
                '''
                INSERT INTO processed_filings (accession_number, ticker, filing_date, status, status_updated_at, error_id)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                ON CONFLICT(accession_number) DO UPDATE SET
                    ticker=excluded.ticker,
                    filing_date=excluded.filing_date,
                    status=excluded.status,
                    status_updated_at=CURRENT_TIMESTAMP,
                    error_id=excluded.error_id
                ''',
                (accession_number, ticker, filing_date, "failed", error_id)
            )
            self.conn.commit()
        except Exception as e:
            print(f"Error marking failure: {e}")

    def cleanup_stale_in_progress(self, max_age_minutes: int = 60) -> int:
        """Marks stale in-progress filings as failed."""
        c = self.conn.cursor()
        c.execute(
            '''
            SELECT accession_number, ticker, filing_date
            FROM processed_filings
            WHERE status = ? AND status_updated_at < datetime('now', ?)
            ''',
            ("in_progress", f"-{max_age_minutes} minutes")
        )
        stale = c.fetchall()
        for accession_number, ticker, filing_date in stale:
            self.mark_failed(
                accession_number,
                ticker or "",
                filing_date or "",
                "stale_in_progress",
                f"Filing stuck in progress for more than {max_age_minutes} minutes."
            )
        return len(stale)

    def get_processed_count(self) -> int:
        """Returns the total number of processed filings."""
        c = self.conn.cursor()
        c.execute('SELECT COUNT(*) FROM processed_filings')
        count = c.fetchone()[0]
        return count
