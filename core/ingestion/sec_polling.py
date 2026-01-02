import feedparser
import time
import requests
import json
import os
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

class SECPoller:
    """
    Open-source SEC EDGAR RSS Poller.
    Monitors the SEC Atom feed for new 8-K filings.
    """
    RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&company=&dateb=&owner=include&start=0&count=40&output=atom"
    
    def __init__(self, user_agent: str, state_path: str = "data/seen_entries.json"):
        """
        SEC requires a User-Agent header (e.g., "MyCompany MyEmail@example.com")
        """
        self.headers = {"User-Agent": user_agent}
        self.session = requests.Session()
        self.seen_entries = set()
        self.state_path = state_path
        self.etag: Optional[str] = None
        self.last_modified: Optional[str] = None
        self._load_seen_entries()

    def _load_seen_entries(self) -> None:
        if not self.state_path:
            return
        if not os.path.exists(self.state_path):
            return
        try:
            with open(self.state_path, "r") as handle:
                data = json.load(handle)
                self.seen_entries = set(data)
        except Exception:
            self.seen_entries = set()

    def _persist_seen_entries(self) -> None:
        if not self.state_path:
            return
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        with open(self.state_path, "w") as handle:
            json.dump(sorted(self.seen_entries), handle)

    def fetch_latest_filings(self) -> List[Dict]:
        """
        Fetches and parses the latest 8-K filings.
        """
        request_headers = dict(self.headers)
        if self.etag:
            request_headers["If-None-Match"] = self.etag
        if self.last_modified:
            request_headers["If-Modified-Since"] = self.last_modified

        response = self.session.get(self.RSS_URL, headers=request_headers)
        if response.status_code == 304:
            return []
        if response.status_code != 200:
            print(f"Error fetching SEC feed: {response.status_code}")
            return []

        self.etag = response.headers.get("ETag", self.etag)
        self.last_modified = response.headers.get("Last-Modified", self.last_modified)

        feed = feedparser.parse(response.content)
        new_filings = []

        for entry in feed.entries:
            if entry.id not in self.seen_entries:
                filing = {
                    "id": entry.id,
                    "title": entry.title,
                    "link": entry.link,
                    "updated": entry.updated,
                    "ticker": self._extract_ticker(entry.title)
                }
                new_filings.append(filing)
                self.seen_entries.add(entry.id)

        if new_filings:
            self._persist_seen_entries()

        return new_filings

    def _extract_ticker(self, title: str) -> Optional[str]:
        """
        Simple heuristic to extract ticker from SEC title.
        Example title: "8-K - NVIDIA CORP (0001045810) (Filer)"
        """
        try:
            # This is a guestimate; in a real app we'd use a CIK-to-Ticker map
            if "(" in title:
                parts = title.split("(")
                return parts[0].split("-")[-1].strip()
        except:
            pass
        return None

    def get_filing_text(self, entry_url: str) -> str:
        """
        Navigates to the SEC filing page and extracts the main text content.
        """
        # Note: SEC pages are complex; this is a simplified version for V0
        response = self.session.get(entry_url, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the link to the actual .htm or .txt filing
        # Typically the first link in the 'Document' table
        links = soup.find_all('a')
        for link in links:
            if '.htm' in link.get('href', '') and 'ix?doc=' not in link.get('href', ''):
                doc_url = "https://www.sec.gov" + link.get('href')
                doc_res = self.session.get(doc_url, headers=self.headers)
                doc_soup = BeautifulSoup(doc_res.content, 'html.parser')
                return doc_soup.get_text(separator='\n')
        
        return ""

if __name__ == "__main__":
    # Example usage: poller = SECPoller("MyProject contact@example.com")
    print("SEC Poller module defined. Strictly using Open Source libraries (feedparser, bs4).")
