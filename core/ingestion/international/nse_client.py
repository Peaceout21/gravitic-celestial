from core.ingestion.international.base_client import BaseIngestionClient
from core.ingestion.international.pdf_processor import PdfProcessor
from typing import List, Dict, Optional, Any
import feedparser

class NseClient(BaseIngestionClient):
    """
    Ingestion client for the National Stock Exchange of India (NSE).
    Uses RSS feeds for real-time announcements.
    """
    
    RSS_URL = "https://nsearchives.nseindia.com/content/RSS/Corporate_Announcements.xml"

    def get_latest_filings(self, tickers: List[str], limit: int = 5) -> List[Dict]:
        """
        Fetches latest corporate announcements for NSE tickers.
        """
        results = []
        try:
            feed = feedparser.parse(self.RSS_URL)
            
            for entry in feed.entries[:limit * 2]:
                title = entry.title
                
                matched_ticker = None
                for t in tickers:
                    clean_ticker = t.replace('.NS', '')
                    if clean_ticker in title:
                        matched_ticker = t
                        break
                
                if matched_ticker:
                    # Check for PDF
                    pdf_path = None
                    if entry.link.lower().endswith('.pdf'):
                        # Ideally we download lazily, but for "get_latest" we might just flag it
                        pass
                    
                    results.append({
                        "ticker": matched_ticker,
                        "accession_number": entry.id,
                        "filing_date": entry.published,
                        "form": "Corporate Announcement",
                        "url": entry.link,
                        "filing_obj": entry
                    })
                    
                    if len(results) >= limit:
                        break
                        
        except Exception as e:
            print(f"Error fetching NSE RSS: {e}")
            
        return results

    def get_filing_text(self, filing_obj: Any) -> Optional[str]:
        """
        Extracts description from RSS entry.
        If URL is a PDF, downloads it and returns a marker.
        """
        text = ""
        if hasattr(filing_obj, 'description'):
            text += filing_obj.description + "\n"
        if hasattr(filing_obj, 'summary'):
            text += filing_obj.summary + "\n"
            
        if hasattr(filing_obj, 'link') and filing_obj.link.lower().endswith('.pdf'):
            print(f"📥 Downloading PDF from {filing_obj.link}...")
            local_path = PdfProcessor.download_pdf(filing_obj.link)
            if local_path:
                # We return a special marker that the Extractor can recognize
                text += f"\n[PDF_DOWNLOADED: {local_path}]"
            else:
                text += "\n[PDF_DOWNLOAD_FAILED]"
                
        return text if text.strip() else None

if __name__ == "__main__":
    client = NseClient()
    print("Fetching NSE Announcements...")
    # Note: This will only show something if there's a live announcement for these tickers in the RSS feed
    # For testing, we might want to print *any* announcement.
    feed = feedparser.parse(NseClient.RSS_URL)
    if feed.entries:
        print(f"Found {len(feed.entries)} total announcements.")
        print(f"Latest: {feed.entries[0].title}")
    else:
        print("No entries found in RSS.")
