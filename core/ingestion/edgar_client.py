from core.ingestion.international.base_client import BaseIngestionClient
from edgar import set_identity, Company, Filings
from typing import List, Dict, Optional
import os

# Set identity for SEC EDGAR compliance (from environment or default)
# Real users must set this environment variable
identity = os.environ.get("SEC_IDENTITY", "Research Agent contact@example.com")
set_identity(identity)

class EdgarClient(BaseIngestionClient):
    """
    Wrapper around edgartools to fetch 8-K filings and latest earnings press releases.
    Automatically handles rate limiting via the library.
    """
    
    def get_latest_filings(self, tickers: List[str], limit: int = 5) -> List[Dict]:
        """
        Fetches the latest 8-K filings for a list of tickers.
        """
        results = []
        for ticker in tickers:
            try:
                company = Company(ticker)
                # Filter for 8-K filings
                filings_container = company.get_filings(form="8-K").latest(limit)
                
                # If it's a single Filing object (not a list/container), wrap it
                # edgartools consistency varies, check provided methods
                if hasattr(filings_container, 'accession_no'):
                    # It's a single filing
                    iterable_filings = [filings_container]
                else:
                    # It's a container, iterate over it
                    iterable_filings = filings_container

                for filing in iterable_filings:
                    # Double check it has the attribute
                    if not hasattr(filing, 'accession_no'):
                        continue
                        
                    results.append({
                        "ticker": ticker,
                        "accession_number": filing.accession_no,
                        "filing_date": str(filing.filing_date),
                        "form": filing.form,
                        "url": filing.url if hasattr(filing, 'url') else "",
                        "filing_obj": filing 
                    })
            except Exception as e:
                print(f"Error fetching filings for {ticker}: {e}")
        
        return results

    def get_filing_text(self, filing_obj) -> Optional[str]:
        """
        Fetches the content of a filing using edgartools methods.
        Prioritizes Markdown > HTML > Text for LLM consumption.
        """
        try:
            if not filing_obj:
                return None
                
            # Try to get Markdown first (cleanest for LLMs)
            if hasattr(filing_obj, 'markdown'):
                content = filing_obj.markdown()
                if content: 
                    return content
            
            # Fallback to HTML
            if hasattr(filing_obj, 'html'):
                content = filing_obj.html()
                if content:
                    return content
                    
            # Fallback to plain text
            if hasattr(filing_obj, 'text'):
                content = filing_obj.text() or ""
            else:
                content = ""
            
            # Check for Exhibit 99.1 (Press Release)
            if hasattr(filing_obj, 'attachments'):
                # attachments is usually a list of Attachment objects
                for attachment in filing_obj.attachments:
                    # Check description or type. Usually 'EX-99.1'
                    # edgartools attachment might have .description or .type
                    desc = getattr(attachment, 'description', '').upper()
                    if 'EX-99.1' in desc or 'PRESS RELEASE' in desc:
                        print("  📎 Found Exhibit 99.1 (Press Release), appending...")
                        # Try to get text of attachment
                        if hasattr(attachment, 'download'):
                            # For text attachments, download returns content
                            # For binary, we might need OCR, but 99.1 is often HTML/Text
                            att_content = attachment.download()
                            if att_content:
                                content += f"\n\n--- EXHIBIT 99.1 ---\n{att_content}"
            
            return content
        except Exception as e:
            print(f"Error extracting text from filing: {e}")
            return None

if __name__ == "__main__":
    # Test
    client = EdgarClient()
    print("Fetching NVDA 8-Ks...")
    filings = client.get_latest_8k_filings(["NVDA"], limit=2)
    for f in filings:
        print(f"- {f['filing_date']} | {f['accession_number']} | {f['url']}")
