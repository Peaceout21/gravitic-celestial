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
        Prioritizes Exhibit 99.1 (Press Release) if available, 
        concatenating it with the main filing text.
        """
        try:
            if not filing_obj:
                return None
                
            content = ""
            
            # 1. Get main filing text
            if hasattr(filing_obj, 'markdown'):
                main_text = filing_obj.markdown()
            elif hasattr(filing_obj, 'text'):
                main_text = filing_obj.text()
            else:
                main_text = ""
                
            if main_text:
                content += f"--- MAIN FILING (FORM {filing_obj.form}) ---\n{main_text}\n"

            # 2. Check for Exhibit 99.1 (Press Release)
            if hasattr(filing_obj, 'attachments'):
                # attachments is a list-like object
                for attachment in filing_obj.attachments:
                    # Check description or filename. Usually 'EX-99.1' or 'ex99-1'
                    desc = getattr(attachment, 'description', '').upper()
                    doc_name = getattr(attachment, 'document', '').upper()
                    
                    is_press_release = (
                        'EX-99.1' in desc or 
                        'PRESS RELEASE' in desc or 
                        'EX991' in doc_name or
                        'EX-99.1' in doc_name
                    )

                    if is_press_release:
                        print(f"  📎 Found Exhibit 99.1 ({doc_name}), extracting...")
                        
                        att_text = None
                        # Prefer markdown from attachment if the library supports it
                        if hasattr(attachment, 'markdown'):
                            att_text = attachment.markdown()
                        elif hasattr(attachment, 'text'):
                            att_text = attachment.text()
                        elif hasattr(attachment, 'download'):
                            # download() usually returns the raw document (HTML or Text)
                            # We'll treat it as text/html for now
                            att_text = attachment.download()
                            if isinstance(att_text, bytes):
                                att_text = att_text.decode('utf-8', errors='ignore')
                        
                        if att_text:
                            content += f"\n\n--- EXHIBIT 99.1 (PRESS RELEASE) ---\n{att_text}"
            
            return content.strip() if content else None
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
