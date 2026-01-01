from edgar import set_identity, Company
import os

# Set identity for SEC EDGAR compliance
identity = os.environ.get("SEC_IDENTITY", "Research Agent contact@example.com")
set_identity(identity)

def inspect_filing_methods():
    print("Fetching NVDA last 8-K...")
    try:
        company = Company("NVDA")
        filing = company.get_filings(form="8-K").latest()
        
        print(f"\n--- Filing Object Type: {type(filing)} ---")
        print(f"Accession: {filing.accession_no}")
        
        # Check for common text extraction methods
        methods = [m for m in dir(filing) if not m.startswith('_')]
        print(f"Available Methods/Attributes: {methods}")
        
        # Try to get text
        if hasattr(filing, 'text'):
            print("\nPreview of .text():")
            print(filing.text()[:200])
        elif hasattr(filing, 'markdown'):
             print("\nPreview of .markdown():")
             print(filing.markdown()[:200])
        elif hasattr(filing, 'html'):
             print("\nPreview of .html():")
             print(filing.html()[:200])
             
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_filing_methods()
