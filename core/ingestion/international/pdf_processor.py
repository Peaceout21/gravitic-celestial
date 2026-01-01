import requests
import os
import tempfile
from typing import Optional

class PdfProcessor:
    """
    Utility for handling PDF downloads and basic processing for international clients.
    """
    
    @staticmethod
    def download_pdf(url: str, timeout: int = 15) -> Optional[str]:
        """
        Downloads a PDF from a URL to a temporary file.
        Returns the absolute path to the temporary file.
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
            }
            response = requests.get(url, headers=headers, stream=True, timeout=timeout)
            
            if response.status_code == 200:
                # Create a temp file
                fd, path = tempfile.mkstemp(suffix=".pdf")
                with os.fdopen(fd, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return path
            else:
                print(f"Failed to download PDF. Status: {response.status_code}")
                
        except Exception as e:
            print(f"Error downloading PDF from {url}: {e}")
            
        return None
