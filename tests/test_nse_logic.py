import unittest
from unittest.mock import MagicMock, patch
from core.ingestion.international.nse_client import NseClient

class TestNseClient(unittest.TestCase):
    
    @patch('core.ingestion.international.nse_client.feedparser.parse')
    @patch('core.ingestion.international.nse_client.PdfProcessor.download_pdf')
    def test_pdf_extraction_trigger(self, mock_download, mock_feed_parse):
        # Setup Mock Feed
        mock_entry = MagicMock()
        mock_entry.title = "REL : Press Release regarding Quarterly Results"
        mock_entry.link = "https://nsearchives.nseindia.com/corporate/REL_results.pdf"
        mock_entry.id = "123456"
        mock_entry.published = "Mon, 01 Jan 2026 10:00:00 GMT"
        mock_entry.description = "Summary text"
        mock_entry.summary = "More summary" # Explicitly set to string to avoid MagicMock auto-creation
        
        mock_feed = MagicMock()
        mock_feed.entries = [mock_entry]
        mock_feed_parse.return_value = mock_feed
        
        # Setup Mock Download
        mock_download.return_value = "/tmp/mocked_file.pdf"
        
        # Instantiate Client
        client = NseClient()
        tickers = ["REL.NS"]
        
        # 1. Test get_latest_filings
        filings = client.get_latest_filings(tickers)
        self.assertEqual(len(filings), 1)
        self.assertEqual(filings[0]['ticker'], "REL.NS")
        
        # 2. Test get_filing_text triggers download
        filing_obj = filings[0]['filing_obj']
        text = client.get_filing_text(filing_obj)
        
        # Verify
        print(f"Extracted Text: {text}")
        self.assertIn("[PDF_DOWNLOADED: /tmp/mocked_file.pdf]", text)
        mock_download.assert_called_with("https://nsearchives.nseindia.com/corporate/REL_results.pdf")

if __name__ == '__main__':
    unittest.main()
