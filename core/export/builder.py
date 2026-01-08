"""
Gravity Auto-Modeler: Excel Builder
Generates a professional, audit-ready XLSX from aligned data.
"""
import xlsxwriter
import pandas as pd
from io import BytesIO
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class ExcelBuilder:
    """
    Constructs a multi-tab Excel workbook from aligned financial and alternative data.
    """
    
    def __init__(self, ticker: str, fiscal_period: str):
        self.ticker = ticker
        self.fiscal_period = fiscal_period
        self.output = BytesIO()
        self.workbook = xlsxwriter.Workbook(self.output, {'in_memory': True})
        
        # Define styles
        self.header_format = self.workbook.add_format({
            'bold': True,
            'font_color': 'white',
            'bg_color': '#4472C4',
            'border': 1,
            'align': 'center'
        })
        self.money_format = self.workbook.add_format({'num_format': '$#,##0.00', 'border': 1})
        self.percent_format = self.workbook.add_format({'num_format': '0.00%', 'border': 1})
        self.text_format = self.workbook.add_format({'border': 1, 'text_wrap': True})
        self.highlight_format = self.workbook.add_format({
            'bold': True,
            'bg_color': '#92D050',
            'border': 1
        })
    
    def add_summary_sheet(self, summary_df: pd.DataFrame):
        """
        Creates the 'Summary Dashboard' tab.
        """
        ws = self.workbook.add_worksheet("Summary")
        
        # Title
        ws.merge_range('A1:D1', f'{self.ticker} Earnings Summary ({self.fiscal_period})', self.header_format)
        ws.set_row(0, 25)
        
        # Headers
        headers = ["Category", "Metric", "Value", "Signal"]
        for col, header in enumerate(headers):
            ws.write(2, col, header, self.header_format)
        
        # Data
        for row_idx, row in enumerate(summary_df.itertuples(), start=3):
            ws.write(row_idx, 0, row.Category, self.text_format)
            ws.write(row_idx, 1, row.Metric, self.text_format)
            ws.write(row_idx, 2, str(row.Value), self.text_format)
            ws.write(row_idx, 3, str(row.Signal), self.text_format)
        
        # Formatting
        ws.set_column('A:A', 15)
        ws.set_column('B:B', 25)
        ws.set_column('C:C', 18)
        ws.set_column('D:D', 40)
        
    def add_financials_sheet(self, financials_df: pd.DataFrame):
        """
        Creates the 'Financials' tab with source traceability.
        """
        ws = self.workbook.add_worksheet("Financials")
        
        headers = ["Ticker", "Period", "Metric", "Value", "Growth YoY"]
        for col, header in enumerate(headers):
            ws.write(0, col, header, self.header_format)
        
        for row_idx, row in enumerate(financials_df.itertuples(), start=1):
            ws.write(row_idx, 0, row.Ticker, self.text_format)
            ws.write(row_idx, 1, row.Fiscal_Period, self.text_format)
            ws.write(row_idx, 2, row.Metric, self.text_format)
            
            # Value cell with source comment for traceability
            ws.write(row_idx, 3, row.Value, self.text_format)
            if hasattr(row, 'Source') and row.Source:
                ws.write_comment(row_idx, 3, f"Source: {row.Source}")
            
            ws.write(row_idx, 4, row.Growth_YoY, self.text_format)
        
        ws.set_column('A:A', 10)
        ws.set_column('B:B', 15)
        ws.set_column('C:C', 20)
        ws.set_column('D:D', 15)
        ws.set_column('E:E', 12)
    
    def add_alt_data_sheet(self, alt_df: pd.DataFrame):
        """
        Creates the 'Alternative Data' tab.
        """
        ws = self.workbook.add_worksheet("Alternative Data")
        
        headers = ["Ticker", "Period", "Signal Type", "Metric", "Value", "Interpretation"]
        for col, header in enumerate(headers):
            ws.write(0, col, header, self.header_format)
        
        for row_idx, row in enumerate(alt_df.itertuples(), start=1):
            ws.write(row_idx, 0, row.Ticker, self.text_format)
            ws.write(row_idx, 1, row.Fiscal_Period, self.text_format)
            ws.write(row_idx, 2, row.Signal_Type, self.text_format)
            ws.write(row_idx, 3, row.Metric, self.text_format)
            ws.write(row_idx, 4, str(row.Value), self.text_format)
            ws.write(row_idx, 5, row.Interpretation, self.text_format)
        
        ws.set_column('A:A', 10)
        ws.set_column('B:B', 15)
        ws.set_column('C:C', 12)
        ws.set_column('D:D', 20)
        ws.set_column('E:E', 12)
        ws.set_column('F:F', 50)
    
    def build(self) -> BytesIO:
        """
        Finalizes and returns the Excel file as BytesIO.
        """
        self.workbook.close()
        self.output.seek(0)
        return self.output

if __name__ == "__main__":
    # Test
    builder = ExcelBuilder("NVDA", "Q3 FY2024")
    summary = pd.DataFrame([{"Category": "Financial", "Metric": "Revenue", "Value": "$18B", "Signal": "+206%"}])
    builder.add_summary_sheet(summary)
    result = builder.build()
    with open("test_output.xlsx", "wb") as f:
        f.write(result.read())
    print("Test file generated: test_output.xlsx")
