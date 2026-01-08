"""
Gravity Auto-Modeler: Main Orchestrator
Coordinates data alignment and Excel generation for one-click model export.
"""
from io import BytesIO
from typing import Dict, List, Optional
import logging

from core.export.aligner import DataAligner
from core.export.builder import ExcelBuilder
from core.fusion.nebula_bridge import NebulaBridge

logger = logging.getLogger(__name__)

class AutoModeler:
    """
    Generates a professional Excel model from Celestial financials and Nebula signals.
    """
    
    def __init__(self, ticker: str, fiscal_period: str):
        self.ticker = ticker
        self.fiscal_period = fiscal_period
        self.aligner = DataAligner()
        self.nebula = NebulaBridge()
    
    def generate(self, kpis: List[Dict]) -> BytesIO:
        """
        Main export routine.
        
        Args:
            kpis: List of KPI dictionaries from Celestial extraction.
            
        Returns:
            BytesIO: The generated Excel file in memory.
        """
        logger.info(f"Generating Auto-Model for {self.ticker} ({self.fiscal_period})")
        
        # 1. Align Financials
        financials_df = self.aligner.align_financials(kpis, self.ticker, self.fiscal_period)
        
        # 2. Align Alternative Data (from Nebula)
        nebula_signals = self.nebula.get_company_signals(self.ticker)
        alt_df = self.aligner.align_alt_data(nebula_signals, self.ticker, self.fiscal_period)
        
        # 3. Create Unified Summary
        summary_df = self.aligner.get_unified_summary()
        
        # 4. Build Excel
        builder = ExcelBuilder(self.ticker, self.fiscal_period)
        builder.add_summary_sheet(summary_df)
        builder.add_financials_sheet(financials_df)
        if not alt_df.empty:
            builder.add_alt_data_sheet(alt_df)
        
        return builder.build()
    
    def get_filename(self) -> str:
        """Returns a standardized filename for the export."""
        period_clean = self.fiscal_period.replace(" ", "_").replace("/", "-")
        return f"{self.ticker}_{period_clean}_Master_Model.xlsx"

if __name__ == "__main__":
    # Quick test
    modeler = AutoModeler("NVDA", "Q3 FY2024")
    mock_kpis = [
        {"name": "Revenue", "value_actual": "$18.12B", "growth_yoy": "+206%", "source_text": "Revenue was a record $18.12B, up 206% from a year ago."},
        {"name": "Data Center Revenue", "value_actual": "$14.51B", "growth_yoy": "+279%", "source_text": "Data Center revenue of $14.51B, a record."},
        {"name": "EPS (Diluted)", "value_actual": "$4.02", "growth_yoy": "+593%", "source_text": "GAAP earnings per diluted share of $4.02."}
    ]
    excel_bytes = modeler.generate(mock_kpis)
    with open(modeler.get_filename(), "wb") as f:
        f.write(excel_bytes.read())
    print(f"Model generated: {modeler.get_filename()}")
