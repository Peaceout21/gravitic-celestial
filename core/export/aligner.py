"""
Gravity Auto-Modeler: Data Aligner
Normalizes Celestial (financials) and Nebula (alt data) into a unified timeline.
"""
import pandas as pd
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class DataAligner:
    """
    Aligns diverse data streams into a Quarter-indexed DataFrame.
    """
    
    def __init__(self):
        self.financials_df = None
        self.alt_data_df = None
    
    def align_financials(self, kpis: List[Dict], ticker: str, fiscal_period: str) -> pd.DataFrame:
        """
        Converts Celestial KPIs into a structured DataFrame.
        
        Args:
            kpis: List of KPI dicts with 'name', 'value_actual', 'growth_yoy'.
            ticker: Company ticker symbol.
            fiscal_period: e.g., 'Q3 FY2024'.
            
        Returns:
            DataFrame with columns: Metric, Value, Growth, Source.
        """
        rows = []
        for kpi in kpis:
            rows.append({
                "Ticker": ticker,
                "Fiscal_Period": fiscal_period,
                "Metric": kpi.get('name', 'Unknown'),
                "Value": kpi.get('value_actual', 'N/A'),
                "Growth_YoY": kpi.get('growth_yoy', 'N/A'),
                "Source": kpi.get('source_text', '')[:200]  # Truncate for Excel comment
            })
        
        self.financials_df = pd.DataFrame(rows)
        return self.financials_df
    
    def align_alt_data(self, nebula_signals: Dict, ticker: str, fiscal_period: str) -> pd.DataFrame:
        """
        Converts Nebula signals into Quarterly rows.
        
        Args:
            nebula_signals: Dict from NebulaBridge.get_company_signals().
            ticker: Company ticker symbol.
            fiscal_period: e.g., 'Q3 FY2024'.
            
        Returns:
            DataFrame with columns: Signal_Type, Metric, Value, Interpretation.
        """
        rows = []
        
        if 'hiring' in nebula_signals:
            h = nebula_signals['hiring']
            rows.append({
                "Ticker": ticker,
                "Fiscal_Period": fiscal_period,
                "Signal_Type": "Hiring",
                "Metric": "Expansion Velocity",
                "Value": h.get('expansion_velocity', 0),
                "Interpretation": h.get('interpretation', '')
            })
            rows.append({
                "Ticker": ticker,
                "Fiscal_Period": fiscal_period,
                "Signal_Type": "Hiring",
                "Metric": "Total Open Roles",
                "Value": h.get('total_open_roles_macro', 0),
                "Interpretation": ""
            })
            
        if 'shipping' in nebula_signals:
            s = nebula_signals['shipping']
            rows.append({
                "Ticker": ticker,
                "Fiscal_Period": fiscal_period,
                "Signal_Type": "Shipping",
                "Metric": "Incoming TEU",
                "Value": s.get('total_inventory_incoming_teu', 0),
                "Interpretation": s.get('interpretation', '')
            })
            
        if 'digital' in nebula_signals:
            d = nebula_signals['digital']
            rows.append({
                "Ticker": ticker,
                "Fiscal_Period": fiscal_period,
                "Signal_Type": "Digital",
                "Metric": "App Store Rank",
                "Value": d.get('current_value', 'N/A'),
                "Interpretation": d.get('interpretation', '')
            })
        
        self.alt_data_df = pd.DataFrame(rows)
        return self.alt_data_df
    
    def get_unified_summary(self) -> pd.DataFrame:
        """
        Returns a merged summary table for the Summary Dashboard tab.
        """
        summary_rows = []
        
        # Pull key financials
        if self.financials_df is not None:
            for _, row in self.financials_df.iterrows():
                summary_rows.append({
                    "Category": "Financial",
                    "Metric": row['Metric'],
                    "Value": row['Value'],
                    "Signal": row['Growth_YoY']
                })
        
        # Pull key alt signals
        if self.alt_data_df is not None:
            for _, row in self.alt_data_df.iterrows():
                summary_rows.append({
                    "Category": "Alternative",
                    "Metric": row['Metric'],
                    "Value": row['Value'],
                    "Signal": row['Interpretation'][:50]
                })
        
        return pd.DataFrame(summary_rows)

if __name__ == "__main__":
    aligner = DataAligner()
    # Test with mock data
    kpis = [{"name": "Revenue", "value_actual": "$18.12B", "growth_yoy": "+206%"}]
    print(aligner.align_financials(kpis, "NVDA", "Q3 FY2024"))
