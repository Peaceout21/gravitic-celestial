"""
Gravity Auto-Modeler: Stress Test Suite
Verifies robustness across Market Caps (Mega/Mid/Small) and Edge Cases.
"""
import pytest
import os
import sys
from pathlib import Path

# Ensure core modules are in path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core.export.auto_modeler import AutoModeler

# Mock KPIs to simulate diverse company profiles since we lack live PDFs for them
MOCK_SCENARIOS = {
    "MEGA_CAP_NVDA": {
        "ticker": "NVDA",
        "period": "Q3 FY2024",
        "kpis": [
            {"name": "Revenue", "value_actual": "$18.12B", "growth_yoy": "+206%", "source_text": "Record revenue driven by H100 demand."},
            {"name": "Data Center", "value_actual": "$14.51B", "growth_yoy": "+279%", "source_text": "Strongest segment growth."},
            {"name": "Gaming", "value_actual": "$2.86B", "growth_yoy": "+81%", "source_text": "Recovery in channel inventory."},
            {"name": "Gross Margin", "value_actual": "74.0%", "growth_yoy": "+20.4 pts", "source_text": "Record margins due to mix shift."}
        ]
    },
    "MID_CAP_APPN": {
        "ticker": "APPN",
        "period": "Q3 2024",
        "kpis": [
            {"name": "Cloud Subscription Revenue", "value_actual": "$80.9M", "growth_yoy": "+20%", "source_text": "Cloud subscription revenue increased 20% year-over-year."},
            {"name": "Total Revenue", "value_actual": "$142.7M", "growth_yoy": "+16%", "source_text": "Total revenue was $142.7 million."},
            {"name": "Net Retention Rate", "value_actual": "115%", "growth_yoy": "-2%", "source_text": "Cloud subscription NRR was 115%."}
        ]
    },
    "SMALL_CAP_PATH": {
        "ticker": "PATH",
        "period": "Q3 FY2024",
        "kpis": [
            {"name": "ARR", "value_actual": "$1.378B", "growth_yoy": "+24%", "source_text": "Annualized Renewal Run-rate reached $1.378 billion."},
            {"name": "Net New ARR", "value_actual": "$70M", "growth_yoy": "N/A", "source_text": "Net new ARR of $70 million."},
            {"name": "Dollar Based Net Retention", "value_actual": "121%", "growth_yoy": "N/A", "source_text": "DBNR rate of 121 percent."}
        ]
    },
    "EDGE_CASE_MISSING_DATA": {
        "ticker": "UNKNOWN",
        "period": "Q?",
        "kpis": [
            {"name": "Revenue", "value_actual": "N/A", "growth_yoy": "N/A", "source_text": ""}, # Empty source
            {"name": "Very Long Comment", "value_actual": "$1B", "growth_yoy": "+5%", "source_text": "A" * 500} # Stress test comment limit
        ]
    }
}

def test_auto_modeler_generation():
    """
    Generates Excel models for all scenarios to verify no crashes.
    """
    output_dir = Path("stress_test_outputs")
    output_dir.mkdir(exist_ok=True)
    
    for scenario_name, data in MOCK_SCENARIOS.items():
        print(f"Testing Scenario: {scenario_name}...")
        try:
            modeler = AutoModeler(data['ticker'], data['period'])
            excel_bytes = modeler.generate(data['kpis'])
            
            filename = output_dir / f"{scenario_name}.xlsx"
            with open(filename, "wb") as f:
                f.write(excel_bytes.read())
            
            assert filename.exists()
            print(f"✅ Generated {filename} ({filename.stat().st_size} bytes)")
            
        except Exception as e:
            pytest.fail(f"Scenario {scenario_name} failed: {e}")

if __name__ == "__main__":
    test_auto_modeler_generation()
