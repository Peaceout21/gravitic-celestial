import sys
import os
sys.path.append(os.getcwd())

from core.models import EarningsReport, KPI, ExecutiveSummary, Guidance
from core.synthesis.comparison import ComparisonEngine

# Mock Prior Quarter Data (Q2 FY2024)
prior_report = EarningsReport(
    ticker="NVDA",
    company_name="NVIDIA",
    fiscal_period="Q2 FY2024",
    kpis=[
        KPI(name="Revenue", value_actual="$13.51B", period="Q2 2024", context="Q2 Results"),
        KPI(name="Data Center Revenue", value_actual="$10.32B", period="Q2 2024", context="Q2 Results")
    ],
    guidance=[],
    summary=ExecutiveSummary(bull_case=[], bear_case=[], key_themes=[]),
    source_urls=[]
)

# Mock Current Quarter Data (Q3 FY2024)
current_report = EarningsReport(
    ticker="NVDA",
    company_name="NVIDIA",
    fiscal_period="Q3 FY2024",
    kpis=[
        KPI(name="Revenue", value_actual="$18.12B", period="Q3 2024", context="Q3 Results"),
        KPI(name="Data Center Revenue", value_actual="$14.51B", period="Q3 2024", context="Q3 Results")
    ],
    guidance=[
        Guidance(metric="Revenue", midpoint=20.0, unit="B", commentary="Expected strong demand")
    ],
    summary=ExecutiveSummary(
        bull_case=["Record Data Center performance", "Massive AI tailwinds"],
        bear_case=["Short-term supply constraints noted in call"],
        key_themes=["Generative AI", "Accelerated Computing"]
    ),
    source_urls=["https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-third-quarter-fiscal-2024"]
)

def run_simulation():
    engine = ComparisonEngine()
    deltas = engine.compare_to_prior(current_report, prior_report)
    
    print(f"--- {current_report.company_name} ({current_report.ticker}) Earnings Performance ---")
    print(f"Period: {current_report.fiscal_period} vs {prior_report.fiscal_period}")
    print("-" * 50)
    for d in deltas:
        print(f"{d['metric']}: {d['current']} (Prev: {d['prior']}) | Growth: {d['growth']}")
    
    print("\n--- Executive Summary ---")
    print("Bull Case:")
    for b in current_report.summary.bull_case:
        print(f" - {b}")
    
    print("\nNext Quarter Guidance:")
    for g in current_report.guidance:
        print(f" - {g.metric}: Target ~${g.midpoint}{g.unit}")

if __name__ == "__main__":
    run_simulation()
