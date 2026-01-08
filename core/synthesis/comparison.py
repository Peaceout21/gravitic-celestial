from typing import List, Dict
from core.models import EarningsReport, KPI

class ComparisonEngine:
    """
    Handles the delta logic between current earnings and historical data.
    """
    @staticmethod
    def calculate_growth(current: str, previous: str) -> str:
        """
        Helper to calculate percentage change between two formatted strings.
        (Very basic implementation for V0)
        """
        try:
            cur = float(current.replace('$', '').replace('B', '').replace('M', '').replace(',', ''))
            prev = float(previous.replace('$', '').replace('B', '').replace('M', '').replace(',', ''))
            delta = ((cur - prev) / prev) * 100
            return f"{delta:+.2f}%"
        except Exception:
            return "N/A"

    def compare_to_prior(self, current: EarningsReport, prior: EarningsReport) -> Dict:
        """
        Compares current KPIs to prior quarter/year KPIs.
        """
        deltas = []
        prior_kpi_map = {k.name: k.value_actual for k in prior.kpis}
        
        for kpi in current.kpis:
            if kpi.name in prior_kpi_map:
                growth = self.calculate_growth(kpi.value_actual, prior_kpi_map[kpi.name])
                deltas.append({
                    "metric": kpi.name,
                    "current": kpi.value_actual,
                    "prior": prior_kpi_map[kpi.name],
                    "growth": growth
                })
        
        return deltas

    def get_alternative_alpha(self, ticker: str) -> str:
        """
        SOTA: Pulls fused operational alpha from the Nebula Signal Store.
        """
        from core.fusion.nebula_bridge import NebulaBridge
        bridge = NebulaBridge()
        return bridge.get_alpha_context(ticker)

if __name__ == "__main__":
    print("Comparison Engine Initialized.")
