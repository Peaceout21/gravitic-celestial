import json
import pandas as pd
from typing import Dict, List, Optional

class SandbaggingAnalyzer:
    def __init__(self, data_path: str = "data/processed/nvda_historical_guidance.json"):
        self.data_path = data_path
        self.data = self._load_data()

    def _load_data(self) -> List[Dict]:
        """Loads historical guidance vs actuals data."""
        try:
            with open(self.data_path, 'r') as f:
                return json.load(f)
        except getattr(FileNotFoundError, 'error_code', FileNotFoundError):
             # Fallback for demo if file doesn't exist
             return []

    def calculate_coefficient(self, ticker: str = "NVDA") -> Dict:
        """
        Calculates the average 'beat' percentage (Sandbagging Coefficient).
        
        Returns:
            Dict containing:
            - coefficient: float (e.g., 0.045 for 4.5% beat)
            - consistency: float (0.0 to 1.0, how often they beat)
            - narrative: str (LLM-ready string explaining the trend)
            - history: DataFrame (for plotting)
        """
        company_data = [d for d in self.data if d['ticker'] == ticker]
        
        if not company_data:
            return {"coefficient": 0.0, "consistency": 0.0, "narrative": "No historical data found.", "history": pd.DataFrame()}

        beats = []
        rows = []
        
        for record in company_data:
            guidance = record['guidance']['midpoint']
            actual = record['actual']['value']
            
            # Calculate percent beat
            beat_pct = (actual - guidance) / guidance
            beats.append(beat_pct)
            
            rows.append({
                "Quarter": record['fiscal_period'],
                "Guidance": guidance,
                "Actual": actual,
                "Beat %": round(beat_pct * 100, 2)
            })

        df = pd.DataFrame(rows)
        avg_beat = sum(beats) / len(beats)
        positive_beats = sum(1 for b in beats if b > 0)
        consistency = positive_beats / len(beats)
        
        narrative = f"Usually guides conservatively. On average, {ticker} beats their own revenue guidance by {avg_beat*100:.1f}%. Beaten guidance in {positive_beats}/{len(beats)} recent quarters."

        return {
            "coefficient": avg_beat,
            "consistency": consistency,
            "narrative": narrative,
            "history": df
        }

    def predict_actual(self, current_guidance_midpoint: float, ticker: str = "NVDA") -> float:
        """
        Predicts the likely actual number based on the sandbagging coefficient.
        """
        stats = self.calculate_coefficient(ticker)
        coefficient = stats['coefficient']
        
        # Adjust guidance by the sandbagging factor
        predicted_actual = current_guidance_midpoint * (1 + coefficient)
        return round(predicted_actual, 2)
