from typing import List, Optional, Dict
from pydantic import BaseModel, Field

class KPI(BaseModel):
    name: str = Field(..., description="Name of the Key Performance Indicator (e.g., Revenue, GAAP EPS, Gross Margin)")
    value_actual: str = Field(..., description="The reported actual value")
    value_consensus: Optional[str] = Field(None, description="The analyst consensus value, if mentioned")
    period: str = Field(..., description="The fiscal period (e.g., Q4 2023)")
    is_beat: Optional[bool] = Field(None, description="True if the actual value exceeded consensus")
    context: Optional[str] = Field("", description="A brief snippet from the source text justifying this KPI")

class Guidance(BaseModel):
    metric: str = Field(..., description="The metric being guided (e.g., FY2024 Revenue)")
    low: Optional[float] = Field(None)
    high: Optional[float] = Field(None)
    midpoint: Optional[float] = Field(None)
    unit: str = Field(..., description="Unit of measurement (e.g., Billions, %)")
    commentary: Optional[str] = Field("", description="Management's narrative regarding this guidance")

class ExecutiveSummary(BaseModel):
    bull_case: List[str] = Field(..., description="3-5 bullet points of positive highlights")
    bear_case: List[str] = Field(..., description="3-5 bullet points of negative/risk highlights")
    key_themes: List[str] = Field(..., description="Broad themes mentioned (e.g., 'Inventory Normalization', 'AI Tailwinds')")

class EarningsReport(BaseModel):
    ticker: str
    company_name: str
    fiscal_period: str
    kpis: List[KPI]
    guidance: List[Guidance]
    summary: ExecutiveSummary
    source_urls: List[str]
