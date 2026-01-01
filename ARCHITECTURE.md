# 🏗️ Solution Architecture: Gravitic Financial Analyst Platform

This document provides a deep technical specification of the entire system, designed to enable another engineer (or LLM) to build new features independently.

---

## 1. System Overview

The platform consists of **two main service clusters**:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          GRAVITIC PLATFORM                              │
├─────────────────────────────────┬───────────────────────────────────────┤
│   gravitic-celestial (V1)       │   gravitic-nebula (V2 - Planned)      │
│   "The Analyst Brain"           │   "The Signal Scraper"                │
│   ────────────────────────      │   ────────────────────────            │
│   • SEC/NSE Ingestion           │   • Bill of Lading Scraper            │
│   • Gemini Extraction           │   • Job Board Monitor                 │
│   • Report Generation           │   • App Store Tracker                 │
│   • Notifications               │   • Digital Footprint Aggregator      │
└─────────────────────────────────┴───────────────────────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │   PostgreSQL    │  (Shared State)
                    │   + TimescaleDB │
                    └─────────────────┘
```

---

## 2. gravitic-celestial: Current Architecture (V1)

### 2.1 Directory Structure
```
gravitic-celestial/
├── core/
│   ├── ingestion/              # Data Fetching
│   │   ├── edgar_client.py     # SEC 8-K Fetcher (edgartools)
│   │   ├── polling_engine.py   # Orchestration Loop
│   │   ├── state_manager.py    # SQLite Deduplication
│   │   └── international/
│   │       ├── base_client.py      # ABC Interface
│   │       ├── nse_client.py       # India NSE (RSS + PDF)
│   │       ├── market_registry.py  # Ticker -> Client Router
│   │       └── pdf_processor.py    # PDF Downloader
│   ├── extraction/             # LLM Processing
│   │   ├── engine.py           # Gemini 2.0 Flash (Structured)
│   │   ├── multimodal_engine.py# Gemini 3 Flash (Vision/Audio)
│   │   └── robust_engine.py    # Model Fallback Logic
│   ├── synthesis/              # Analysis
│   │   ├── comparison.py       # QoQ/YoY Delta Calc
│   │   └── hybrid_rag.py       # ChromaDB + BM25
│   ├── analysis/               # Advanced Insights
│   │   ├── sandbagging.py      # Guidance Beat Predictor
│   │   └── contagion.py        # Supply Chain Graph
│   ├── notifications/
│   │   └── client.py           # Slack/Discord Webhook
│   └── models.py               # Pydantic Schemas
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── data/                       # Gitignored
│   ├── celestial.db            # SQLite State
│   ├── chroma_db/              # Vector Store
│   └── reports/                # Generated .md files
└── web/
    └── dashboard.py            # Streamlit UI
```

### 2.2 Data Flow (Current)

```
[SEC RSS / NSE RSS]
        │
        ▼
┌───────────────────────┐
│   PollingEngine       │  (docker: celestial-poller)
│   - Tickers: List     │
│   - StateManager      │
└───────────┬───────────┘
            │ get_latest_filings()
            ▼
┌───────────────────────┐      ┌───────────────────────┐
│   EdgarClient         │  OR  │   NseClient           │
│   (edgartools)        │      │   (feedparser + PDF)  │
└───────────┬───────────┘      └───────────┬───────────┘
            │ get_filing_text()             │
            ▼                               ▼
┌───────────────────────────────────────────────────────┐
│   ExtractionEngine (Gemini 2.0 Flash)                 │
│   - Input: Raw Text / Markdown                        │
│   - Output: EarningsReport (Pydantic)                 │
└───────────────────────────────────────────────────────┘
            │
            ▼
┌───────────────────────┐      ┌───────────────────────┐
│   _save_report()      │ ───► │   NotificationClient  │
│   data/reports/*.md   │      │   (Slack/Discord)     │
└───────────────────────┘      └───────────────────────┘
```

### 2.3 Key Interfaces (Contracts)

#### `BaseIngestionClient` (ABC)
```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class BaseIngestionClient(ABC):
    @abstractmethod
    def get_latest_filings(self, tickers: List[str], limit: int = 5) -> List[Dict]:
        """
        Returns list of:
        {
            "ticker": str,
            "accession_number": str,  # Unique ID
            "filing_date": str,
            "form": str,              # e.g., "8-K", "Corporate Announcement"
            "url": str,
            "filing_obj": Any         # Raw object for text extraction
        }
        """
        pass

    @abstractmethod
    def get_filing_text(self, filing_obj: Any) -> Optional[str]:
        """Extracts full text/markdown from the filing object."""
        pass
```

#### `EarningsReport` (Pydantic Schema)
```python
class KPI(BaseModel):
    name: str
    value_actual: str
    value_consensus: Optional[str] = None
    period: str
    is_beat: Optional[bool] = None
    context: str

class Guidance(BaseModel):
    metric: str
    midpoint: float
    unit: str
    commentary: str

class Summary(BaseModel):
    bull_case: List[str] = []
    bear_case: List[str] = []
    key_themes: List[str] = []

class EarningsReport(BaseModel):
    ticker: str
    company_name: str
    fiscal_period: str
    kpis: List[KPI] = []
    guidance: List[Guidance] = []
    summary: Optional[Summary] = None
    source_urls: List[str] = []
```

### 2.4 Database Schema (SQLite - Current)

```sql
-- data/celestial.db
CREATE TABLE processed_filings (
    accession_number TEXT PRIMARY KEY,
    ticker TEXT,
    filing_date TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 3. gravitic-nebula: Planned Architecture (V2)

### 3.1 Purpose
A **separate microservice** for high-volume, potentially blocking scraping tasks. Decoupled to prevent instability in the core analyst.

### 3.2 Proposed Directory Structure
```
gravitic-nebula/
├── scrapers/
│   ├── shipping/
│   │   └── importgenius_scraper.py   # Bill of Lading
│   ├── hiring/
│   │   └── career_page_monitor.py    # Firecrawl-based
│   └── digital/
│       └── app_store_tracker.py      # App Annie / SensorTower
├── transformers/
│   └── signal_classifier.py          # Gemini 3 for classification
├── exporters/
│   └── signal_publisher.py           # Push to celestial or Postgres
├── config/
│   └── targets.yaml                  # Tickers + URLs to monitor
└── docker-compose.yml
```

### 3.3 Data Flow (Planned)

```
┌─────────────────────────┐
│   gravitic-nebula       │
│   (Scraper Cluster)     │
├─────────────────────────┤
│   Shipping Scraper      │──┐
│   Hiring Scraper        │──┼──► Signal Queue (Redis/Kafka)
│   App Tracker           │──┘
└─────────────────────────┘
            │
            │ (Signals: JSON)
            ▼
┌─────────────────────────────────────────────────────────┐
│                     PostgreSQL + TimescaleDB            │
│   ─────────────────────────────────────────────────     │
│   signals (time-series)                                 │
│   - signal_id, ticker, signal_type, value, timestamp    │
│   - e.g., ("RELIANCE.NS", "HIRING_SPIKE", 45, ...)      │
└─────────────────────────────────────────────────────────┘
            │
            ▼ (Query on Report Generation)
┌─────────────────────────┐
│   gravitic-celestial    │
│   ExtractionEngine      │
│   - Enriches report     │
│     with Alt Data       │
└─────────────────────────┘
```

### 3.4 Signal Schema (Proposed)

```sql
-- PostgreSQL + TimescaleDB
CREATE TABLE signals (
    signal_id SERIAL,
    ticker VARCHAR(20) NOT NULL,
    signal_type VARCHAR(50) NOT NULL,  -- HIRING_SPIKE, SHIPPING_VOLUME, APP_RANK
    value NUMERIC,
    metadata JSONB,                     -- Raw context
    source_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
SELECT create_hypertable('signals', 'created_at');
```

### 3.5 Key Component Specs

#### Shipping Scraper (Crawlee)
- **Target**: ImportGenius, Panjiva (public search)
- **Input**: List of company names from `targets.yaml`
- **Output**: `{ ticker, container_count, destination, date }`
- **Schedule**: Daily (Cron)

#### Hiring Scraper (Firecrawl)
- **Target**: Company career pages (e.g., `careers.company.com`)
- **Input**: Direct URLs from `targets.yaml`
- **Output**: `{ ticker, job_count, top_roles: List[str] }`
- **Schedule**: Weekly

#### App Tracker (API)
- **Target**: SensorTower / App Annie API (or scrape App Store directly)
- **Input**: App Bundle IDs
- **Output**: `{ ticker, app_rank, downloads_estimate }`
- **Schedule**: Daily

---

## 4. Audio Intelligence (Planned Feature)

### 4.1 Flow

```
[Earnings Call .mp3 URL]
        │
        ▼
┌───────────────────────────────────────────────────────┐
│   AudioIngestClient                                   │
│   - Downloads .mp3 from IR page                       │
│   - Stores temporarily                                │
└───────────────┬───────────────────────────────────────┘
                │
                ▼
┌───────────────────────────────────────────────────────┐
│   Gemini 3 Flash (Native Audio)                       │
│   - Prompt: "Analyze tone shifts, identify evasion"   │
│   - Output: AudioAnalysis (Pydantic)                  │
└───────────────────────────────────────────────────────┘
```

### 4.2 AudioAnalysis Schema (Proposed)

```python
class ToneShift(BaseModel):
    speaker: str              # "CEO", "CFO", "Analyst (Morgan Stanley)"
    timestamp: str            # "00:15:32"
    sentiment_before: str     # "Confident"
    sentiment_after: str      # "Hesitant"
    topic: str                # "Gross Margin Guidance"

class AudioAnalysis(BaseModel):
    call_duration_minutes: int
    key_topics: List[str]
    tone_shifts: List[ToneShift]
    evasive_answers: List[str]  # Direct quotes
    overall_sentiment_score: float  # -1 to 1
```

---

## 5. Deployment Architecture (Target State)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Cloud Provider (AWS/GCP)                      │
├─────────────────────────────┬───────────────────────────────────────────┤
│   Container Orchestration   │   Managed Services                        │
│   (Docker Compose / K8s)    │                                           │
│   ──────────────────────    │   ──────────────────────                  │
│   celestial-poller          │   PostgreSQL (RDS/CloudSQL)               │
│   celestial-dashboard       │   TimescaleDB (Timescale Cloud)           │
│   nebula-scrapers (x3)      │   Redis / Pub-Sub (Signal Queue)          │
│                             │   ChromaDB (Self-hosted or Pinecone)      │
└─────────────────────────────┴───────────────────────────────────────────┘
```

---

## 6. API / Message Contracts

### 6.1 Internal Signal (nebula -> celestial)

```json
{
  "signal_type": "HIRING_SPIKE",
  "ticker": "LTIM.NS",
  "timestamp": "2026-01-01T10:00:00Z",
  "value": 150,
  "metadata": {
    "top_roles": ["GenAI Engineer", "Cloud Architect"],
    "source_url": "https://careers.ltimindtree.com"
  }
}
```

### 6.2 Notification Webhook Payload

```json
{
  "text": "🚨 **New Earnings Note Generated!**\n**Rocket Lab (RKLB)** - Q3 2025\n📄 `data/reports/RKLB_xxx.md`"
}
```

---

## 7. Development Roadmap Summary

| Phase | Scope | Repo | Key Tech |
|-------|-------|------|----------|
| V1 (Done) | SEC/NSE Ingestion, Gemini Extraction, Notifications | `gravitic-celestial` | Edgartools, Feedparser, Gemini 2.0/3 |
| V2 | Alt Data Scrapers (Shipping, Hiring, App) | `gravitic-nebula` | Crawlee, Firecrawl, TimescaleDB |
| V3 | Audio Intelligence | `gravitic-celestial` | Gemini 3 Flash (Native Audio) |
| V4 | Forensic Mode (10-K Footnote RAG) | `gravitic-celestial` | Enhanced RAG, Neo4j (optional) |

---

*This document is the single source of truth for any new LLM or engineer building on this platform.*
