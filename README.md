# 15-Minute Financial Analyst - MVP

An AI-powered earnings analysis tool that generates structured reports within 15 minutes of a press release.

## Quick Start

### 1. Set up Environment
```bash
cd /Users/arjun/.gemini/antigravity/playground/gravitic-celestial
python3 -m venv celestial-venv
source celestial-venv/bin/activate
pip install .
```
To run in local

cd /Users/arjun/.gemini/antigravity/scratch/gravitic-macro && source macro-venv/bin/activate && export PYTHONPATH=$PYTHONPATH:. && python3 scripts/test_improvements.py


### 2. Set API Key
```bash
export GOOGLE_API_KEY="your-google-api-key"
```

### 3. Run the Dashboard
```bash
streamlit run ui/app.py
```

## Project Structure
```
gravitic-celestial/
├── core/
│   ├── models.py                 # Pydantic schemas
│   ├── monitor.py                # SEC polling loop
│   ├── extraction/               # LLM extraction engines
│   ├── ingestion/                # Data sources (SEC, RSS)
│   └── synthesis/                # Delta logic & Hybrid RAG
├── data/                         # Raw & processed data
├── scripts/                      # Demo scripts
└── ui/app.py                     # Streamlit dashboard
```

## Features
- **Live SEC Monitoring**: RSS-based polling for new 8-K filings.
- **Gemini 2.0 Extraction**: Multi-modal analysis of text and slidedecks.
- **Hybrid RAG**: BM25 + Semantic search for competitor analysis.
- **Streamlit Dashboard**: Visual interface for reports.

## License
Open Source (MIT)
