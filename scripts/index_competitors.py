import sys
import os

# Patch for Python 3.9 importlib.metadata issues
if sys.version_info < (3, 10):
    try:
        import importlib_metadata
        import importlib.metadata
        importlib.metadata.packages_distributions = importlib_metadata.packages_distributions
    except ImportError:
        pass

sys.path.append(os.getcwd())

from core.synthesis.hybrid_rag import HybridRAGEngine
from core.extraction.engine import ExtractionEngine

def chunk_report(report, ticker: str, period: str) -> list:
    """
    Splits an EarningsReport into RAG-friendly chunks with metadata.
    """
    chunks = []
    
    # KPIs as individual chunks
    for kpi in report.kpis:
        chunks.append({
            "id": f"{ticker}_{period}_{kpi.name.lower().replace(' ', '_')}",
            "text": f"{kpi.name}: {kpi.value_actual}. {kpi.context}",
            "metadata": {"ticker": ticker, "fiscal_period": period, "topic": kpi.name}
        })
    
    # Guidance as individual chunks
    for g in report.guidance:
        chunks.append({
            "id": f"{ticker}_{period}_guidance_{g.metric.lower().replace(' ', '_')}",
            "text": f"Guidance for {g.metric}: Midpoint ~{g.midpoint} {g.unit}. {g.commentary}",
            "metadata": {"ticker": ticker, "fiscal_period": period, "topic": "Guidance"}
        })
    
    # Summary themes
    if report.summary.bull_case:
        chunks.append({
            "id": f"{ticker}_{period}_bull_case",
            "text": "Bull Case: " + " | ".join(report.summary.bull_case),
            "metadata": {"ticker": ticker, "fiscal_period": period, "topic": "Bull Case"}
        })
    if report.summary.bear_case:
        chunks.append({
            "id": f"{ticker}_{period}_bear_case",
            "text": "Bear Case: " + " | ".join(report.summary.bear_case),
            "metadata": {"ticker": ticker, "fiscal_period": period, "topic": "Bear Case"}
        })
    if report.summary.key_themes:
        chunks.append({
            "id": f"{ticker}_{period}_themes",
            "text": "Key Themes: " + ", ".join(report.summary.key_themes),
            "metadata": {"ticker": ticker, "fiscal_period": period, "topic": "Themes"}
        })
    
    return chunks

def main():
    if not os.environ.get("GOOGLE_API_KEY"):
        print("ERROR: GOOGLE_API_KEY not set.")
        sys.exit(1)

    extractor = ExtractionEngine()
    rag_engine = HybridRAGEngine(collection_name="competitor_reports")
    
    companies = [
        {"ticker": "NVDA", "period": "Q3 FY2024", "file": "data/raw/nvda_q3_2024_pr.txt"},
        {"ticker": "AMD", "period": "Q3 FY2024", "file": "data/raw/amd_q3_2024_pr.txt"},
        {"ticker": "INTC", "period": "Q3 FY2024", "file": "data/raw/intc_q3_2024_pr.txt"},
    ]

    all_chunks = []
    
    for company in companies:
        print(f"\n--- Extracting {company['ticker']} ---")
        with open(company["file"], "r") as f:
            text = f.read()
        
        report = extractor.extract_from_text(text, company["ticker"])
        chunks = chunk_report(report, company["ticker"], company["period"])
        all_chunks.extend(chunks)
        print(f"  Generated {len(chunks)} chunks for {company['ticker']}")

    print(f"\n--- Indexing {len(all_chunks)} total chunks into RAG ---")
    rag_engine.add_documents(all_chunks)
    print("Indexing complete!")

    # Test search
    print("\n--- Testing Cross-Company Search ---")
    query = "What are semiconductor companies saying about AI demand and growth?"
    results = rag_engine.search(query, top_k=5)
    
    print(f"Query: {query}\n")
    for i, res in enumerate(results):
        print(f"[{i+1}] {res['metadata']['ticker']} - {res['metadata']['topic']}")
        print(f"    {res['text'][:120]}...\n")

if __name__ == "__main__":
    main()
