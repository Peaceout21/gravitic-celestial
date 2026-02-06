# Test script for Hybrid RAG search
from core.synthesis.hybrid_rag import HybridRAGEngine

engine = HybridRAGEngine()
results = engine.search("Microsoft cloud revenue", top_k=5)

for i, r in enumerate(results):
    ticker = r["metadata"]["ticker"]
    topic = r["metadata"]["topic"]
    text_preview = r["text"][:120].replace("\n", " ")
    print(f"{i+1}. {ticker} - {topic}: {text_preview}...")
