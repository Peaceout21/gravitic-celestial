import sys
import os
sys.path.append(os.getcwd())

from core.synthesis.hybrid_rag import HybridRAGEngine

def seed_demo_data(engine: HybridRAGEngine):
    """Seeds the RAG engine with sample earnings report chunks."""
    documents = [
        {
            "id": "nvda_q3_2024_revenue",
            "text": "NVIDIA reported record revenue of $18.12 billion for Q3 FY2024, up 206% year-over-year. Data Center revenue hit an all-time high of $14.51 billion, driven by strong demand for AI infrastructure.",
            "metadata": {"ticker": "NVDA", "fiscal_period": "Q3 FY2024", "topic": "Revenue"}
        },
        {
            "id": "nvda_q3_2024_ai_outlook",
            "text": "Jensen Huang stated that NVIDIA is at the center of the generative AI revolution. Demand for H100 GPUs continues to outpace supply, with customers accelerating their AI workload deployments.",
            "metadata": {"ticker": "NVDA", "fiscal_period": "Q3 FY2024", "topic": "AI Outlook"}
        },
        {
            "id": "amd_q3_2024_datacenter",
            "text": "AMD's Data Center segment revenue was $1.6 billion, up 21% year-over-year. The MI300 AI accelerator is ramping production, with AMD targeting $2 billion in AI GPU revenue for 2024.",
            "metadata": {"ticker": "AMD", "fiscal_period": "Q3 FY2024", "topic": "Data Center"}
        },
        {
            "id": "intc_q3_2024_challenges",
            "text": "Intel reported Data Center revenue of $3.8 billion, down 10% YoY. Management acknowledged challenges in the AI accelerator market and is focusing on cost reductions and process technology improvements.",
            "metadata": {"ticker": "INTC", "fiscal_period": "Q3 FY2024", "topic": "Challenges"}
        }
    ]
    engine.add_documents(documents)
    print(f"Seeded {len(documents)} documents into the RAG engine.")

def run_demo():
    engine = HybridRAGEngine()
    seed_demo_data(engine)

    # Example Hybrid Search
    query = "What are semiconductor companies saying about AI demand?"
    print(f"\n--- Query: {query} ---")
    results = engine.search(query, top_k=3)

    for i, res in enumerate(results):
        print(f"\n[{i+1}] {res['metadata']['ticker']} - {res['metadata']['topic']}")
        print(f"    {res['text'][:150]}...")

if __name__ == "__main__":
    run_demo()
