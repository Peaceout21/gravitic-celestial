import os
from typing import List, Dict, Tuple
import chromadb
from chromadb.config import Settings
from rank_bm25 import BM25Okapi
import re

class HybridRAGEngine:
    """
    A Hybrid RAG engine combining:
    - ChromaDB for semantic (vector) search.
    - BM25 for keyword-based search.
    - Reciprocal Rank Fusion (RRF) to merge results.
    """
    def __init__(self, collection_name: str = "earnings_reports", persist_dir: str = "data/chroma_db"):
        self.persist_dir = persist_dir
        self.client = chromadb.PersistentClient(path=persist_dir)
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        self.bm25_corpus: List[List[str]] = []
        self.bm25_doc_ids: List[str] = []
        self.bm25: BM25Okapi | None = None

    def _tokenize(self, text: str) -> List[str]:
        """Simple tokenizer for BM25."""
        return re.findall(r'\w+', text.lower())

    def add_documents(self, documents: List[Dict]):
        """
        Adds documents to both ChromaDB and the BM25 index.
        Each document should have: 'id', 'text', 'metadata' (dict with ticker, period, etc.)
        """
        ids = [doc['id'] for doc in documents]
        texts = [doc['text'] for doc in documents]
        metadatas = [doc['metadata'] for doc in documents]

        # Add to ChromaDB (it handles embedding automatically)
        self.collection.add(
            ids=ids,
            documents=texts,
            metadatas=metadatas
        )

        # Add to BM25 corpus
        tokenized_texts = [self._tokenize(t) for t in texts]
        self.bm25_corpus.extend(tokenized_texts)
        self.bm25_doc_ids.extend(ids)
        self.bm25 = BM25Okapi(self.bm25_corpus)

    def _reciprocal_rank_fusion(self, results_list: List[List[Tuple[str, float]]], k: int = 60) -> List[Tuple[str, float]]:
        """
        Fuses multiple ranked lists using RRF.
        Each result is a list of (doc_id, score) tuples.
        """
        fused_scores: Dict[str, float] = {}
        for results in results_list:
            for rank, (doc_id, _) in enumerate(results):
                if doc_id not in fused_scores:
                    fused_scores[doc_id] = 0.0
                fused_scores[doc_id] += 1.0 / (k + rank + 1)
        
        sorted_results = sorted(fused_scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_results

    def search(self, query: str, top_k: int = 10) -> List[Dict]:
        """
        Performs hybrid search: BM25 + Semantic, then fuses with RRF.
        """
        # Semantic search via ChromaDB
        chroma_results = self.collection.query(
            query_texts=[query],
            n_results=top_k * 2  # Fetch more for fusion
        )
        semantic_ranked = list(zip(chroma_results['ids'][0], chroma_results['distances'][0]))

        # BM25 search
        if self.bm25 is None:
            bm25_ranked = []
        else:
            tokenized_query = self._tokenize(query)
            bm25_scores = self.bm25.get_scores(tokenized_query)
            bm25_ranked_indices = sorted(range(len(bm25_scores)), key=lambda i: bm25_scores[i], reverse=True)[:top_k * 2]
            bm25_ranked = [(self.bm25_doc_ids[i], bm25_scores[i]) for i in bm25_ranked_indices]

        # Reciprocal Rank Fusion
        fused = self._reciprocal_rank_fusion([semantic_ranked, bm25_ranked])

        # Retrieve final documents
        final_ids = [doc_id for doc_id, _ in fused[:top_k]]
        final_docs = self.collection.get(ids=final_ids, include=["documents", "metadatas"])
        
        results = []
        for i, doc_id in enumerate(final_docs['ids']):
            results.append({
                "id": doc_id,
                "text": final_docs['documents'][i],
                "metadata": final_docs['metadatas'][i]
            })
        return results

if __name__ == "__main__":
    print("Hybrid RAG Engine (ChromaDB + BM25 + RRF) initialized.")
