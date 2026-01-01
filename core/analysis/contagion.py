import graphviz
from typing import List, Dict, Tuple

class ContagionGraph:
    def __init__(self):
        # Mock knowledge base of suppliers
        # In a real app, this would be populated by the RAG engine extracting "Customer of" / "Supplier to" relationships
        self.supply_chain = {
            "NVDA": {
                "suppliers": ["TSM (TSMC)", "MU (Micron)", "SK Hynix", "Quanta Computer"],
                "risk_level": "Medium",
                "active_risks": []
            },
            "TSM (TSMC)": {
                "suppliers": ["ASML", "Applied Materials"],
                "risk_level": "Low",
                "active_risks": []
            },
            "SK Hynix": {
                "suppliers": [],
                "risk_level": "High",
                "active_risks": ["Delay in HBM3e production yield reported 2 days ago."]
            },
            "ASML": {
                "suppliers": [],
                "risk_level": "Low",
                "active_risks": []
            }
        }

    def get_suppliers(self, ticker: str) -> List[str]:
        return self.supply_chain.get(ticker, {}).get("suppliers", [])

    def analyze_risk(self, ticker: str) -> Tuple[str, List[str]]:
        """
        Recursive analysis of supplier risks.
        Returns: (Overall Risk Status, List of specific risk flags)
        """
        flags = []
        direct_suppliers = self.get_suppliers(ticker)
        
        # Check direct suppliers
        for supplier in direct_suppliers:
            supp_data = self.supply_chain.get(supplier, {})
            if supp_data.get("risk_level") == "High":
                flags.append(f"CRITICAL: Supplier {supplier} has High Risk status.")
            
            for risk in supp_data.get("active_risks", []):
                flags.append(f"RISK from {supplier}: {risk}")
                
            # Check secondary suppliers (simplified 1-level deep for MVP)
            for sub_supplier in supp_data.get("suppliers", []):
                sub_data = self.supply_chain.get(sub_supplier, {})
                if sub_data.get("risk_level") == "High":
                    flags.append(f"INDIRECT RISK: Sub-supplier {sub_supplier} (to {supplier}) is High Risk.")

        status = "High" if flags else "Low"
        return status, flags

    def generate_graph(self, root_ticker: str = "NVDA") -> graphviz.Digraph:
        """Generates a Graphviz directional graph for the supply chain."""
        dot = graphviz.Digraph(comment=f'{root_ticker} Supply Chain')
        dot.attr(rankdir='LR')
        
        # Add Root
        dot.node(root_ticker, root_ticker, shape='box', style='filled', fillcolor='#AED9E0')
        
        # Add Suppliers
        direct_suppliers = self.get_suppliers(root_ticker)
        for supp in direct_suppliers:
            risk = "High" if self.supply_chain.get(supp, {}).get("risk_level") == "High" else "Low"
            color = "#FFB7B2" if risk == "High" else "#E2F0CB"
            
            dot.node(supp, supp, style='filled', fillcolor=color)
            dot.edge(supp, root_ticker, label="Supplies")
            
            # Add Sub-suppliers
            sub_suppliers = self.get_suppliers(supp)
            for sub in sub_suppliers:
                dot.node(sub, sub, style='filled', fillcolor="#E2F0CB")
                dot.edge(sub, supp)
                
        return dot
