import sys
import os
sys.path.append(os.getcwd())

from core.analysis.sandbagging import SandbaggingAnalyzer
from core.analysis.contagion import ContagionGraph

def verify_sandbagging():
    print("\n--- Verifying Sandbagging Analyzer ---")
    analyzer = SandbaggingAnalyzer()
    stats = analyzer.calculate_coefficient("NVDA")
    print(f"Coefficient: {stats['coefficient']:.4f}")
    print(f"Narrative: {stats['narrative']}")
    
    pred = analyzer.predict_actual(24.0, "NVDA")
    print(f"Prediction for $24.0B Guidance: ${pred}B")
    
    if stats['coefficient'] > 0:
        print("✅ Sandbagging logic working.")
    else:
        print("❌ Sandbagging logic failed (or coefficient is 0).")

def verify_contagion():
    print("\n--- Verifying Contagion Graph ---")
    cg = ContagionGraph()
    suppliers = cg.get_suppliers("NVDA")
    print(f"Direct Suppliers: {suppliers}")
    
    status, flags = cg.analyze_risk("NVDA")
    print(f"Overall Risk: {status}")
    print(f"Flags: {flags}")
    
    graph = cg.generate_graph("NVDA")
    print(f"Graph generated: {type(graph)}")
    
    if "SK Hynix" in str(flags):
        print("✅ Risk detection working.")
    else:
        print("❌ Risk detection failed.")

if __name__ == "__main__":
    verify_sandbagging()
    verify_contagion()
