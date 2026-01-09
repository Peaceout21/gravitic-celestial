import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Add paths
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'gravitic-macro'))

try:
    from core.fusion.macro_bridge import MacroBridge
    bridge = MacroBridge()
    
    print("--- Testing Live Scan for NVDA ---")
    signals = bridge.run_live_scan("NVDA")
    
    if not signals:
        print("❌ No signals found for NVDA")
    else:
        print(f"✅ Found {len(signals)} signals")
        for s in signals:
            print(f"- {s['title']} ({s['probability_yes']:.1%})")

except Exception as e:
    print(f"💥 CRASH: {e}")
    import traceback
    traceback.print_exc()
