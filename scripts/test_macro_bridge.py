import sys
import os
import logging

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core.fusion.macro_bridge import MacroBridge

logging.basicConfig(level=logging.INFO)

def main():
    print("🌉 Testing MacroBridge Connectivity...")
    
    bridge = MacroBridge()
    print(f"Target DB: {bridge.db_path}")
    
    # Test 1: NVDA Signals
    print("\n[Test 1] Fetching NVDA Signals...")
    nvda_df = bridge.get_ticker_signals("NVDA")
    if nvda_df.empty:
        print("❌ No signals found for NVDA.")
    else:
        print(f"✅ Found {len(nvda_df)} signals.")
        print(nvda_df[['event_title', 'probability_yes']].to_string(index=False))
        
    # Test 2: Macro Signals
    print("\n[Test 2] Fetching Macro Risk Signals...")
    macro_df = bridge.get_macro_risk_signals()
    if macro_df.empty:
        print("❌ No macro signals found.")
    else:
        print(f"✅ Found {len(macro_df)} signals.")
        print(macro_df[['event_title', 'probability_yes']].to_string(index=False))

if __name__ == "__main__":
    main()
