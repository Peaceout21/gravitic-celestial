import sys
import os

# Add paths logic same as app.py/debug script
current_dir = os.path.dirname(os.path.abspath(__file__))
# gravitic-celestial/scripts -> gravitic-celestial
celestial_root = os.path.join(current_dir, '..')
# gravitic-celestial -> gravitic-macro
macro_root = os.path.join(celestial_root, '..', 'gravitic-macro')

sys.path.append(os.path.abspath(celestial_root))
sys.path.append(os.path.abspath(macro_root))

print(f"✅ Added to sys.path: {os.path.abspath(macro_root)}")
print(f"📂 Resolved macro_core path: {os.path.join(os.path.abspath(macro_root), 'macro_core')}")

try:
    import macro_core
    print("✅ Successfully imported macro_core package")
except ImportError as e:
    print(f"❌ Failed to import macro_core: {e}")

try:
    from macro_core.scrapers.polymarket import PolymarketScraper
    print("✅ Successfully imported PolymarketScraper")
except ImportError as e:
    print(f"❌ Failed to import PolymarketScraper: {e}")

try:
    from core.fusion.macro_bridge import MacroBridge
    bridge = MacroBridge()
    print("✅ Successfully initialized MacroBridge")
    bridge.run_live_scan("NVDA")
except Exception as e:
    print(f"❌ Bridge Failure: {e}")
