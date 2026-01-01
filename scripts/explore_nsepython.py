try:
    from nsepython import nse_quote
    import json

    def check_ticker(ticker):
        print(f"--- Checking {ticker} ---")
        try:
            # remove .NS suffix
            sym = ticker.replace(".NS", "")
            data = nse_quote(sym)
            
            # Check for corporate announcements in the payload
            # The structure varies, often keys are 'corporate_announcements' or inside 'info'
            # Let's verify what keys we get
            # print(data.keys()) 
            
            # Usually corporate announcements are not in quote, but in a separate call
            # But let's see if nsepython has a dedicated corporate_announcement function
            # Or if it's part of quote.
            
            # For now, let's just print the keys to explore
            print(f"Keys found: {list(data.keys())}")
            
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

    check_ticker("TATAELXSI")
    
except ImportError:
    print("nsepython not installed")
