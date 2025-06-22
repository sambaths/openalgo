#!/usr/bin/env python3
"""
NSE Index Constituents Fetcher
=============================

⚠️  IMPORTANT LEGAL DISCLAIMER ⚠️

This software is provided for EDUCATIONAL and RESEARCH purposes ONLY.
By using this script, you acknowledge and agree to the following terms:

1. EDUCATIONAL PURPOSE: This script is designed solely for learning about data retrieval,
   web scraping techniques, and financial market data structure analysis.

2. NO FINANCIAL ADVICE: This script does NOT provide financial advice, investment 
   recommendations, or trading signals. Any data retrieved should NOT be used for 
   actual trading or investment decisions.

3. USE AT YOUR OWN RISK: Users assume ALL responsibility and risk for the use of this
   software. The authors disclaim ALL warranties and shall NOT be liable for any 
   damages, losses, or legal consequences arising from its use.

4. COMPLIANCE RESPONSIBILITY: Users are solely responsible for ensuring compliance 
   with all applicable laws, regulations, and terms of service of data providers.
   This includes but is not limited to NSE's terms of use and API usage policies.

5. NO COMMERCIAL USE: This script is NOT intended for commercial use, automated 
   trading systems, or any production trading environment.

6. DATA ACCURACY: The authors make NO guarantees about the accuracy, completeness,
   or timeliness of any data retrieved using this script.

7. LEGAL COMPLIANCE: Users must respect website terms of service, rate limiting,
   and applicable securities regulations in their jurisdiction.

8. INDEMNIFICATION: Users agree to indemnify and hold harmless the authors from
   any claims, damages, or legal actions arising from the use of this software.

By proceeding to use this script, you acknowledge that you have read, understood,
and agree to be bound by these terms. If you do not agree, DO NOT USE this software.

For educational purposes only - Use at your own discretion and risk.
"""

import requests, os
import urllib.parse
import pandas as pd
from tqdm import tqdm
import time
import random
import json

def print_disclaimer():
    """Print important disclaimer before script execution."""
    print("=" * 80)
    print("⚠️  EDUCATIONAL SOFTWARE - USE AT YOUR OWN RISK ⚠️")
    print("=" * 80)
    print("📚 This script is for EDUCATIONAL PURPOSES ONLY")
    print("🚫 NOT for financial advice or commercial trading")
    print("⚖️  Users assume ALL legal and financial responsibility")
    print("📖 Please read the full disclaimer in the script header")
    print("=" * 80)
    print("🔴 IMPORTANT: By executing this script, you acknowledge that you have")
    print("   read, understood, and agree to be bound by all terms in the")
    print("   disclaimer above. If you do not agree, stop execution now.")
    print("=" * 80)
    print()

# More comprehensive headers to mimic a real browser
def get_realistic_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "DNT": "1",
        "Pragma": "no-cache"
    }

def create_nse_session():
    """
    Create a properly authenticated NSE session that mimics real browser behavior.
    """
    session = requests.Session()
    
    # Set initial headers
    session.headers.update(get_realistic_headers())
    
    try:
        print("🌐 Establishing NSE session...")
        
        # Step 1: Visit homepage like a real user
        print("📋 Loading NSE homepage...")
        homepage_response = session.get(
            "https://www.nseindia.com", 
            timeout=20,
            allow_redirects=True
        )
        homepage_response.raise_for_status()
        
        # Simulate reading the page
        time.sleep(random.uniform(2, 4))
        
        # Step 2: Visit market data section 
        print("📊 Accessing market data section...")
        session.headers.update({
            "Referer": "https://www.nseindia.com/",
            "Sec-Fetch-Site": "same-origin"
        })
        
        market_response = session.get(
            "https://www.nseindia.com/market-data/live-equity-market", 
            timeout=20
        )
        
        # Simulate user interaction
        time.sleep(random.uniform(1, 3))
        
        # Step 3: Make a test API call to warm up session
        print("🔧 Warming up API session...")
        session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors"
        })
        
        # Try a simple API call first
        test_response = session.get(
            "https://www.nseindia.com/api/allIndices",
            timeout=20
        )
        
        if test_response.status_code == 200:
            print("✅ NSE session established successfully")
            return session
        else:
            print(f"⚠️  Session test returned {test_response.status_code}")
            return session
        
    except Exception as e:
        print(f"❌ Error establishing NSE session: {e}")
        return None

def get_nse_master():
    """
    Retrieve the NSE master equity data from the official API.
    """
    session = create_nse_session()
    if not session:
        return None, None
        
    master_url = "https://www.nseindia.com/api/equity-master"
    
    try:
        print("📥 Fetching NSE master data...")
        
        # Add random delay to mimic human behavior
        time.sleep(random.uniform(1, 3))
        
        response = session.get(master_url, timeout=20)
        response.raise_for_status()
        data = response.json()
        print(f"✅ Successfully retrieved master data with {len(data)} categories")
        return data, session
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("🔒 Authentication failed. Trying enhanced session...")
            
            # Enhanced retry with different approach
            session = create_nse_session()
            if session:
                try:
                    # Visit more pages to look more human
                    session.get("https://www.nseindia.com/market-data/bonds-traded-in-capital-market", timeout=20)
                    time.sleep(random.uniform(2, 4))
                    
                    response = session.get(master_url, timeout=20)
                    response.raise_for_status()
                    data = response.json()
                    return data, session
                except Exception as retry_e:
                    print(f"❌ Enhanced retry failed: {retry_e}")
        
        print(f"❌ HTTP Error retrieving master data: {e}")
        return None, session
        
    except Exception as e:
        print(f"❌ Error retrieving master data: {e}")
        return None, session

def get_index_constituents(session, index_name, max_retries=3):
    """
    Retrieve the constituents for a given index using enhanced session handling.
    """
    encoded_index = urllib.parse.quote(index_name)
    url = f"https://www.nseindia.com/api/equity-stockIndices?index={encoded_index}"
    
    for attempt in range(max_retries):
        try:
            # Human-like delay with increasing back-off
            if attempt > 0:
                delay = random.uniform(3, 8) * (attempt + 1)
                print(f"⏳ Retrying {index_name} after {delay:.1f}s delay...")
                time.sleep(delay)
            else:
                # Even first requests need some delay
                time.sleep(random.uniform(0.5, 2.0))
            
            # Refresh some headers to look more dynamic
            session.headers.update({
                "Referer": "https://www.nseindia.com/market-data/live-equity-market",
                "Accept": "application/json, text/plain, */*"
            })
            
            response = session.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            all_constituents = data.get("data", [])
            if not all_constituents:
                print(f"📭 No constituents data found for {index_name}")
                return []
                
            # Skip the first element if its symbol matches the index name
            if all_constituents and all_constituents[0].get("symbol", "").upper() == index_name.upper():
                constituents = all_constituents[1:]  # Skip index itself
            else:
                constituents = all_constituents
                
            print(f"✅ Successfully retrieved {len(constituents)} constituents for {index_name}")
            return constituents
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print(f"🔒 Authentication failed for {index_name} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print("🔄 Creating fresh session...")
                    # Create completely new session
                    new_session = create_nse_session()
                    if new_session:
                        session.cookies.clear()
                        session.cookies.update(new_session.cookies)
                        session.headers.clear()
                        session.headers.update(new_session.headers)
            elif e.response.status_code == 429:
                print(f"🚫 Rate limited for {index_name}. Waiting longer...")
                time.sleep(random.uniform(10, 20))
            else:
                print(f"❌ HTTP {e.response.status_code} error for {index_name}: {e}")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"🌐 Network error for {index_name} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                break
                
        except Exception as e:
            print(f"❌ Unexpected error for {index_name}: {e}")
            break
            
        # Longer delay between retries
        time.sleep(random.uniform(2, 5))
    
    print(f"❌ Failed to retrieve constituents for {index_name} after {max_retries} attempts")
    return []

def main():
    print("🚀 Starting NSE Index Constituents Retrieval")
    print("=" * 50)
    
    master_data, session = get_nse_master()
    if master_data is None:
        print("❌ Failed to retrieve master data. Exiting.")
        return

    # Focus on most important indices to reduce API load
    important_indices = [
        "NIFTY 50", "NIFTY NEXT 50", "NIFTY 100", "NIFTY 200", "NIFTY 500",
        "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
        "NIFTY SMALLCAP 50", "NIFTY SMALLCAP 100", "NIFTY SMALLCAP 250",
        "NIFTY BANK", "NIFTY IT", "NIFTY FMCG", "NIFTY PHARMA", "NIFTY AUTO",
        "NIFTY FINANCIAL SERVICES", "NIFTY ENERGY", "NIFTY METAL", "NIFTY REALTY"
    ]

    print(f"📊 Processing {len(important_indices)} important indices...")
    rows = []
    successful_indices = 0
    
    # Process indices with progress bar
    for i, index_name in enumerate(tqdm(important_indices, desc="Processing indices", ncols=100)):
        print(f"\n📈 Processing: {index_name} ({i+1}/{len(important_indices)})")
        
        constituents = get_index_constituents(session, index_name)
        
        if constituents:
            successful_indices += 1
            for c in constituents:
                row = {
                    "Index": index_name,
                    "Index Type": "INDEX",
                    "Stock": c.get("symbol", None)
                }
                # Include all additional columns
                for k, v in c.items():
                    if k not in row:
                        row[k] = v
                rows.append(row)
        
        # Human-like browsing pattern - longer delays after every few requests
        if (i + 1) % 5 == 0:
            delay = random.uniform(5, 10)
            print(f"⏸️  Taking a short break ({delay:.1f}s) to avoid detection...")
            time.sleep(delay)
        else:
            time.sleep(random.uniform(1, 3))

    print(f"\n📊 Processing completed!")
    print(f"✅ Successfully processed: {successful_indices}/{len(important_indices)} indices")

    if rows:
        final_df = pd.DataFrame(rows)
        
        # Reorder columns
        cols = final_df.columns.tolist()
        for col in ["Index", "Index Type", "Stock"]:
            if col in cols:
                cols.remove(col)
                cols.insert(0, col)
        final_df = final_df[cols]

        # Ensure data folder exists
        if not os.path.exists("data"):
            os.makedirs("data")

        # Process data
        final_df["Index"] = final_df["Index"].str.upper()
        midcap_stocks = set(final_df[final_df["Index"].str.contains("MIDCAP")]['Stock'])
        smallcap_stocks = set(final_df[final_df["Index"].str.contains("SMALLCAP")]['Stock'])

        def classify_market_cap(stock):
            if stock in midcap_stocks:
                return "Mid Cap"
            elif stock in smallcap_stocks:
                return "Small Cap"
            else:
                return "Large Cap"

        final_df["Market Cap Classification"] = final_df["Stock"].apply(classify_market_cap)

        # Save results
        final_df.to_csv("data/index_constituents.csv", index=False)
        print(f"\n✅ Successfully saved {len(final_df)} records to data/index_constituents.csv")
        
        # Print summary
        print(f"\n📈 Summary:")
        print(f"   📊 Total unique stocks: {final_df['Stock'].nunique()}")
        print(f"   🏢 Large Cap: {sum(final_df['Market Cap Classification'] == 'Large Cap')}")
        print(f"   🏭 Mid Cap: {sum(final_df['Market Cap Classification'] == 'Mid Cap')}")
        print(f"   🏪 Small Cap: {sum(final_df['Market Cap Classification'] == 'Small Cap')}")
        
    else:
        print("\n❌ No constituent data retrieved.")
        print("💡 Suggestions:")
        print("   1. Check your internet connection")
        print("   2. Try running the script at a different time")
        print("   3. NSE may have updated their bot detection")
        print("   4. Consider using Option 2 (Selenium) below")

if __name__ == "__main__":
    print_disclaimer()
    main()