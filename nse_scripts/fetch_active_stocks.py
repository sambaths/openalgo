#!/usr/bin/env python3
"""
NSE Active Stocks Data Fetcher
==============================

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

import requests
import pandas as pd
from collections import defaultdict
import time
import random
import os

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
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="135", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "DNT": "1",
        "Pragma": "no-cache"
    }

def create_enhanced_nse_session():
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

# Enhanced function to fetch data from a given NSE API endpoint with retry logic
def fetch_nse_data(url, session, category_name, max_retries=3):
    for attempt in range(max_retries):
        try:
            # Human-like delay with increasing back-off
            if attempt > 0:
                delay = random.uniform(3, 8) * (attempt + 1)
                print(f"⏳ Retrying {category_name} after {delay:.1f}s delay...")
                time.sleep(delay)
            else:
                # Even first requests need some delay
                time.sleep(random.uniform(0.5, 2.0))
            
            # Refresh some headers to look more dynamic
            session.headers.update({
                "Referer": "https://www.nseindia.com/market-data/live-equity-market",
                "Accept": "application/json, text/plain, */*"
            })
            
            print(f"📥 Fetching {category_name}...")
            response = session.get(url, timeout=20)
            response.raise_for_status()
            
            data = response.json()
            print(f"✅ Successfully retrieved {category_name}")
            return data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print(f"🔒 Authentication failed for {category_name} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print("🔄 Creating fresh session...")
                    # Create completely new session
                    new_session = create_enhanced_nse_session()
                    if new_session:
                        session.cookies.clear()
                        session.cookies.update(new_session.cookies)
                        session.headers.clear()
                        session.headers.update(new_session.headers)
            elif e.response.status_code == 429:
                print(f"🚫 Rate limited for {category_name}. Waiting longer...")
                time.sleep(random.uniform(10, 20))
            else:
                print(f"❌ HTTP {e.response.status_code} error for {category_name}: {e}")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"🌐 Network error for {category_name} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                break
                
        except Exception as e:
            print(f"❌ Unexpected error for {category_name}: {e}")
            break
            
        # Longer delay between retries
        time.sleep(random.uniform(2, 5))
    
    print(f"❌ Failed to retrieve {category_name} after {max_retries} attempts")
    return None

# Main function to aggregate data from multiple endpoints
def main():
    print("🚀 Starting NSE Active Stocks Data Retrieval")
    print("=" * 50)
    
    session = create_enhanced_nse_session()
    if not session:
        print("❌ Failed to establish NSE session. Exiting.")
        return

    # Define the NSE API endpoints and their corresponding categories
    endpoints = {
        "Most Active by Value": "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value",
        "Volume Gainers": "https://www.nseindia.com/api/live-analysis-volume-gainers",
        "Large Deals": "https://www.nseindia.com/api/snapshot-capital-market-largedeal",
        "Most Active by Volume": "https://www.nseindia.com/api/live-analysis-most-active-securities?index=volume",
        "Top Gainers": "https://www.nseindia.com/api/live-analysis-variations?index=gainers",
        "Top Losers": "https://www.nseindia.com/api/live-analysis-variations?index=loosers",
    }

    print(f"📊 Processing {len(endpoints)} data categories...")
    
    # Fetch data from each endpoint with enhanced session handling
    data = {}
    successful_categories = 0
    
    for i, (category, url) in enumerate(endpoints.items()):
        print(f"\n📈 Processing: {category} ({i+1}/{len(endpoints)})")
        
        result = fetch_nse_data(url, session, category)
        if result is not None:
            data[category] = result
            successful_categories += 1
        else:
            data[category] = []
        
        # Human-like browsing pattern - longer delays after every few requests
        if (i + 1) % 3 == 0 and i < len(endpoints) - 1:
            delay = random.uniform(3, 6)
            print(f"⏸️  Taking a short break ({delay:.1f}s) to avoid detection...")
            time.sleep(delay)
        else:
            time.sleep(random.uniform(1, 2))

    print(f"\n📊 Data fetching completed!")
    print(f"✅ Successfully processed: {successful_categories}/{len(endpoints)} categories")

    if successful_categories == 0:
        print("\n❌ No data retrieved. Please check your internet connection and try again.")
        return

    # Process the fetched data
    print("\n🔄 Processing and structuring data...")
    
    # Extract data from nested structures
    processed_data = {}
    
    if 'Most Active by Value' in data and data['Most Active by Value']:
        processed_data['Most Active by Value'] = data['Most Active by Value'].get('data', [])
    
    if 'Most Active by Volume' in data and data['Most Active by Volume']:
        processed_data['Most Active by Volume'] = data['Most Active by Volume'].get('data', [])
    
    if 'Volume Gainers' in data and data['Volume Gainers']:
        processed_data['Volume Gainers'] = data['Volume Gainers'].get('data', [])

    # Large Deals - split into sub-categories
    if 'Large Deals' in data and data['Large Deals']:
        large_deals = data['Large Deals']
        processed_data['Bulk Deals Data'] = large_deals.get('BULK_DEALS_DATA', [])
        processed_data['Block Deals Data'] = large_deals.get('BLOCK_DEALS_DATA', [])
        processed_data['Short Deals Data'] = large_deals.get('SHORT_DEALS_DATA', [])

    # Gainers and Losers
    if 'Top Gainers' in data and data['Top Gainers']:
        processed_data['Top Gainers'] = data['Top Gainers'].get('allSec', {}).get('data', [])

    if 'Top Losers' in data and data['Top Losers']:
        processed_data['Top Losers'] = data['Top Losers'].get('allSec', {}).get('data', [])

    # Aggregate reasons for each stock's inclusion
    stock_reasons = defaultdict(set)
    all_data = pd.DataFrame()
    
    for category, entries in processed_data.items():
        if not entries:  # Skip empty categories
            continue
            
        print(f"📋 Processing {len(entries)} entries from {category}")
        category_df = pd.DataFrame()
        
        for entry in entries:
            if isinstance(entry, dict):
                temp = pd.DataFrame(entry, index=[0])
                temp['Reason'] = category
                temp['relevant_columns'] = ", ".join(entry.keys())
                category_df = pd.concat([category_df, temp], ignore_index=True)
        
        if not category_df.empty:
            all_data = pd.concat([all_data, category_df], ignore_index=True)

    # Ensure data folder exists
    if not os.path.exists("data"):
        os.makedirs("data")

    if not all_data.empty:
        # Save to CSV
        output_file = "data/active_stocks_with_reasons.csv"
        all_data.to_csv(output_file, index=False)
        
        print(f"\n✅ Successfully saved {len(all_data)} records to {output_file}")
        
        # Print summary
        print(f"\n📈 Summary:")
        print(f"   📊 Total records: {len(all_data)}")
        print(f"   🔍 Categories processed: {all_data['Reason'].nunique()}")
        print(f"   📋 Category breakdown:")
        for reason, count in all_data['Reason'].value_counts().items():
            print(f"      - {reason}: {count} records")
        
        # Show unique symbols if symbol column exists
        symbol_columns = [col for col in all_data.columns if 'symbol' in col.lower()]
        if symbol_columns:
            unique_symbols = all_data[symbol_columns[0]].nunique()
            print(f"   🏢 Unique stocks: {unique_symbols}")
    else:
        print("\n❌ No valid data processed from any category.")
        print("💡 This might indicate:")
        print("   1. API structure has changed")
        print("   2. Network connectivity issues")
        print("   3. NSE has updated their bot detection")

if __name__ == "__main__":
    print_disclaimer()
    main()