import logging
import yaml, json
import pandas as pd
import requests
import urllib.request, urllib.parse
import pandas as pd
import holidays
from psycopg2.errors import UndefinedTable
import multiprocessing
import time
from logger import logger
from bs4 import BeautifulSoup
import re
import os
from datetime import datetime
from typing import Dict, Optional
import pytz

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --------------------- Helper Functions for NSE -------------------------
def stock_symbols():
    eq_list_pd = pd.read_csv(
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    )
    return eq_list_pd["SYMBOL"].tolist()


def get_all_equities_nse(filter="NIFTY TOTAL MARKET"):
    index_master = [
        "NIFTY 50",
        "NIFTY NEXT 50",
        "NIFTY 100",
        "NIFTY 200",
        "NIFTY TOTAL MARKET",
        "NIFTY 500",
        "NIFTY500 MULTICAP 50:25:25",
        "NIFTY500 LARGEMIDSMALL EQUAL-CAP WEIGHTED",
        "NIFTY MIDCAP 150",
        "NIFTY MIDCAP 50",
        "NIFTY MIDCAP SELECT",
        "NIFTY MIDCAP 100",
        "NIFTY SMALLCAP 250",
        "NIFTY SMALLCAP 50",
        "NIFTY SMALLCAP 100",
        "NIFTY MICROCAP 250",
        "NIFTY LARGEMIDCAP 250",
        "NIFTY MIDSMALLCAP 400",
        "NIFTY AUTO",
        "NIFTY BANK",
        "NIFTY FINANCIAL SERVICES",
        "NIFTY FINANCIAL SERVICES 25/50",
        "NIFTY FINANCIAL SERVICES EX BANK",
        "NIFTY FMCG",
        "NIFTY HEALTHCARE",
        "NIFTY IT",
        "NIFTY MEDIA",
        "NIFTY METAL",
        "NIFTY PHARMA",
        "NIFTY PRIVATE BANK",
        "NIFTY PSU BANK",
        "NIFTY REALTY",
        "NIFTY CONSUMER DURABLES",
        "NIFTY OIL AND GAS",
        "NIFTY MIDSMALL FINANCIAL SERVICES",
        "NIFTY MIDSMALL HEALTHCARE",
        "NIFTY MIDSMALL IT & TELECOM",
    ]
    assert filter in index_master, f"{filter} not in index_master"
    headers = {
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        + "(KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;"
        + "q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    }
    with requests.Session() as session:
        session.get("http://nseindia.com", headers=headers)
        url = urllib.parse.quote(
            f"https://www.nseindia.com/api/equity-stockIndices?index={filter}",
            safe=":/?&=",
        )
        output = session.get(url, headers=headers).json()
    out = pd.DataFrame(output["data"])
    return out.iloc[1:]


def get_live_indices(indextype: list = ["eq"], indexsubtype: list = ["bm", "sc"]):
    """
    Get the Live Indices data
    """
    with urllib.request.urlopen(
        "https://iislliveblob.niftyindices.com/jsonfiles/LiveIndicesWatch.json"
    ) as url:
        data = json.load(url)
    live_indices = pd.DataFrame(data["data"])
    live_indices = live_indices[
        (live_indices["indexType"].isin(indextype))
        & (live_indices["indexSubType"].isin(indexsubtype))
        & (live_indices["indexName"] != "INDIA VIX")
    ]
    # Since this will mostly be bm or sc (Broad Market or Sector), renaming them to be more meaningful
    return live_indices


def top_n_indices(n: int):
    """
    Get Top N movers (Indices) in the Broad and Sectoral Scope
    """
    live_indices = get_live_indices(indextype=["eq"], indexsubtype=["bm", "sc"])
    broad = live_indices[live_indices["indexSubType"] == "bm"]
    broad_gain_n = broad.sort_values("percChange", ascending=False).head(n)
    broad_lose_n = broad.sort_values("percChange", ascending=True).head(n)

    sec = live_indices[live_indices["indexSubType"] == "sc"]
    sec_gain_n = sec.sort_values("percChange", ascending=False).head(n)
    sec_lose_n = sec.sort_values("percChange", ascending=True).head(n)

    out = pd.concat(
        [sec_gain_n, sec_lose_n, broad_gain_n, broad_lose_n], axis=0
    ).reset_index(drop=True)
    out["indexSubType"] = out["indexSubType"].map(
        {"bm": "Broad Market", "sc": "Sector"}
    )
    return out


def get_index_constituents(index: str):
    """
    Get Information regarding the Stocks present in the Index along
    with how much it changed the index
    """
    indices_names = index_mapping()
    with urllib.request.urlopen(
        urllib.parse.quote(
            f"https://iislliveblob.niftyindices.com/jsonfiles/Heatmap/FinalHeatMap{indices_names[index].upper()}.json",
            safe=":/?&=",
        )
    ) as url:
        data = json.load(url)
    out = pd.DataFrame(data)
    return out


def get_top_n_stocks(top_indices_n: int = 2, top_stocks_n: int = 3):
    """
    Give the Top `top_stocks_n` * 2 (Gainers and Losers) movers stocks from
    the Top `n` Indices both Sector wise and Broadly.
    """

    top_movers = top_n_indices(top_indices_n)

    all_movers = pd.DataFrame()
    # Identify Top Stocks from Top Movers
    for idx, row in top_movers.iterrows():
        # Get Index Constituents
        index = row["indexName"]
        indices_const = get_index_constituents(index)

        # Top 3 stocks that moved up most in the indices
        gain_3 = indices_const.sort_values("perchange", ascending=False).head(
            top_stocks_n
        )

        # Top 3 stocks that moved down most in the indices
        loss_3 = indices_const.sort_values("perchange", ascending=True).head(
            top_stocks_n
        )

        out = pd.concat([gain_3, loss_3], axis=0)
        out["index"] = index
        all_movers = pd.concat([all_movers, out])

    return {"stocks": all_movers, "indices": parse_data(top_movers)}


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        # logger.error(f"Error loading configuration: {str(e)}")
        raise


def get_nse_trading_days(year):
    """
    Returns a list of dates (as pd.Timestamp) for which NSE trading likely occurred,
    by generating all business days (Monday–Friday) for the year and removing Indian public holidays.

    Note: This is an approximation. NSE may observe additional closures or half-days.

    Args:
        year (int): The year (e.g., 2023).

    Returns:
        list of pd.Timestamp: Trading day dates.
    """
    # Generate all weekdays (Monday-Friday) for the year.
    all_bdays = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31", freq="B")

    # Use the 'holidays' package for India (install via pip install holidays).
    india_holidays = holidays.India(years=year)

    # Filter out dates that are public holidays.
    trading_days = [day.date() for day in all_bdays if day.date() not in india_holidays]
    return trading_days


def get_previous_trading_day(trading_days, current_date):
    """
    Given a sorted list of trading day dates (datetime.date objects) and a current date,
    returns the previous trading day (i.e. the greatest trading day strictly before current_date).

    Args:
        trading_days (list): A sorted list of datetime.date objects representing trading days.
        current_date (datetime.date): The reference date.

    Returns:
        datetime.date or None: The previous trading day, or None if none exists.
    """
    previous_day = None
    for day in trading_days:
        if day < current_date:
            previous_day = day
        else:
            break
    return previous_day


def get_last_ending_capital(db_pool, date):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT ending_capital FROM capital_log where log_date <= CAST('{date}' AS DATE) ORDER BY log_date DESC LIMIT 1"
        )
        result = cursor.fetchone()
        cursor.close()
        if result:
            return result[0]
        else:
            return None
    except UndefinedTable:
        logger.info("`capital_log` doesn't exist.")
        return None
    finally:
        db_pool.putconn(conn)


def validate_shared_state(shared_state, shared_lock, worker_id=None):
    """
    Validate that the shared state is accessible and properly synchronized across processes.
    
    Args:
        shared_state: Manager.dict() object
        shared_lock: Manager.Lock() object  
        worker_id: Optional worker ID for logging
    
    Returns:
        bool: True if shared state is valid and accessible
    """
    try:
        with shared_lock:
            # Test basic read access
            test_keys = list(shared_state.keys())
            
            # Test basic write access
            test_key = f"health_check_{worker_id or 'main'}_{int(time.time())}"
            shared_state[test_key] = True
            
            # Test read-back
            if shared_state.get(test_key) == True:
                # Clean up test key
                del shared_state[test_key]
                logger.debug(f"Shared state validation successful for {worker_id or 'main'}")
                return True
            else:
                logger.error(f"Shared state read-back test failed for {worker_id or 'main'}")
                return False
                
    except Exception as e:
        logger.error(f"Shared state validation failed for {worker_id or 'main'}: {str(e)}")
        return False

def safe_update_shared_state(shared_state, shared_lock, key, value, worker_id=None):
    """
    Safely update shared state with proper error handling and logging.
    
    Args:
        shared_state: Manager.dict() object
        shared_lock: Manager.Lock() object
        key: Key to update
        value: Value to set
        worker_id: Optional worker ID for logging
    
    Returns:
        bool: True if update was successful
    """
    try:
        with shared_lock:
            shared_state[key] = value
            logger.debug(f"Successfully updated shared_state[{key}] = {value} from {worker_id or 'main'}")
            return True
    except Exception as e:
        logger.error(f"Failed to update shared_state[{key}] from {worker_id or 'main'}: {str(e)}")
        return False

def safe_get_shared_state(shared_state, shared_lock, key, default=None, worker_id=None):
    """
    Safely get value from shared state with proper error handling.
    
    Args:
        shared_state: Manager.dict() object
        shared_lock: Manager.Lock() object
        key: Key to get
        default: Default value if key doesn't exist
        worker_id: Optional worker ID for logging
    
    Returns:
        Value from shared state or default
    """
    try:
        with shared_lock:
            value = shared_state.get(key, default)
            logger.debug(f"Successfully retrieved shared_state[{key}] = {value} from {worker_id or 'main'}")
            return value
    except Exception as e:
        logger.error(f"Failed to get shared_state[{key}] from {worker_id or 'main'}: {str(e)}")
        return default

def monitor_manager_health(shared_state, shared_lock, interval=30):
    """
    Monitor the health of the multiprocessing Manager in a background thread.
    
    Args:
        shared_state: Manager.dict() object
        shared_lock: Manager.Lock() object
        interval: Check interval in seconds
    """
    def health_monitor():
        while True:
            try:
                # Validate shared state periodically
                if not validate_shared_state(shared_state, shared_lock, "monitor"):
                    logger.error("Manager health check failed!")
                else:
                    logger.debug("Manager health check passed")
                
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Manager health monitor error: {str(e)}")
                time.sleep(interval)
    
    import threading
    monitor_thread = threading.Thread(target=health_monitor, daemon=True)
    monitor_thread.start()
    return monitor_thread


def get_amfi_symbol_categorization() -> Dict[str, str]:
    """
    Fetch the latest NSE symbol to market cap categorization mapping from AMFI.
    
    Returns:
        Dict[str, str]: Dictionary mapping NSE symbols to their market cap categories
        
    Example:
        {
            "RELIANCE": "Large Cap",
            "TCS": "Large Cap", 
            "ADANIPORTS": "Mid Cap",
            ...
        }
    """
    try:
        # AMFI research page URL
        base_url = "https://www.amfiindia.com"
        research_page = "https://www.amfiindia.com/research-information/other-data/categorization-of-stocks"
        
        # Set up session with headers
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        logger.debug("Fetching latest AMFI market cap categorization data...")
        
        # Try to get the latest Excel file URL from the research page
        try:
            response = session.get(research_page, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for Excel file links containing market cap data
            excel_links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href and ('AverageMarketCapitalization' in href or 'MarketCap' in href) and href.endswith('.xlsx'):
                    if not href.startswith('http'):
                        href = base_url + href
                    excel_links.append(href)
            
            if excel_links:
                # Sort by date in filename to get the most recent
                def extract_date_from_filename(url: str) -> datetime:
                    try:
                        match = re.search(r'(\d{2})(\w{3})(\d{4})', url)
                        if match:
                            day, month_str, year = match.groups()
                            month_map = {
                                'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                                'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                            }
                            month = month_map.get(month_str, 12)
                            return datetime(int(year), month, int(day))
                        return datetime(1900, 1, 1)
                    except:
                        return datetime(1900, 1, 1)
                
                excel_links.sort(key=extract_date_from_filename, reverse=True)
                latest_url = excel_links[0]
                logger.debug(f"Found latest market cap file: {latest_url}")
            else:
                # Fallback to known URL
                latest_url = "https://www.amfiindia.com/Themes/Theme1/downloads/AverageMarketCapitalizationoflistedcompaniesduringthesixmonthsended31Dec2024.xlsx"
                logger.debug("Using fallback URL for market cap data")
                
        except Exception as e:
            logger.debug(f"Could not scrape research page: {e}, using fallback URL")
            latest_url = "https://www.amfiindia.com/Themes/Theme1/downloads/AverageMarketCapitalizationoflistedcompaniesduringthesixmonthsended31Dec2024.xlsx"
        
        # Download and process the Excel file
        logger.debug("Downloading and processing market cap data...")
        response = session.get(latest_url, timeout=60)
        response.raise_for_status()
        
        # Use a temporary file
        temp_filename = f"temp_amfi_{int(time.time())}.xlsx"
        try:
            with open(temp_filename, 'wb') as f:
                f.write(response.content)
            
            # Read and process the Excel file - first try with default header
            df = pd.read_excel(temp_filename, engine='openpyxl')
            
            # Check if the actual headers are in the second row (common in AMFI files)
            # Look at the first few rows to determine the correct header row
            header_row = 1
            # for row_idx in range(min(3, len(df))):
            #     row_values = [str(val).lower() for val in df.iloc[row_idx].values if str(val) != 'nan']
            #     # Check if this row contains header-like terms
            #     if any(term in ' '.join(row_values) for term in ['symbol', 'nse', 'categorization', 'company', 'scrip']):
            #         header_row = row_idx
            #         logger.debug(f"Found headers in row {row_idx + 1}")
            #         break
            
            # Re-read with correct header row if needed
            if header_row > 0:
                df = pd.read_excel(temp_filename, engine='openpyxl', header=header_row)
                logger.debug(f"Re-reading Excel with header at row {header_row + 1}")
            
            # Find relevant columns
            symbol_cols = []
            category_cols = []
            
            logger.debug(f"Available columns: {list(df.columns)}")
            
            # Look for symbol columns (NSE, Symbol, etc.)
            for col in df.columns:
                col_str = str(col).lower()
                if any(term in col_str for term in ['nse symbol']):
                    symbol_cols.append(col)
            
            # Look for categorization columns (usually the last column or contains 'category', 'cap', etc.)
            for col in df.columns:
                col_str = str(col).lower()
                if any(term in col_str for term in ['categorization']):
                    category_cols.append(col)
            
            # If no specific category column found, use the last column
            if not category_cols and len(df.columns) > 1:
                category_cols = [df.columns[-1]]
            
            logger.debug(f"Found symbol columns: {symbol_cols}")
            logger.debug(f"Found category columns: {category_cols}")
            
            # Extract the mapping
            symbol_category_mapping = {}
            
            if symbol_cols and category_cols:
                symbol_col = symbol_cols[0]  # Use first symbol column
                category_col = category_cols[0]  # Use first category column
                
                logger.debug(f"Using symbol column: {symbol_col}, category column: {category_col}")
                
                for _, row in df.iterrows():
                    symbol = str(row[symbol_col]).strip()
                    category = str(row[category_col]).strip()
                    
                    # Skip empty or invalid entries
                    if symbol and category and symbol != 'nan' and category != 'nan':
                        symbol_category_mapping[symbol] = category
            
            logger.debug(f"Successfully extracted {len(symbol_category_mapping)} symbol-category mappings")
            return symbol_category_mapping
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
                
    except Exception as e:
        logger.error(f"Error fetching AMFI market cap categorization: {e}")
        return {}


def structure_data(ticker: str, data: dict) -> pd.DataFrame:
    """
    Structure data into a standard format for backtesting.
    
    Args:
        ticker: Stock ticker symbol
        data: Raw data from Fyers API
        
    Returns:
        pd.DataFrame: Structured DataFrame with required fields
    """

    IST = pytz.timezone('Asia/Kolkata')

    # Check if data is already a DataFrame
    if isinstance(data, pd.DataFrame):
        return data
        
    # Handle empty or invalid responses
    if not isinstance(data, dict) or 'candles' not in data or not data['candles']:
        return pd.DataFrame()  # Return empty DataFrame
        
    # Create DataFrame from fetched data
    df = pd.DataFrame(data['candles'], columns=['t', 'o', 'h', 'l', 'c', 'v'])
    
    # Convert timestamp to datetime with proper timezone
    df['datetime'] = pd.to_datetime(df['t'], unit='s').dt.tz_localize(pytz.UTC).dt.tz_convert(IST)
    
    # Calculate cumulative volume
    df['cum_vol'] = df['v'].cumsum()
    # Calculate previous close price using shift
    df['prev_close'] = df['c'].shift(1).fillna(df['o'].iloc[0])

    # Calculate change and change percentage
    df['ch'] = df['c'] - df['prev_close']
    df['chp'] = (df['ch'] / df['prev_close'] * 100).fillna(0)

    # Calculate average trade price
    df['avg_trade_price'] = (df['o'] + df['h'] + df['l'] + df['c']) / 4

    # Prepare the final DataFrame with required metrics
    metrics_df = pd.DataFrame({
        'datetime': df['datetime'],  # Keep the datetime column as is
        'symbol': ticker,
        'ltp': df['c'].round(2),
        'vol_traded_today': df['cum_vol'].astype(int),
        'last_traded_time': df['t'].astype(int),
        'exch_feed_time': df['t'].astype(int),
        'bid_size': 0,
        'ask_size': 0,
        'bid_price': (df['c'] - 0.05).round(2),
        'ask_price': (df['c'] + 0.05).round(2),
        'last_traded_qty': df['v'].astype(int),
        'tot_buy_qty': (df['cum_vol'] // 2).astype(int),
        'tot_sell_qty': (df['cum_vol'] - (df['cum_vol'] // 2)).astype(int),
        'avg_trade_price': df['avg_trade_price'].round(2),
        'low_price': df['l'].round(2),
        'high_price': df['h'].round(2),
        'open_price': df['o'].round(2),
        'prev_close_price': df['prev_close'].round(2),
        'type': "historical",
        'ch': df['ch'].round(2),
        'chp': df['chp'].round(2)
    })
    
    return metrics_df


if __name__ == "__main__":
    print(stock_symbols())