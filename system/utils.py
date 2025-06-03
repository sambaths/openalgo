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
                logger.info(f"Shared state validation successful for {worker_id or 'main'}")
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
