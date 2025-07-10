#!/usr/bin/env python3
"""
socket_simulator.py

This module fetches actual historical tick data for a given day (using the Fyers API)
for one or more symbols and simulates a live data feed by streaming the data over a WebSocket.
For each symbol, it sends one record per second (from market open to market close).
The output structure matches that of your simulated data.
"""

import os
import asyncio
import websockets
import json
import pandas as pd
from datetime import datetime, timedelta
import argparse
import system_utils as utils


# --------------------- Helper Functions ---------------------
def format_symbol(ticker: str) -> str:
    """
    Convert a ticker (e.g. "RELIANCE") into the desired format (e.g. "NSE:RELIANCE-EQ").
    """
    return f"NSE:{ticker}-EQ"


def fetch_historical_ticks(
    fyers_model, ticker: str, date: str, resolution: str = "5S"
) -> list:
    """
    Fetch historical tick data for the given ticker and date using the Fyers API.

    Args:
        fyers_model: An authenticated Fyers API client.
        ticker (str): Ticker symbol (e.g. "RELIANCE").
        date (str): Date in 'YYYY-MM-DD' format.
        resolution (str): Resolution to use (e.g. '5S' for 5-second data).

    Returns:
        A list of dictionaries—one per tick—with the following fields:
          - symbol
          - ltp
          - vol_traded_today
          - last_traded_time
          - exch_feed_time
          - bid_size
          - ask_size
          - bid_price
          - ask_price
          - last_traded_qty
          - tot_buy_qty
          - tot_sell_qty
          - avg_trade_price
          - low_price
          - high_price
          - open_price
          - prev_close_price
          - type
          - ch
          - chp
    """
    # Build API parameters.
    data_headers = {
        "symbol": f"NSE:{ticker}-EQ",
        "resolution": resolution,
        "date_format": "1",
        "range_from": date,
        "range_to": date,
        "cont_flag": "1",
    }

    # Fetch data from Fyers API.
    data = fyers_model.history(data=data_headers)
    if "candles" not in data or len(data["candles"]) == 0:
        print(f"No historical data returned for {ticker}.")
        return []

    df = pd.DataFrame(data["candles"], columns=["t", "o", "h", "l", "c", "v"])
    # Convert timestamp to datetime.
    df["datetime"] = pd.to_datetime(
        df["t"].apply(
            lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d %H:%M:%S")
        )
    )

    # Filter for market hours (09:15:00 to 15:30:00).
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    market_open = date_obj.replace(hour=9, minute=15, second=0)
    market_close = date_obj.replace(hour=15, minute=30, second=0)
    df = df[(df["datetime"] >= market_open) & (df["datetime"] <= market_close)]
    df = df.sort_values(by="datetime").reset_index(drop=True)

    # Compute cumulative volume.
    df["cum_vol"] = df["v"].cumsum()

    # Determine the "previous close" price.
    prev_close = df["o"].iloc[0]
    prev_closes = [prev_close]
    for idx in range(1, len(df)):
        prev_closes.append(df["c"].iloc[idx - 1])
    df["prev_close"] = prev_closes

    # Build the list of tick records.
    records = []
    for idx, row in df.iterrows():
        ltp = row["c"]
        prev_close_price = row["prev_close"]
        ch = ltp - prev_close_price
        chp = (ch / prev_close_price * 100) if prev_close_price != 0 else 0
        record = {
            "symbol": format_symbol(ticker),
            "ltp": round(ltp, 2),
            "vol_traded_today": int(row["cum_vol"]),
            "last_traded_time": int(row["t"]),
            "exch_feed_time": int(row["t"]),
            "bid_size": 0,
            "ask_size": 0,
            "bid_price": round(ltp - 0.05, 2),
            "ask_price": round(ltp + 0.05, 2),
            "last_traded_qty": int(row["v"]),
            "tot_buy_qty": int(row["cum_vol"] // 2),
            "tot_sell_qty": int(row["cum_vol"] - (row["cum_vol"] // 2)),
            "avg_trade_price": round(
                (row["o"] + row["h"] + row["l"] + row["c"]) / 4, 2
            ),
            "low_price": round(row["l"], 2),
            "high_price": round(row["h"], 2),
            "open_price": round(row["o"], 2),
            "prev_close_price": round(prev_close_price, 2),
            "type": "historical",
            "ch": round(ch, 2),
            "chp": round(chp, 2),
            "lower_ckt": 0,
            "upper_ckt": 0,
        }
        records.append(record)
    return records


# --------------------- WebSocket Streaming ---------------------
async def client_handler(websocket, path, records_dict):
    """
    When a client connects, stream the historical records for each symbol concurrently.
    """
    print(f"Client connected: {websocket.remote_address}")
    max_ticks = max((len(records) for records in records_dict.values()), default=0)

    try:
        for i in range(max_ticks):
            # For each symbol, if a tick exists at index i, send it.
            for ticker, records in records_dict.items():
                if i < len(records):
                    try:
                        await websocket.send(json.dumps(records[i]))
                    except websockets.exceptions.ConnectionClosed:
                        print("Client disconnected during transmission.")
                        return
                    await asyncio.sleep(0.5)
            await asyncio.sleep(0.5)
    finally:
        print(f"Client disconnected: {websocket.remote_address}")


async def main_simulation(
    fyers_model, tickers: list, date: str, host: str, port: int, resolution: str = "5S"
):
    """
    Fetch historical data for each symbol and start the WebSocket server
    to stream out one record per second per symbol.
    """
    print(f"Fetching historical data for {', '.join(tickers)} on {date} ...")

    records_dict = {}
    for ticker in tickers:
        recs = await asyncio.to_thread(
            fetch_historical_ticks, fyers_model, ticker, date, resolution
        )
        if recs:
            records_dict[ticker] = recs
        else:
            print(f"Warning: No data for {ticker}.")

    if not records_dict:
        print("No records available for simulation for any symbol. Exiting.")
        return

    async def handler(websocket, path=None):
        await client_handler(websocket, path, records_dict)

    print(f"Starting WebSocket server on {host}:{port} ...")
    async with websockets.serve(handler, host, port):
        print("WebSocket server started. Waiting for client connections...")
        await asyncio.Future()  # Run forever


# --------------------- Main Entry Point ---------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Historical Data Simulator")
    # Note: Either --ticker or --index must be provided.
    parser.add_argument(
        "--ticker",
        type=str,
        nargs="+",
        default=None,
        help="Ticker symbol(s) (e.g. RELIANCE TCS HDFC)",
    )
    parser.add_argument(
        "--index",
        type=str,
        default=None,
        help="Index filter for NSE stocks (e.g. 'NIFTY 50')",
    )
    parser.add_argument(
        "--date", type=str, required=None, help="Date in DD-MM-YYYYY format"
    )
    parser.add_argument(
        "--host", type=str, default="localhost", help="Host for the WebSocket server"
    )
    parser.add_argument(
        "--port", type=int, default=8080, help="Port for the WebSocket server"
    )
    parser.add_argument(
        "--resolution",
        type=str,
        default="1",
        help="Resolution for historical data (e.g. '5S' for 5-second intervals)",
    )
    args = parser.parse_args()

    config = utils.load_config(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    )
    if (
        config["trading_setting"]["simulation"]
        and args.ticker is None
        and args.index is None
    ):
        prev_trading_date = utils.get_previous_trading_day(
            utils.get_nse_trading_days(datetime.now().date().year),
            pd.to_datetime(
                config["trading_setting"]["simulation_date"], format="%d-%m-%Y"
            ).date(),
        )
        # vol_args = volatility_scanner.argparse.Namespace(
        #     date=pd.to_datetime(config['trading_setting']['simulation_date'], format = "%d-%m-%Y").date().strftime("%d-%m-%Y"),
        #     top=config['scanners']['top'],
        #     mcap=config['scanners']['mcap'],
        #     local=None,
        #     historical=None,
        #     sector=None,
        #     sharpe=0.5,
        #     volume=1e6,
        #     vol=0.05
        # )
        print(
            "(SIMULATED) Previous Trading Date - ",
            prev_trading_date.strftime("%d-%m-%Y"),
        )
        print(
            "(SIMULATED) Current Trading Date - ",
            config["trading_setting"]["simulation_date"],
        )
        # args.ticker = volatility_scanner.main(vol_args)['SYMBOL'].tolist()
        args.ticker = config["trading_setting"]["symbols"]
        args.date = config["trading_setting"]["simulation_date"]
    # Determine the list of tickers.
    if args.index:
        try:
            index_df = utils.get_all_equities_nse(args.index)
            # Try to extract the ticker column; adjust based on your API response.
            if "symbol" in index_df.columns:
                tickers = index_df["symbol"].tolist()
            elif "SYMBOL" in index_df.columns:
                tickers = index_df["SYMBOL"].tolist()
            else:
                print("Error: Unable to determine ticker column from index data.")
                exit(1)
            print(f"Using tickers from index '{args.index}': {tickers}")
        except AssertionError as e:
            print(f"Error: {e}")
            exit(1)
    elif args.ticker:
        tickers = args.ticker
    else:
        print("Error: Please provide either --ticker or --index argument.")
        exit(1)

    # --------------------- Initialize Fyers API Client ---------------------
    from fyers_utils import FyersBroker

    fyers_broker = FyersBroker()
    fyers_model = fyers_broker.fyers_model

    args.date = pd.to_datetime(args.date, format="%d-%m-%Y").strftime("%Y-%m-%d")
    if fyers_model is None:
        print(
            "Error: Please initialize your Fyers API client (fyers_model) before running the simulation."
        )
    else:
        asyncio.run(
            main_simulation(
                fyers_model, tickers, args.date, args.host, args.port, args.resolution
            )
        )
