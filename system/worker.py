import queue
from multiprocessing import Process, Lock
import logging, os
from logger import logger
import pandas as pd
import importlib
from zoneinfo import ZoneInfo  # Python 3.9+

# logger.setLevel(getattr(logging, os.environ["DEBUG_LEVEL"].upper(), None))
from threading import Thread
from datetime import datetime
import time
import json

from db import setup, DBHandler, MarketData, StockSignals
from strategy_manager import StrategyManager, create_strategy_manager

# Re-usable IST tzinfo instance
IST = ZoneInfo("Asia/Kolkata")

###############################################################################
# MarketDataHandler Class
###############################################################################
class MarketDataHandler:
    """
    Handles the market data stream:
      - Inserts raw data into the market_data table (using MarketDataDBHandler).
      - Processes each bar to generate signals using a provided signal processor.
      - Manages trade execution and logging.

    Attributes:
      db_handler (MarketDataDBHandler): Database handler for raw market data.
      data_queue (queue.Queue): Queue for incoming data bars.
      signal_processors (dict): Dictionary mapping symbols to signal processor instances.
      signals_db (SignalsDBHandler): Handler for inserting signals into the database.
      trade_log_db (TradeLogDBHandler): Handler for logging completed trades.
      capital_log_db (CapitalLogDBHandler): Handler for logging capital changes.
      base_signal_processor (object): Prototype signal processor (used for instantiation per symbol).
      capital_manager (CapitalManager): Reference to the capital manager for trade sizing and entries.
      aggregation_resolution (int): Time window in minutes for aggregating market data.
      aggregated_data (dict): Temporary storage for aggregated market data.
      strategy_manager (StrategyManager): Manages multiple strategies for different symbols.
    """

    def __init__(
        self, signal_processor=None, aggregation_resolution=None, risk_manager=None, shared_state=None, shared_lock=None, config=None, historical_data=None
    ):
        logger.debug("Initializing MarketDataHandler.")
        # Create process-local database connection
        self.engine, self.session = setup()
        self.db_handler = DBHandler(self.engine)
        self.data_queue = queue.SimpleQueue()
        self.running = True
        self.batch_size = 1
        self.aggregation_resolution = (
            int(aggregation_resolution) if aggregation_resolution is not None else None
        )
        self.aggregated_data = {}
        self.signal_processors = {}  # Will create processors per symbol using strategy manager
        self.risk_manager = risk_manager
        self.shared_state = shared_state
        self.shared_lock = shared_lock
        self.historical_data = historical_data
        
        # Initialize strategy manager if config is provided
        if config:
            try:
                self.strategy_manager = create_strategy_manager(config)
                logger.debug("StrategyManager initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize StrategyManager: {e}")
                logger.warning("Falling back to legacy single-strategy mode")
                self.strategy_manager = None
        else:
            logger.warning("No config provided, running in legacy mode")
            self.strategy_manager = None
            
        logger.debug("MarketDataHandler initialized.")

    def get_or_create_signal_processor(self, symbol):
        """
        Get or create a signal processor (strategy instance) for a given symbol.
        
        Args:
            symbol: The symbol to get/create strategy for
            
        Returns:
            Strategy instance for the symbol
        """
        if symbol in self.signal_processors:
            return self.signal_processors[symbol]
        
        try:
            if self.strategy_manager:
                # Use strategy manager to create appropriate strategy for symbol
                # print(self.historical_data[self.historical_data["symbol"] == symbol].reset_index(drop=True))
                processor = self.strategy_manager.create_strategy_instance(
                    symbol=symbol,
                    historical_data=self.historical_data[self.historical_data["symbol"] == symbol].reset_index(drop=True),
                    market_date=None,
                    resolution=None,
                    # lookback_period=10000,
                    use_history=True
                )
                logger.debug(f"Created strategy instance for {symbol}: {processor.__class__.__name__}")
            else:
                # Fallback to legacy mode - import and use the default strategy
                from strategy.trendscore import EnhancedTrendScoreStrategy
                processor = EnhancedTrendScoreStrategy(
                    historical_data=self.historical_data[self.historical_data["symbol"] == symbol].reset_index(drop=True),
                    market_date=None,
                    symbol=symbol,
                    resolution=None,
                    # lookback_period=100,
                    use_history=True
                )
                logger.warning(f"Using legacy default strategy for {symbol}: {processor.__class__.__name__}")
            
            # Cache the processor
            self.signal_processors[symbol] = processor
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create signal processor for {symbol}: {e}")
            # Last resort fallback
            try:
                from strategy.trendscore import EnhancedTrendScoreStrategy
                processor = EnhancedTrendScoreStrategy(
                    historical_data=self.historical_data[self.historical_data["symbol"] == symbol].reset_index(drop=True),
                    market_date=None,
                    symbol=symbol,
                    resolution=None,
                    # lookback_period=100,
                    use_history=True
                )
                logger.warning(f"Emergency fallback to default strategy for {symbol}")
                self.signal_processors[symbol] = processor
                return processor
            except Exception as fallback_error:
                logger.critical(f"Complete failure to create strategy for {symbol}: {fallback_error}")
                raise

    def check_incomplete_trades(self, symbol):
        """
        Check if there are incomplete trades for a symbol that need to be handled
        
        Args:
            symbol: The symbol to check
            
        Returns:
            dict: Dictionary with 'has_incomplete' and details about incomplete trades
        """
        if not self.shared_state or not self.shared_lock:
            return {"has_incomplete": False}
            
        incomplete_info = {
            "has_incomplete": False,
            "entry": None,
            "exit": None, 
            "take_profit": None
        }
        
        try:
            with self.shared_lock:
                # Check for incomplete entry trades
                incomplete_entry = dict(self.shared_state.get("incomplete_entry_trades", {}))
                if symbol in incomplete_entry:
                    incomplete_info["has_incomplete"] = True
                    incomplete_info["entry"] = incomplete_entry[symbol]
                    
                # Check for incomplete exit trades  
                incomplete_exit = dict(self.shared_state.get("incomplete_exit_trades", {}))
                if symbol in incomplete_exit:
                    incomplete_info["has_incomplete"] = True
                    incomplete_info["exit"] = incomplete_exit[symbol]
                    
                # Check for incomplete take_profit trades
                incomplete_tp = dict(self.shared_state.get("incomplete_takeprofit_trades", {}))
                if symbol in incomplete_tp:
                    incomplete_info["has_incomplete"] = True
                    incomplete_info["take_profit"] = incomplete_tp[symbol]
                
        except Exception as e:
            logger.error(f"Error checking incomplete trades for {symbol}: {e}")
            
        return incomplete_info

    def handle_exit_or_takeprofit_signal(self, symbol, new_signal):
        """
        Handle exit or take_profit signals, merging with incomplete trades if needed
        
        Args:
            symbol: The symbol
            new_signal: The new signal
            
        Returns:
            dict: The signal to process (original or merged) or None to discard
        """
        signal_type = new_signal.get("type")
        if signal_type not in ["exit", "take_profit"]:
            return new_signal
            
        incomplete_info = self.check_incomplete_trades(symbol)
        
        # Check if there's an incomplete trade of the same type
        incomplete_trade = incomplete_info.get(signal_type)
        
        if incomplete_trade:
            # Merge relevant keys from previous signal to new signal
            previous_signal = incomplete_trade["signal"]
            merged_signal = new_signal.copy()
            
            # Copy relevant keys from previous signal that might be important
            merge_keys = ["position_to_close", "stop_loss", "take_profit_price", "type", 'signal', 'position_size', 'current_position', 'starting_position_size', 'current_position_size', 'closed_position_size', 'remaining_position_size', 'status', 'note', 'trailing_stop_triggered', 'trailing_stop_level', 'auto_square_off']
            for key in merge_keys:
                if key in previous_signal and key not in merged_signal:
                    merged_signal[key] = previous_signal[key]
                    logger.debug(f"Merged key '{key}' from incomplete {signal_type} trade for {symbol}")
            
            # Update attempt count
            attempt_count = incomplete_trade.get("attempt_count", 1) + 1
            logger.debug(f"Retrying {signal_type} signal for {symbol} (attempt #{attempt_count})")
            
            return merged_signal
        
        return new_signal

    def process_queue(self):
        """
        Continuously processes the data queue.
        """
        
        logger.debug("MarketDataHandler process_queue started.")
        while self.running:
            batch = []
            try:
                while len(batch) < self.batch_size:
                    try:
                        data = self.data_queue.get(timeout=5)
                        if isinstance(data, dict) and "symbol" in data:
                            batch.append(data)
                        else:
                            logger.error(f"Invalid data received: {data}")
                    except queue.Empty:
                        break

                if batch:
                    # Process the batch
                    # if self.aggregation_resolution and not any(
                    #     data["symbol"] in self.risk_manager.active_trade_symbols
                    #     for data in batch
                    # ):
                    if self.aggregation_resolution:
                        logger.debug(f"Aggregating data and processing batch: {batch}")
                        self.aggregate_data_and_process_batch(batch)
                    else:
                        logger.debug(f"Inserting Market data to the Database: {batch}")
                        # Insert Market data to the Database
                        self.db_handler.insert_records(
                            records=[MarketData(**records) for records in batch]
                        )
                        self.process_batch(batch)

            except Exception as e:
                logger.error(f"Error in process_queue: {str(e)}")
                continue

    def aggregate_data_and_process_batch(self, batch):
        """
        Aggregates incoming market data over the specified resolution.
        """
        logger.debug(f"Aggregating data: {batch}")
        self.db_handler.insert_records(
            records=[MarketData(**records) for records in batch]
        )
        for data in batch:
            symbol = data["symbol"]

            # Convert Unix timestamp to a datetime object - ensure it's a numeric value first
            timestamp = data["last_traded_time"]
            if isinstance(timestamp, (int, float)):
                # Convert epoch → tz-aware IST → naive IST
                last_traded_time = datetime.fromtimestamp(timestamp, tz=IST).replace(tzinfo=None)
            else:
                # If it's already a datetime or string, try to handle it appropriately
                if isinstance(timestamp, str):
                    try:
                        # Parse ISO string and normalise to IST
                        dt = datetime.fromisoformat(timestamp)
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(IST).replace(tzinfo=None)
                        last_traded_time = dt
                    except ValueError:
                        # If string format is unknown, log and use current time as fallback
                        logger.warning(
                            f"Could not parse timestamp: {timestamp}, using current time"
                        )
                        last_traded_time = datetime.now(tz=IST).replace(tzinfo=None)
                else:
                    # If it's already a datetime object, use it directly
                    last_traded_time = timestamp

            # Initialize aggregation if not already done
            if symbol not in self.aggregated_data:
                self.aggregated_data[symbol] = {
                    "open_price": data[
                        "open_price"
                    ],  # Opening price is the first price in the period
                    "high_price": data[
                        "high_price"
                    ],  # High price starts as the last traded price
                    "low_price": data[
                        "low_price"
                    ],  # Low price starts as the last traded price
                    "ltp": data[
                        "ltp"
                    ],  # Close price will be updated to the last price
                    "vol_traded_today": data[
                        "vol_traded_today"
                    ],  # Today's traded volume
                    "last_traded_time": last_traded_time,  # Already converted above
                    "exch_feed_time": self._safe_timestamp_conversion(
                        data["exch_feed_time"]
                    ),  # Convert safely
                    "bid_size": data["bid_size"],  # Bid size
                    "ask_size": data["ask_size"],  # Ask size
                    "bid_price": data["bid_price"],  # Bid price
                    "ask_price": data["ask_price"],  # Ask price
                    "last_traded_qty": data["last_traded_qty"],  # Last traded quantity
                    "tot_buy_qty": data["tot_buy_qty"],  # Total buy quantity
                    "tot_sell_qty": data["tot_sell_qty"],  # Total sell quantity
                    "avg_trade_price": data["avg_trade_price"],  # Average trade price
                    "ch": 0.0,  # Price change (to be calculated)
                    "chp": 0.0,  # Percentage price change (to be calculated)
                    "start_time": last_traded_time,
                    "end_time": last_traded_time,
                    "lower_ckt": data["lower_ckt"],  # Lower circuit price
                    "upper_ckt": data["upper_ckt"],  # Upper circuit price
                }
            else:
                # Update the aggregated data
                self.aggregated_data[symbol]["high_price"] = max(
                    self.aggregated_data[symbol]["high_price"], data["high_price"]
                )
                self.aggregated_data[symbol]["low_price"] = min(
                    self.aggregated_data[symbol]["low_price"], data["low_price"]
                )
                self.aggregated_data[symbol]["ltp"] = data[
                    "ltp"
                ]  # Last price is the close
                self.aggregated_data[symbol]["vol_traded_today"] += data[
                    "vol_traded_today"
                ]  # Aggregate volume
                self.aggregated_data[symbol]["last_traded_time"] = (
                    last_traded_time  # Update last traded time
                )
                self.aggregated_data[symbol]["exch_feed_time"] = (
                    self._safe_timestamp_conversion(data["exch_feed_time"])
                )
                self.aggregated_data[symbol]["bid_size"] = data[
                    "bid_size"
                ]  # Update bid size
                self.aggregated_data[symbol]["ask_size"] = data[
                    "ask_size"
                ]  # Update ask size
                self.aggregated_data[symbol]["bid_price"] = data[
                    "bid_price"
                ]  # Update bid price
                self.aggregated_data[symbol]["ask_price"] = data[
                    "ask_price"
                ]  # Update ask price
                self.aggregated_data[symbol]["last_traded_qty"] = data[
                    "last_traded_qty"
                ]  # Update last traded quantity
                self.aggregated_data[symbol]["tot_buy_qty"] += data[
                    "tot_buy_qty"
                ]  # Aggregate total buy quantity
                self.aggregated_data[symbol]["tot_sell_qty"] += data[
                    "tot_sell_qty"
                ]  # Aggregate total sell quantity
                self.aggregated_data[symbol]["lower_ckt"] = data[
                    "lower_ckt"
                ]  # Update lower circuit price
                self.aggregated_data[symbol]["upper_ckt"] = data[
                    "upper_ckt"
                ]  # Update upper circuit price
                # Calculate price change and percentage change
                self.aggregated_data[symbol]["ch"] = (
                    self.aggregated_data[symbol]["ltp"]
                    - self.aggregated_data[symbol]["open_price"]
                )
                self.aggregated_data[symbol]["chp"] = (
                    (
                        self.aggregated_data[symbol]["ch"]
                        / self.aggregated_data[symbol]["open_price"]
                    )
                    * 100
                    if self.aggregated_data[symbol]["open_price"] != 0
                    else 0
                )

            # Update the end time for the aggregation
            self.aggregated_data[symbol]["end_time"] = last_traded_time

            # Check if we need to emit the aggregated data
            if self.should_emit_aggregated_data(symbol):
                logger.debug(f"Emitting Aggregated data for {symbol}")
                aggregated_record = self.emit_aggregated_data(symbol)
                self.process_batch(aggregated_record)

    def should_emit_aggregated_data(self, symbol):
        """
        Checks if the aggregated data for the symbol should be emitted based on the resolution.
        """
        # Ensure both times are datetime objects
        start_time = self.aggregated_data[symbol]["start_time"]
        end_time = self.aggregated_data[symbol]["end_time"]

        # Calculate the time difference in minutes
        try:
            time_diff = (end_time - start_time).total_seconds() / 60
            return time_diff >= self.aggregation_resolution
        except Exception as e:
            logger.error(
                f"Error calculating time difference: {e}, start_time: {start_time}, end_time: {end_time}"
            )
            return False

    def emit_aggregated_data(self, symbol):
        """
        Emits the aggregated data for the symbol and resets the aggregation.
        """
        aggregated_record = [{
            "symbol": symbol,
            "last_traded_time": self.aggregated_data[symbol][
                "last_traded_time"
            ],  # Last traded time
            "exch_feed_time": self.aggregated_data[symbol][
                "exch_feed_time"
            ],  # Exchange feed time
            "open_price": self.aggregated_data[symbol]["open_price"],
            "high_price": self.aggregated_data[symbol]["high_price"],
            "low_price": self.aggregated_data[symbol]["low_price"],
            "ltp": self.aggregated_data[symbol]["ltp"],
            "volume": self.aggregated_data[symbol][
                "vol_traded_today"
            ],  # Today's traded volume
            "bid_size": self.aggregated_data[symbol]["bid_size"],  # Bid size
            "ask_size": self.aggregated_data[symbol]["ask_size"],  # Ask size
            "bid_price": self.aggregated_data[symbol]["bid_price"],  # Bid price
            "ask_price": self.aggregated_data[symbol]["ask_price"],  # Ask price
            "last_traded_qty": self.aggregated_data[symbol][
                "last_traded_qty"
            ],  # Last traded quantity
            "tot_buy_qty": self.aggregated_data[symbol][
                "tot_buy_qty"
            ],  # Total buy quantity
            "tot_sell_qty": self.aggregated_data[symbol][
                "tot_sell_qty"
            ],  # Total sell quantity
            "avg_trade_price": self.aggregated_data[symbol][
                "avg_trade_price"
            ],  # Average trade price
            "ch": self.aggregated_data[symbol]["ch"],  # Price change
            "chp": self.aggregated_data[symbol]["chp"],  # Percentage price change
            "start_time": self.aggregated_data[symbol][
                "start_time"
            ],  # Start time of aggregation
            "end_time": self.aggregated_data[symbol][
                "end_time"
            ],  # End time of aggregation
            "lower_ckt": self.aggregated_data[symbol][
                "lower_ckt"
            ],  # Lower circuit price
            "upper_ckt": self.aggregated_data[symbol][
                "upper_ckt"
            ],  # Upper circuit price
        }]
        logger.debug(f"Emitting Aggregated data for {symbol}: {aggregated_record}")
        # Reset the aggregated data for the symbol
        del self.aggregated_data[symbol]
        return aggregated_record

    def process_batch(self, batch):
        """
        Processes a batch of data to generate signals and execute trades.
        """
        for data in batch:
            try:
                logger.debug(f"Processing data: {data}")
                if not isinstance(data, dict):
                    logger.error(
                        f"Invalid data format. Expected dict, got {type(data)}"
                    )
                    continue

                symbol = data.get("symbol")
                if not symbol:
                    logger.error(f"No symbol found in data: {data}")
                    continue

                if symbol not in self.signal_processors:
                    # Create a new processor instance
                    processor = self.get_or_create_signal_processor(symbol)
                    logger.debug(f"Created new signal processor for symbol {symbol}")

                # Process the bar with this processor
                try:
                    # Check if there is anything to be discarded due to force exits
                    logger.debug(
                        f"Risk Manager - {self.risk_manager.force_exit_triggered_symbols}"
                    )
                    if len(self.risk_manager.force_exit_triggered_symbols.keys()) > 0:
                        if (
                            symbol
                            in self.risk_manager.force_exit_triggered_symbols.keys()
                        ):
                            logger.info(
                                f"Force Exit was Triggered for this symbol ({symbol}), Any Previous signals for this  will be discarded."
                            )
                            self.signal_processors[symbol]._discard_signal()
                            self.signal_processors[symbol]._reset_position_tracking()
                    
                    # Check for incomplete entry trades and discard signal BEFORE running
                    incomplete_info = self.check_incomplete_trades(symbol)
                    if incomplete_info.get("entry"):
                        logger.debug(f"Discarding signal for {symbol} BEFORE run() - incomplete entry trade exists: {incomplete_info['entry']}")
                        try:
                            self.signal_processors[symbol]._discard_signal()
                            self.signal_processors[symbol]._reset_position_tracking()
                            logger.debug(f"Called _discard_signal() for {symbol} due to incomplete entry trade")
                        except Exception as e:
                            logger.error(f"Error calling _discard_signal() for {symbol}: {e}")
                        # Continue to next symbol, don't run signal generation
                        # continue
                    
                    results = self.signal_processors[symbol].run(data)
                    logger.debug(f"Signal results for {symbol}: {results}")
                    if results:
                        # Apply signal management logic for exit/take_profit signals only
                        processed_signal = self.apply_signal_management(symbol, results)
                        
                        if processed_signal:
                            if processed_signal["type"] in ['entry', 'exit', 'take_profit']:
                                logger.info(f"Putting signal for {symbol} to trade_signal_queue: {processed_signal}")
                                self.trade_signal_queue.put(processed_signal)
                            else:
                                logger.debug(f"Signal for {symbol} was not forwarded to trade_signal_queue because it is not a valid signal (entry, exit, take_profit)")
                            result_copy = processed_signal.copy()
                            if "trade_id" in result_copy:
                                result_copy.pop("trade_id")
                            self.db_handler.insert_records(StockSignals(**result_copy))

                            # Begin latency logging similar to Driver
                            current_ts = time.time()
                            if not hasattr(self, "_last_signal_time_print"):
                                self._last_signal_time_print = current_ts
                            # Log every 30 seconds
                            if current_ts - getattr(self, "_last_signal_time_print", 0) >= 30:
                                # Extract timestamps
                                data_time_val = (
                                    data.get("last_traded_time")
                                    or data.get("last_traded_time")
                                    or data.get("time")
                                    or "Unknown"
                                )
                                sig_time_val = processed_signal.get("time", "Unknown")

                                # Helper to format datetime-like values
                                def _fmt(ts_val):
                                    if hasattr(ts_val, "strftime"):
                                        return ts_val.strftime("%Y-%m-%d %H:%M:%S")
                                    elif isinstance(ts_val, (int, float)):
                                        # Convert epoch timestamp to readable datetime
                                        try:
                                            dt = datetime.fromtimestamp(ts_val, tz=IST).replace(tzinfo=None)
                                            return dt.strftime("%Y-%m-%d %H:%M:%S")
                                        except (ValueError, OSError):
                                            return f"Invalid timestamp: {ts_val}"
                                    return str(ts_val)

                                logger.info(
                                    f"📅 [MarketData] Data Time: {_fmt(data_time_val)} | "
                                    f"Signal Time: {_fmt(sig_time_val)} | "
                                    f"System Time: {time.strftime('%Y-%m-%d %H:%M:%S')}"
                                )
                                self._last_signal_time_print = current_ts
                            # End latency logging

                        else:
                            logger.debug(f"Signal for {symbol} was discarded due to incomplete trade management")
                except Exception as e:
                    logger.error(
                        f"Error processing bar for {symbol}: {str(e)}", exc_info=True
                    )
                    continue

            except Exception as e:
                logger.error(f"Error in batch processing: {str(e)}")
                continue

    def apply_signal_management(self, symbol, signal):
        """
        Apply signal management logic based on incomplete trades
        Note: Entry signal discarding is handled before run() is called
        
        Args:
            symbol: The symbol
            signal: The generated signal
            
        Returns:
            dict: The processed signal or None if discarded
        """
        if not signal:
            return None
            
        signal_type = signal.get("type")
        
        # Entry signals are already handled before run() is called, so just pass through
        if signal_type == "entry":
            return signal
            
        # For exit and take_profit signals, handle merging with incomplete trades
        elif signal_type in ["exit", "take_profit"]:
            return self.handle_exit_or_takeprofit_signal(symbol, signal)
            
        # For other signal types, pass through
        return signal

    def stop(self):
        """Stops the processing loop and cleans up resources."""
        self.running = False
        if hasattr(self, "engine"):
            self.engine.dispose()
        if hasattr(self, "session"):
            self.session.close()
        logger.debug("MarketDataHandler stopped.")

    def _safe_timestamp_conversion(self, timestamp):
        """Helper method to safely convert various timestamp formats to datetime objects"""
        if isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp, tz=IST).replace(tzinfo=None)
        elif isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp)
                if dt.tzinfo is not None:
                    dt = dt.astimezone(IST).replace(tzinfo=None)
                return dt
            except ValueError:
                logger.warning(
                    f"Could not parse timestamp: {timestamp}, using current time"
                )
                return datetime.now(tz=IST).replace(tzinfo=None)
        else:
            # If it's already a datetime object or something else, return as is
            return timestamp


# --- Worker Process Definition ---
class Worker(Process):
    """
    Worker process that handles a subset of symbols.
    """

    def __init__(
        self,
        worker_id,
        symbols,
        in_queue,
        trade_signal_queue,
        config,
        margin_dict,
        capital_manager,
        trade_manager,
        shared_state,
        shared_lock,
        historical_data_cache = ".cache/data"
    ):
        super().__init__()
        self.worker_id = worker_id
        self.symbols = symbols
        self.in_queue = in_queue
        self.trade_signal_queue = trade_signal_queue
        self.config = config
        self.margin_dict = margin_dict
        self.risk_manager = capital_manager
        self.trade_manager = (
            trade_manager  # This is safe as DB connection is lazy-loaded
        )
        self.md_handler = None
        self.lock = Lock()
        self.shared_state = shared_state
        self.shared_lock = shared_lock
        self.historical_data_cache = historical_data_cache

        logger.debug(f"Worker {self.worker_id} initialized")

    def get_symbol_historical_data(self, symbol):
        """
        Get historical data for a given symbol, resolution, start date, and end date
        """
        if os.path.exists(f"{self.historical_data_cache}/{symbol.replace(':', '_').replace('-', '_')}.csv"):
            return pd.read_csv(f"{self.historical_data_cache}/{symbol.replace(':', '_').replace('-', '_')}.csv")
        else:
            logger.warning(f"No historical data found for {symbol}")
            return None
    
    def get_historical_data(self):

        # get the historical data for all symbols in one dataframe
        historical_data = pd.DataFrame()
        for symbol in self.symbols:
            symbol_historical_data = self.get_symbol_historical_data(symbol)
            if symbol_historical_data is not None:
                historical_data = pd.concat([historical_data, symbol_historical_data])
            else:
                logger.warning(f"No historical data found for {symbol}")
        return historical_data

    def print_shared_state_detailed(self):
        """
        Print detailed shared state information in a formatted way
        """
        try:
            with self.shared_lock:
                shared_state_copy = dict(self.shared_state)
            
            # Create the formatted output
            separator = "=" * 80
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            header = f"[Worker {self.worker_id}] SHARED STATE SNAPSHOT - {timestamp}"
            
            # Start building the output
            output_lines = [
                "",
                separator,
                header,
                separator
            ]
            
            for key, value in shared_state_copy.items():
                output_lines.append(f"\n📊 {key.upper()}:")
                output_lines.append(f"   Type: {type(value).__name__}")
                
                if isinstance(value, dict):
                    if len(value) == 0:
                        output_lines.append("   Value: {} (empty)")
                    else:
                        output_lines.append(f"   Count: {len(value)} items")
                        # Print first few items for dictionaries
                        for i, (sub_key, sub_value) in enumerate(value.items()):
                            if i < 5:  # Show first 5 items
                                if isinstance(sub_value, dict):
                                    output_lines.append(f"   - {sub_key}: {json.dumps(sub_value, indent=6, default=str)}")
                                else:
                                    output_lines.append(f"   - {sub_key}: {sub_value}")
                            elif i == 5:
                                output_lines.append(f"   ... and {len(value) - 5} more items")
                                break
                elif isinstance(value, (list, tuple)):
                    output_lines.append(f"   Count: {len(value)} items")
                    if len(value) > 0:
                        output_lines.append(f"   Sample: {value[:3]}{'...' if len(value) > 3 else ''}")
                else:
                    output_lines.append(f"   Value: {value}")
            
            # Special handling for incomplete trades to show more details
            incomplete_trade_keys = ["incomplete_entry_trades", "incomplete_exit_trades", "incomplete_takeprofit_trades"]
            for incomplete_key in incomplete_trade_keys:
                if incomplete_key in shared_state_copy and shared_state_copy[incomplete_key]:
                    output_lines.append(f"\n🚨 {incomplete_key.upper()} DETAILS:")
                    for symbol, trade_data in shared_state_copy[incomplete_key].items():
                        output_lines.append(f"   Symbol: {symbol}")
                        output_lines.append(f"   - Reason: {trade_data.get('reason', 'Unknown')}")
                        output_lines.append(f"   - Attempts: {trade_data.get('attempt_count', 0)}")
                        output_lines.append(f"   - Timestamp: {trade_data.get('timestamp', 'Unknown')}")
                        output_lines.append(f"   - Order ID: {trade_data.get('order_id', 'None')}")
                        signal_type = trade_data.get('signal', {}).get('type', 'Unknown')
                        output_lines.append(f"   - Signal Type: {signal_type}")
            
            output_lines.extend([
                "",
                separator,
                f"[Worker {self.worker_id}] END SHARED STATE SNAPSHOT",
                separator,
                ""
            ])
            
            # Print to console
            # for line in output_lines:
            #     print(line)
            
            # Also send to logger
            full_output = "\n".join(output_lines)
            logger.debug(f"SHARED STATE DETAILED SNAPSHOT:\n{full_output}")
            
            # Additionally log a summary for easier parsing
            summary_info = {
                "worker_id": self.worker_id,
                "timestamp": timestamp,
                "shared_state_keys": list(shared_state_copy.keys()),
                "summary": {}
            }
            
            for key, value in shared_state_copy.items():
                if isinstance(value, dict):
                    summary_info["summary"][key] = {"type": "dict", "count": len(value)}
                elif isinstance(value, (list, tuple)):
                    summary_info["summary"][key] = {"type": type(value).__name__, "count": len(value)}
                else:
                    summary_info["summary"][key] = {"type": type(value).__name__, "value": str(value)}
            
            logger.debug(f"SHARED STATE SUMMARY: {json.dumps(summary_info, indent=2, default=str)}")
            
        except Exception as e:
            error_msg = f"Error printing shared state in worker {self.worker_id}: {str(e)}"
            print(error_msg)
            logger.error(error_msg, exc_info=True)

    def _load_strategy_class_from_string(self, strategy_string):
        """
        Load strategy class from string specification.
        
        Args:
            strategy_string: String like 'system.strategy.kama.KAMATrendFollowingStrategy'
            
        Returns:
            Strategy class
        """
        try:
            # Split the string into module path and class name
            module_path, class_name = strategy_string.rsplit('.', 1)
            
            # Import the module
            module = importlib.import_module(module_path)
            
            # Get the class
            strategy_class = getattr(module, class_name)
            
            return strategy_class
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error loading strategy class '{strategy_string}': {e}")
            raise

    def _get_strategy_for_symbol(self, symbol):
        """
        Get the appropriate strategy class for a given symbol based on config.
        
        Args:
            symbol: The symbol to get strategy for
            
        Returns:
            Strategy class
        """
        try:
            # Simple case: strategy_class directly specified in config (like backtest_config.yaml)
            if 'strategy_class' in self.config:
                strategy_string = self.config['strategy_class']
                if isinstance(strategy_string, str):
                    return self._load_strategy_class_from_string(strategy_string)
                else:
                    # Already a class
                    return strategy_string
            
            # Complex case: strategy_config with assignment rules (like config.yaml)
            elif 'strategy_config' in self.config:
                strategy_config = self.config['strategy_config']
                
                # Check for direct symbol override first
                stocks_config = strategy_config.get('stocks', {})
                if symbol in stocks_config and 'strategy' in stocks_config[symbol]:
                    strategy_string = stocks_config[symbol]['strategy']
                    # Check if it's already a full path or just a class name
                    if '.' in strategy_string:
                        return self._load_strategy_class_from_string(strategy_string)
                    else:
                        # Legacy format - construct full path
                        strategy_string = f"system.strategy.{strategy_string.lower().replace('strategy', '')}.{strategy_string}"
                        return self._load_strategy_class_from_string(strategy_string)
                
                # Apply assignment rules if enabled
                assignment_rules = strategy_config.get('assignment_rules', [])
                for rule_group in assignment_rules:
                    if not rule_group.get('enabled', False):
                        continue
                        
                    rules = rule_group.get('rules', [])
                    for rule in rules:
                        condition = rule.get('condition', {})
                        
                        # Check symbol-based condition
                        if 'symbol' in condition and condition['symbol'] == symbol:
                            strategy_string = rule['strategy']
                            # Check if it's already a full path or just a class name
                            if '.' in strategy_string:
                                return self._load_strategy_class_from_string(strategy_string)
                            else:
                                # Legacy format - construct full path
                                strategy_string = f"system.strategy.{strategy_string.lower().replace('strategy', '')}.{strategy_string}"
                                return self._load_strategy_class_from_string(strategy_string)
                        
                        # Check market_cap-based condition
                        if 'market_cap' in condition and symbol in stocks_config:
                            symbol_market_cap = stocks_config[symbol].get('market_cap')
                            if symbol_market_cap == condition['market_cap']:
                                strategy_string = rule['strategy']
                                # Check if it's already a full path or just a class name
                                if '.' in strategy_string:
                                    return self._load_strategy_class_from_string(strategy_string)
                                else:
                                    # Legacy format - construct full path
                                    strategy_string = f"system.strategy.{strategy_string.lower().replace('strategy', '')}.{strategy_string}"
                                    return self._load_strategy_class_from_string(strategy_string)
                        
                        # Check sector-based condition
                        if 'sector' in condition and symbol in stocks_config:
                            symbol_sector = stocks_config[symbol].get('sector')
                            if symbol_sector == condition['sector']:
                                strategy_string = rule['strategy']
                                # Check if it's already a full path or just a class name
                                if '.' in strategy_string:
                                    return self._load_strategy_class_from_string(strategy_string)
                                else:
                                    # Legacy format - construct full path
                                    strategy_string = f"system.strategy.{strategy_string.lower().replace('strategy', '')}.{strategy_string}"
                                    return self._load_strategy_class_from_string(strategy_string)
                
                # Use default strategy if no rules matched
                default_strategy = strategy_config.get('default_strategy', 'system.strategy.kama.KAMATrendFollowingStrategy')
                # Check if it's already a full path or just a class name
                if '.' in default_strategy:
                    return self._load_strategy_class_from_string(default_strategy)
                else:
                    # Legacy format - construct full path
                    strategy_string = f"system.strategy.{default_strategy.lower().replace('strategy', '')}.{default_strategy}"
                    return self._load_strategy_class_from_string(strategy_string)
            
            # Fallback: use hardcoded default
            logger.warning(f"Worker {self.worker_id}: No strategy configuration found, using default KAMATrendFollowingStrategy")
            from strategy.kama import KAMATrendFollowingStrategy
            return KAMATrendFollowingStrategy
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error determining strategy for {symbol}: {e}")
            # Emergency fallback
            from strategy.kama import KAMATrendFollowingStrategy
            return KAMATrendFollowingStrategy

    def get_or_create_signal_processor(self, symbol):
        """
        Get or create a signal processor (strategy instance) for a given symbol.
        Now supports dynamic strategy loading based on configuration.
        
        Args:
            symbol: The symbol to get/create strategy for
            
        Returns:
            Strategy instance for the symbol
        """
        try:
            # Get the appropriate strategy class for this symbol
            strategy_class = self._get_strategy_for_symbol(symbol)
            
            # Create strategy instance with appropriate parameters
            processor = strategy_class(
                use_history=True
            )
            
            logger.debug(f"Worker {self.worker_id}: Created {strategy_class.__name__} instance for {symbol}")
            return processor
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Failed to create signal processor for {symbol}: {e}")
            # Emergency fallback
            try:
                from strategy.kama import KAMATrendFollowingStrategy
                processor = KAMATrendFollowingStrategy(use_history=True)
                logger.warning(f"Worker {self.worker_id}: Emergency fallback to KAMATrendFollowingStrategy for {symbol}")
                return processor
            except Exception as fallback_error:
                logger.critical(f"Worker {self.worker_id}: Complete failure to create strategy for {symbol}: {fallback_error}")
                raise

    def run(self):
        try:
            print(f"[Worker {self.worker_id}] Starting. Symbols: {self.symbols}")

            # Create a base signal processor using configuration - just use the first symbol
            if self.symbols:
                base_signal_processor = self.get_or_create_signal_processor(self.symbols[0])
            else:
                # Fallback if no symbols assigned
                base_signal_processor = None
                
            # Instantiate MarketDataHandler to process incoming bars
            self.md_handler = MarketDataHandler(
                signal_processor=base_signal_processor,
                aggregation_resolution=self.config['trading_setting']['aggregation_resolution'],
                risk_manager=self.risk_manager,
                shared_state=self.shared_state,
                shared_lock=self.shared_lock,
                config=self.config,
                historical_data=self.get_historical_data()
            )
            logger.debug(f"MarketDataHandler initialized")
            # Pass the shared trade signal queue to the handler
            self.md_handler.trade_signal_queue = self.trade_signal_queue

            # Start the MarketDataHandler's processing loop in a daemon thread
            Thread(target=self.md_handler.process_queue, daemon=True).start()

            # Initialize monitoring variables
            last_shared_state_print = time.time()
            shared_state_print_interval = 30  # Print every 30 seconds
            iteration_count = 0

            # Main loop: fetch data from the input queue and feed it to the handler
            while True:
                iteration_count += 1
                current_time = time.time()
                
                # Print shared state periodically
                if current_time - last_shared_state_print >= shared_state_print_interval:
                    if os.environ["DEBUG_LEVEL"] == "DEBUG":
                        # self.print_shared_state_detailed()
                        pass
                    last_shared_state_print = current_time
                
                # Also print brief shared state info every 100 iterations for debugging
                if iteration_count % 100 == 0:
                    with self.shared_lock:
                        shared_state_keys = list(self.shared_state.keys())
                    logger.debug(f"Worker {self.worker_id} - Iteration {iteration_count} - Shared State Keys: {shared_state_keys}")
                
                try:
                    data = self.in_queue.get(timeout=5)
                    if data.get("symbol") in self.symbols:
                        self.md_handler.data_queue.put(data)
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Error in worker {self.worker_id}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Fatal error in worker {self.worker_id}: {str(e)}")
            raise

    def terminate(self):
        """Clean up resources before terminating"""
        try:
            if self.md_handler:
                self.md_handler.stop()
            if hasattr(self.trade_manager, "engine") and self.trade_manager.engine:
                self.trade_manager.engine.dispose()
            if hasattr(self.trade_manager, "session") and self.trade_manager.session:
                self.trade_manager.session.close()
        except Exception as e:
            logger.error(f"Error during worker {self.worker_id} cleanup: {str(e)}")
        finally:
            super().terminate()
