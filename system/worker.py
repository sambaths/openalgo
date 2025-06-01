import queue
from multiprocessing import Process, Lock
import logging, os
from logger import logger

logger.setLevel(getattr(logging, os.environ["DEBUG_LEVEL"].upper(), None))
from threading import Thread
from datetime import datetime

from db import setup, DBHandler, MarketData, StockSignals
from strategy.trendscore import EnhancedTrendScoreStrategy


CurrentStrategy = EnhancedTrendScoreStrategy


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
    """

    def __init__(
        self, signal_processor=None, aggregation_resolution=None, risk_manager=None
    ):
        logger.info("Initializing MarketDataHandler.")
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
        self.signal_processors = {}  # Will create processors per symbol
        self.risk_manager = risk_manager
        logger.info("MarketDataHandler initialized.")

    def process_queue(self):
        """
        Continuously processes the data queue.
        """
        logger.info("MarketDataHandler process_queue started.")
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
                    if self.aggregation_resolution and not any(
                        data["symbol"] in self.risk_manager.active_trade_symbols
                        for data in batch
                    ):
                        self.aggregate_data_and_process_batch(batch)
                    else:
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
        self.db_handler.insert_records(
            records=[MarketData(**records) for records in batch]
        )
        for data in batch:
            symbol = data["symbol"]

            # Convert Unix timestamp to a datetime object - ensure it's a numeric value first
            timestamp = data["last_traded_time"]
            if isinstance(timestamp, (int, float)):
                last_traded_time = datetime.fromtimestamp(timestamp)
            else:
                # If it's already a datetime or string, try to handle it appropriately
                if isinstance(timestamp, str):
                    try:
                        last_traded_time = datetime.fromisoformat(timestamp)
                    except ValueError:
                        # If string format is unknown, log and use current time as fallback
                        logger.warning(
                            f"Could not parse timestamp: {timestamp}, using current time"
                        )
                        last_traded_time = datetime.now()
                else:
                    # If it's already a datetime object, use it directly
                    last_traded_time = timestamp

            # Initialize aggregation if not already done
            if symbol not in self.aggregated_data:
                self.aggregated_data[symbol] = {
                    "open_price": data[
                        "ltp"
                    ],  # Opening price is the first price in the period
                    "high_price": data[
                        "ltp"
                    ],  # High price starts as the last traded price
                    "low_price": data[
                        "ltp"
                    ],  # Low price starts as the last traded price
                    "close_price": data[
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
                    self.aggregated_data[symbol]["high_price"], data["ltp"]
                )
                self.aggregated_data[symbol]["low_price"] = min(
                    self.aggregated_data[symbol]["low_price"], data["ltp"]
                )
                self.aggregated_data[symbol]["close_price"] = data[
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
                    self.aggregated_data[symbol]["close_price"]
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
        aggregated_record = {
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
            "close_price": self.aggregated_data[symbol]["close_price"],
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
        }

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
                    processor = CurrentStrategy  # Create a fresh instance
                    self.signal_processors[symbol] = processor(symbol=symbol)
                    logger.info(f"Created new signal processor for symbol {symbol}")

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

                    results = self.signal_processors[symbol].run(data)
                    logger.debug(f"Signal results for {symbol}: {results}")
                    if results:
                        self.trade_signal_queue.put(results)
                        result_copy = results.copy()
                        if "trade_id" in result_copy:
                            result_copy.pop("trade_id")
                        self.db_handler.insert_records(StockSignals(**result_copy))
                except Exception as e:
                    logger.error(
                        f"Error processing bar for {symbol}: {str(e)}", exc_info=True
                    )
                    continue

            except Exception as e:
                logger.error(f"Error in batch processing: {str(e)}")
                continue

    def stop(self):
        """Stops the processing loop and cleans up resources."""
        self.running = False
        if hasattr(self, "engine"):
            self.engine.dispose()
        if hasattr(self, "session"):
            self.session.close()
        logger.info("MarketDataHandler stopped.")

    def _safe_timestamp_conversion(self, timestamp):
        """Helper method to safely convert various timestamp formats to datetime objects"""
        if isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp)
        elif isinstance(timestamp, str):
            try:
                return datetime.fromisoformat(timestamp)
            except ValueError:
                logger.warning(
                    f"Could not parse timestamp: {timestamp}, using current time"
                )
                return datetime.now()
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
        logger.info(f"Worker {self.worker_id} initialized")

    def run(self):
        try:
            print(f"[Worker {self.worker_id}] Starting. Symbols: {self.symbols}")

            # Create a base signal processor using configuration
            base_signal_processor = CurrentStrategy
            # Instantiate MarketDataHandler to process incoming bars
            self.md_handler = MarketDataHandler(
                signal_processor=base_signal_processor,
                aggregation_resolution=None,
                risk_manager=self.risk_manager,
            )
            logger.info(f"MarketDataHandler initialized")
            # Pass the shared trade signal queue to the handler
            self.md_handler.trade_signal_queue = self.trade_signal_queue

            # Start the MarketDataHandler's processing loop in a daemon thread
            Thread(target=self.md_handler.process_queue, daemon=True).start()

            # Main loop: fetch data from the input queue and feed it to the handler
            while True:
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
