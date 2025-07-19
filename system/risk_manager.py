# import redis
import time

import time
import threading
import multiprocessing
from collections import OrderedDict
from logger import logger
from db import ActiveTrades, TradeLog, CapitalLog, setup, DBHandler


class RiskManager:
    """
    Global Risk Manager that calculates required risk parameters for trades and
    tracks capital, returns, and active trade state. It does NOT execute trades.

    External functions can:
      - Assess a potential trade (calculate position size, allocated capital, and eligibility)
      - Register a trade entry (to update active trades and update internal counters)
      - Register a trade exit (to update total capital and global returns)

    This class is implemented as a per-process singleton and is thread-safe.
    For multi-process coordination, an external shared resource is recommended.
    """

    # _instance = None
    # _instance_lock = threading.Lock()

    # def __new__(cls, *args, **kwargs):
    #     if cls._instance is None:
    #         with cls._instance_lock:
    #             if cls._instance is None:
    #                 cls._instance = super(GlobalRiskManager, cls).__new__(cls)
    #     return cls._instance

    def __init__(
        self,
        total_capital: float,
        trade_counter: int,
        risk_per_trade: float,
        max_allocation: float,
        max_active_trades: int,
        config: dict,
        margin_dict: dict,
    ):
        """
        Initialize the Global Risk Manager.

        Args:
            total_capital (float): Starting and current capital.
            trade_counter (int): Initial count of trades.
            risk_per_trade (float): Fraction of total capital risked per trade.
            max_allocation (float): Maximum fraction of total capital to allocate per trade.
            max_active_trades (int): Maximum number of concurrently active trades.
            trade_log_db: Interface to the trade log database (for tracking purposes).
        """
        # Prevent reinitialization if already set.
        if hasattr(self, "_initialized") and self._initialized:
            return
        # self.engine, self.session = setup()
        # self.db_handler = DBHelper(self.engine)
        self.config = config
        self.total_capital = total_capital
        self.risk_per_trade = risk_per_trade
        self.trade_counter = trade_counter
        self.max_allocation = max_allocation * total_capital  # Absolute max allocation.
        self.max_active_trades = max_active_trades
        self.start_capital = total_capital
        self.active_trades = OrderedDict()  # Maps trade_id -> trade info.
        self.lock = multiprocessing.Lock()
        self.margin_dict = margin_dict  # Local mapping: symbol -> margin multiplier.
        self.most_recent_exit_trade_id = 0
        self.global_returns = 0.0
        self.halt_trading = False
        self.halt_trading_reason = "Trading in Progress"
        self.active_trade_symbols = set()
        self._initialized = True
        self.max_trade_id = None
        self.price_tracker = {}
        self.force_exit_triggered_symbols = {}
        self.incremented_trade_id = None

        logger.debug(
            f"{self.__class__.__name__} initialized with total_capital={self.total_capital}, "
            f"risk_per_trade={self.risk_per_trade}, max_allocation={self.max_allocation}, "
            f"max_active_trades={self.max_active_trades}, margin_dict={self.margin_dict}"
        )

    def initialize_db(self):
        engine, session = setup()
        db_handler = DBHandler(engine)
        if self.max_trade_id is None:
            self.max_trade_id = db_handler.get_max_trade_id(TradeLog)
            logger.info(
                f"{self.__class__.__name__}: Max trade id is {self.max_trade_id}"
            )
        return db_handler

    @property
    def available_capital(self) -> float:
        """
        Returns the available capital (total_capital minus the capital allocated to active trades).
        """
        used_capital = sum(trade["allocated"] for trade in self.active_trades.values())
        available = self.total_capital - used_capital
        logger.debug(f"{self.__class__.__name__}: Available capital is {available}")
        return available

    def calculate_position_size(
        self, entry_price: float, stop_loss: float, margin_val: float, symbol: str
    ) -> int:
        """
        Calculate the number of units to trade based on risk parameters.

        1. Determine risk amount = total_capital * risk_per_trade.
        2. Compute price risk (abs(entry_price - stop_loss)).
        3. Adjust entry price based on margin.
        4. Determine units from risk, affordability, and maximum allocation.

        Args:
            entry_price (float): Price at entry.
            stop_loss (float): Price for stop loss.
            margin_val (float): Margin multiplier (default 1 if not set).
            symbol (str): Trading symbol.

        Returns:
            int: Calculated number of units.
        """
        avail = self.total_capital  # Use total capital for risk calculation.
        risk_amount = avail * self.risk_per_trade
        price_risk = abs(entry_price - stop_loss)
        margin_discount_price = entry_price / margin_val if margin_val else entry_price
        if price_risk <= 0:
            logger.debug(
                f"{self.__class__.__name__}: Non-positive price risk for {symbol}; returning 0 units."
            )
            return 0
        units_risk = risk_amount / price_risk
        units_afford = self.available_capital / margin_discount_price
        raw_units = min(units_risk, units_afford)
        max_units_cap = self.max_allocation / margin_discount_price
        final_units = min(raw_units, max_units_cap)
        logger.debug(
            f"{self.__class__.__name__}: For {symbol}, calculated units: {int(final_units)} "
            f"(units_risk={units_risk}, units_afford={units_afford}, max_units_cap={max_units_cap})"
        )
        return int(final_units)

    def calculate_position_size_detailed(
        self, entry_price: float, stop_loss: float, margin_val: float, symbol: str
    ) -> int:
        """
        Calculate the number of units to trade based on risk parameters.

        1. Determine risk amount = total_capital * risk_per_trade.
        2. Compute price risk (abs(entry_price - stop_loss)).
        3. Adjust entry price based on margin.
        4. Determine units from risk, affordability, and maximum allocation.

        Args:
            entry_price (float): Price at entry.
            stop_loss (float): Price for stop loss.
            margin_val (float): Margin multiplier (default 1 if not set).
            symbol (str): Trading symbol.

        Returns:
            int: Calculated number of units.
        """
        avail = self.total_capital  # Use total capital for risk calculation.
        risk_amount = avail * self.risk_per_trade
        price_risk = abs(entry_price - stop_loss)
        margin_discount_price = entry_price / margin_val if margin_val else entry_price
        if price_risk <= 0:
            logger.debug(
                f"{self.__class__.__name__}: Non-positive price risk for {symbol}; returning 0 units."
            )
            return 0
        units_risk = risk_amount / price_risk
        units_afford = self.available_capital / margin_discount_price
        raw_units = min(units_risk, units_afford)
        max_units_cap = self.max_allocation / margin_discount_price
        final_units = min(raw_units, max_units_cap)
        logger.debug(
            f"{self.__class__.__name__}: For {symbol}, calculated units: {int(final_units)} "
            f"(units_risk={units_risk}, units_afford={units_afford}, max_units_cap={max_units_cap})"
        )
        return int(final_units), int(units_risk), int(units_afford), int(max_units_cap)

    def assess_trade(self, symbol: str, entry_price: float, stop_loss: float) -> dict:
        """
        Assess a potential trade by calculating the position size and allocated capital.

        This method checks if the trade meets risk management requirements (e.g.,
        sufficient available capital, minimum trade size, etc.) and returns a dictionary
        with the calculated parameters and a flag indicating trade eligibility.

        Args:
            symbol (str): Trading symbol.
            entry_price (float): Proposed entry price.
            stop_loss (float): Proposed stop loss price.

        Returns:
            dict: Contains keys 'eligible' (bool), 'units' (int), 'allocated' (float),
                  and 'reason' (str) if not eligible.
        """
        with self.lock:
            margin_val = self.margin_dict.get(symbol, 1)
            logger.debug(f"{self.__class__.__name__}: Margin value for {symbol} is {margin_val}")
            units, units_risk, units_afford, max_units_cap = (
                self.calculate_position_size_detailed(
                    entry_price, stop_loss, margin_val, symbol
                )
            )
            allocated = (
                (entry_price * units) / margin_val
                if margin_val
                else entry_price * units
            )

            # Check Gloabl Eligibility Criteria
            if self.config["risk_management"]["max_loss_cap"] is not None:
                # returns = self.get_current_global_returns()
                returns = self.global_returns
                if returns < self.config["risk_management"]["max_loss_cap"]:
                    reason = f"Max Loss for the Day Achieved. Stopping Trading - Current Loss - {returns}, Max Loss Threshold - {self.config['risk_management']['max_loss_cap']}"
                    logger.info(
                        f"{self.__class__.__name__}: Trade for {symbol} not eligible: {reason}"
                    )
                    return {
                        "eligible": False,
                        "units": 0,
                        "allocated": allocated,
                        "reason": reason,
                    }
            if self.config["risk_management"]["max_profit_cap"] is not None:
                returns = self.get_current_global_returns()
                if returns > self.config["risk_management"]["max_profit_cap"]:
                    reason = f"Max Profit for the Day Achieved. Stopping Trading - Current Profit - {returns}, Max Profit Threshold - {self.config['risk_management']['max_profit_cap']}"
                    logger.info(
                        f"{self.__class__.__name__}: Trade for {symbol} not eligible: {reason}"
                    )
                    return {
                        "eligible": False,
                        "units": 0,
                        "allocated": allocated,
                        "reason": reason,
                    }
            if self.config["risk_management"]["max_symbol_price"] is not None:
                if entry_price > self.config["risk_management"]["max_symbol_price"]:
                    reason = f"Entry Price is too high for {symbol}. Entry Price - {entry_price}, Max Symbol Price - {self.config['risk_management']['max_symbol_price']}"
                    logger.info(
                        f"{self.__class__.__name__}: Trade for {symbol} not eligible: {reason}"
                    )
                    return {
                        "eligible": False,
                        "units": 0,
                        "allocated": allocated,
                        "reason": reason,
                    }
            
            # Check eligibility criteria.
            if units <= 0 or allocated > self.available_capital:
                detail = ""
                if units_afford > max_units_cap:
                    detail = "Maximum Units Allowed is less than the Units that can be afforded (One Possible reason is that the Price per unit is too high for this stock and with the available capital, we cannot afford even 1 unit)"
                reason = f"Insufficient capital: Required ₹{allocated:.2f}, Available ₹{self.available_capital:.2f}, {detail}"
                logger.info(
                    f"{self.__class__.__name__}: Trade for {symbol} not eligible: {reason}"
                )
                return {
                    "eligible": False,
                    "units": 0,
                    "allocated": allocated,
                    "reason": reason,
                }

            half_units_cost = (entry_price * (units / 2)) / margin_val
            if self.available_capital < half_units_cost:
                reason = f"Not enough capital for at least half the position: Needed ₹{half_units_cost:.2f}"
                logger.info(
                    f"{self.__class__.__name__}: Trade for {symbol} not eligible: {reason}"
                )
                return {
                    "eligible": False,
                    "units": units,
                    "allocated": allocated,
                    "reason": reason,
                }

            if allocated  < 0.05 * self.total_capital:
                reason = f"Trade too small: Allocated ₹{allocated:.2f} is less than 5% of total capital"
                logger.info(
                    f"{self.__class__.__name__}: Trade for {symbol} not eligible: {reason}"
                )
                return {
                    "eligible": False,
                    "units": units,
                    "allocated": allocated,
                    "reason": reason,
                }

            # If all checks pass, return calculated values.
            logger.info(
                f"{self.__class__.__name__}: Trade for {symbol} eligible with {units} units, allocated ₹{allocated:.2f}"
            )
            return {
                "eligible": True,
                "units": units,
                "allocated": allocated,
                "reason": "Trade Accepted",
            }

    def register_trade_entry(self, trade_record: dict) -> int:
        """
        Register a trade entry event after an external trade has been executed.
        This method assumes the trade has passed risk assessment and updates internal tracking.

        Args:
            trade_record (dict): Trade record.

        Returns:
            int: A generated trade ID for tracking.
        """
        db_handler = self.initialize_db()

        with self.lock:
            self.trade_counter += 1
            trade_id = max(self.most_recent_exit_trade_id, self.trade_counter)
            trade_record["trade_id"] = trade_id
            self.active_trades[trade_id] = trade_record
            logger.info(
                f"{self.__class__.__name__}: Registered trade entry {trade_id} for {trade_record['symbol']}: "
                f"{trade_record['position_size']} units, allocated ₹{trade_record['allocated']:.2f}"
            )
            print(
                f"{self.__class__.__name__}: Registered trade entry {trade_id} for {trade_record['symbol']}: "
                f"{trade_record['position_size']} units, allocated ₹{trade_record['allocated']:.2f}"
            )
            try:
                # Create a new ActiveTrades object
                active_trade = ActiveTrades(
                    trade_id=trade_record["trade_id"],
                    symbol=trade_record["symbol"],
                    entry_time=trade_record[
                        "entry_time"
                    ],  # Make sure this key exists in trade_record
                    entry_price=trade_record["entry_price"],
                    stop_loss=trade_record["stop_loss"],
                    position_size=trade_record["position_size"],
                    allocated=trade_record["allocated"],
                    margin_availed=trade_record["margin"],
                    take_profit_price=trade_record["take_profit_price"],
                    order_id=trade_record["order_id"],
                )
                # Insert the record
                db_handler.insert_records(active_trade)
            except Exception as e:
                logger.error(f"Failed to insert trade into ActiveTrades DB: {str(e)}")

            # Add symbol to active trade symbols set
            self.active_trade_symbols.add(trade_record["symbol"])

            return trade_id

    def register_full_trade_exit(self, trade_record: dict):
        """
        Register a trade exit event to update capital and returns.

        Args:
            trade_id (int): The ID of the trade to mark as exited.
            exit_price (float): The exit price achieved.
        """
        db_handler = self.initialize_db()
        with self.lock:
            if trade_record["trade_id"] not in self.active_trades:
                logger.error(
                    f"{self.__class__.__name__}: Attempted to register exit for non-existent trade {trade_record['trade_id']}."
                )
                return

            # Check if this trade_id already exists in TradeLog to prevent duplicates
            # Use both trade_id and trade_date since trade_id resets daily
            existing_trade_log = db_handler.query_records(
                TradeLog,
                criteria=(
                    (TradeLog.trade_id == trade_record["trade_id"]) & 
                    (TradeLog.trade_date == trade_record["entry_time"].date())
                ),
            )

            if existing_trade_log:
                error_msg = f"Trade {trade_record['trade_id']} for date {trade_record['entry_time'].date()} already exists in TradeLog. Cannot register exit twice."
                logger.error(f"{self.__class__.__name__}: {error_msg} - {trade_record}")
                return  # Return instead of raising exception to avoid crashing

            trade = self.active_trades.pop(trade_record["trade_id"])
            realised_pnl = (trade_record["exit_price"] - trade["entry_price"]) * (
                trade["position_size"] - trade["position_closed_tp"]
            )
            total_capital = (
                self.available_capital + realised_pnl
            )  # Update capital based on profit/loss.
            self.global_returns += realised_pnl
            self.most_recent_exit_trade_id = trade_record["trade_id"]
            logger.info(
                f"{self.__class__.__name__}: Registered trade exit {trade_record['trade_id']} for {trade_record['symbol']}: "
                f"PnL ₹{realised_pnl:.2f}, updated total_capital ₹{total_capital:.2f}"
            )
            print(
                f"{self.__class__.__name__}: Registered trade exit {trade_record['trade_id']} for {trade_record['symbol']}: "
                f"PnL ₹{realised_pnl:.2f}, updated total_capital ₹{total_capital:.2f}"
            )
            if trade_record["symbol"] in self.active_trade_symbols:
                self.active_trade_symbols.discard(trade_record["symbol"])

            # Delete from ActiveTrades and insert into TradeLog atomically
            try:
                db_handler.delete_records(
                    model=ActiveTrades,
                    criteria=(ActiveTrades.trade_id == trade_record["trade_id"]),
                )
                
                db_handler.insert_records(
                    TradeLog(
                        trade_id=self.max_trade_id + trade_record["trade_id"],
                        trade_date=trade_record["entry_time"].date(),
                        symbol=trade_record["symbol"],
                        entry_time=trade_record["entry_time"],
                        entry_price=trade_record["entry_price"],
                        exit_time=trade_record["exit_time"],
                        exit_price=trade_record["exit_price"],
                        capital_employed=trade_record["allocated"],
                        capital_at_exit=total_capital,
                        position_size=trade_record["position_size"],
                        pl=realised_pnl,
                        percent_change=realised_pnl / trade_record["allocated"],
                        capital_employed_pct=realised_pnl / self.total_capital,
                        global_profit=self.global_returns,
                        margin_availed=trade_record["margin"],
                        trade_type=trade_record["position_type"],
                        order_id=trade_record["order_id"],
                        instrument=trade_record["symbol"],
                    )
                )

                logger.debug(f"Successfully inserted trade {trade_record['trade_id']} into TradeLog")

                # Update or create CapitalLog entry for the current date
                trade_date = trade_record["exit_time"].date()
                existing_log = db_handler.query_records(
                    CapitalLog, criteria=(CapitalLog.log_date == trade_date)
                )

                if existing_log:
                    # Update existing log
                    db_handler.update_records(
                        CapitalLog,
                        existing_log[0].log_date,
                        {
                            "ending_capital": self.start_capital + self.global_returns,
                            "absolute_gain": self.global_returns,
                            "percent_gain": (self.global_returns / self.start_capital)
                            * 100,
                        },
                        id_column="log_date",  # Specify the correct ID column
                    )
                    logger.info(f"Updated capital log for {trade_date}")
                else:
                    # Create new log
                    capital_log = CapitalLog(
                        log_date=trade_date,
                        starting_capital=self.start_capital,
                        ending_capital=self.start_capital + self.global_returns,
                        absolute_gain=self.global_returns,
                        percent_gain=(self.global_returns / self.start_capital) * 100,
                        instrument="EQUITY",  # Default instrument type
                    )
                    db_handler.insert_records(capital_log)
                    logger.info(f"Created new capital log for {trade_date}")

            except Exception as e:
                logger.error(f"Failed to update trade records for trade {trade_record['trade_id']}: {str(e)}")
                # Re-add the trade back to active_trades if database operation failed
                self.active_trades[trade_record["trade_id"]] = trade
                self.global_returns -= realised_pnl
                self.active_trade_symbols.add(trade_record["symbol"])
                raise

    def register_partial_trade_exit(self, trade_record: dict):
        """
        Register a partial trade exit event to update capital and returns.

        Args:
            trade_id (int): The ID of the trade to mark as partially exited.
            exit_price (float): The exit price achieved.
            position_to_close (float): The position to close.
        """
        db_handler = self.initialize_db()
        with self.lock:
            if trade_record["trade_id"] not in self.active_trades:
                logger.error(
                    f"{self.__class__.__name__}: Attempted to register exit for non-existent trade {trade_record['trade_id']}."
                )
                return

            total_capital = self.available_capital + trade_record["realized_pnl_tp"]
            self.global_returns += trade_record["realized_pnl_tp"]
            self.active_trades[trade_record["trade_id"]] = trade_record
            # Need to update entry to ActiveTrades DB
            logger.info(
                f"{self.__class__.__name__}: Registered Partial Trade Exit {trade_record['trade_id']} for {trade_record['symbol']}: "
                f"PnL ₹{trade_record['realized_pnl_tp']:.2f}, updated total_capital ₹{total_capital:.2f}"
            )
            print(
                f"{self.__class__.__name__}: Registered Partial Trade Exit {trade_record['trade_id']} for {trade_record['symbol']}: "
                f"PnL ₹{trade_record['realized_pnl_tp']:.2f}, updated total_capital ₹{total_capital:.2f}"
            )
            db_handler.update_records(
                ActiveTrades,
                trade_record["trade_id"],
                {
                    "position_closed_tp": trade_record["position_closed_tp"],
                    "realized_pnl_tp": trade_record["realized_pnl_tp"],
                    "position_remaining_tp": trade_record["position_size"]
                    - trade_record["position_closed_tp"],
                    "stop_loss": trade_record["stop_loss"],
                },
            )

            # Update or create CapitalLog entry for the current date
            try:
                trade_date = trade_record.get(
                    "take_profit_time", time.localtime()
                ).date()
                existing_log = db_handler.query_records(
                    CapitalLog, criteria=(CapitalLog.log_date == trade_date)
                )

                if existing_log:
                    # Update existing log
                    db_handler.update_records(
                        CapitalLog,
                        existing_log[0].log_date,
                        {
                            "ending_capital": self.start_capital + self.global_returns,
                            "absolute_gain": self.global_returns,
                            "percent_gain": (self.global_returns / self.start_capital)
                            * 100,
                        },
                        id_column="log_date",  # Specify the correct ID column
                    )
                    logger.info(f"Updated capital log for {trade_date}")
                else:
                    # Create new log
                    capital_log = CapitalLog(
                        log_date=trade_date,
                        starting_capital=self.start_capital,
                        ending_capital=self.start_capital + self.global_returns,
                        absolute_gain=self.global_returns,
                        percent_gain=(self.global_returns / self.start_capital) * 100,
                        instrument="EQUITY",  # Default instrument type
                    )
                    db_handler.insert_records(capital_log)
                    logger.info(f"Created new capital log for {trade_date}")
            except Exception as e:
                logger.error(f"Failed to update capital log: {str(e)}")

    def halt_trading_check(self) -> (bool, str):
        """
        Check global returns against thresholds to decide if trading should be halted.

        Returns:
            tuple: (halt_trading (bool), halt_trading_reason (str))
        """
        with self.lock:
            if self.global_returns >= 0.05 * self.total_capital:
                self.halt_trading = True
                self.halt_trading_reason = "Target reached (5% return achieved)"
                logger.info(
                    f"{self.__class__.__name__}: Trading halted: {self.halt_trading_reason}"
                )
            elif self.global_returns <= -0.02 * self.total_capital:
                self.halt_trading = True
                self.halt_trading_reason = "Stop loss reached (2% loss achieved)"
                logger.info(
                    f"{self.__class__.__name__}: Trading halted: {self.halt_trading_reason}"
                )
            else:
                self.halt_trading = False
                self.halt_trading_reason = "Trading in Progress"
        return self.halt_trading, self.halt_trading_reason

    def track_prices(self, signal: dict):
        """
        Track Prices of the Active Trades
        """
        for trade_id, trade in self.active_trades.items():
            if signal["symbol"] == trade["symbol"]:
                self.price_tracker[trade["symbol"]] = signal["price"]

    def get_current_global_returns(self) -> float:
        """
        Calculate the current global returns, including:
        - Realized PnL from completed trades (self.global_returns)
        - Realized PnL from partial exits (realized_pnl_tp in active trades)
        - Unrealized PnL for active trades (based on current price)

        Args:
            current_prices (dict): Mapping from symbol to current price.

        Returns:
            float: The current global returns (realized + unrealized).
        """
        total_returns = self.global_returns  # Realized PnL from completed trades

        for trade_id, trade in self.active_trades.items():
            symbol = trade["symbol"]
            entry_price = trade["entry_price"]
            position_size = trade["position_size"]
            position_closed_tp = trade.get("position_closed_tp", 0)
            realized_pnl_tp = trade.get("realized_pnl_tp", 0)
            position_type = trade.get("position_type", "long")

            # Add realized PnL from partial exits
            total_returns += realized_pnl_tp

            # Calculate unrealized PnL for the remaining position
            current_price = self.price_tracker.get(symbol)
            if current_price is not None:
                open_position = position_size - position_closed_tp
                if open_position > 0:
                    if position_type == "long":
                        unrealized_pnl = (current_price - entry_price) * open_position
                    else:  # short
                        unrealized_pnl = (entry_price - current_price) * open_position
                    total_returns += unrealized_pnl
        return total_returns

    def get_current_returns_for_symbol(
        self, symbol: str, current_price: float
    ) -> float:
        """
        Get the current returns for a specific symbol based on the current price.
        Includes realized PnL from partial exits and unrealized PnL for the open position.

        Args:
            symbol (str): The trading symbol.
            current_price (float): The current price of the symbol.

        Returns:
            float: The current returns for the symbol (realized + unrealized).
        """
        for trade in self.active_trades.values():
            if trade["symbol"] == symbol:
                entry_price = trade["entry_price"]
                position_size = trade["position_size"]
                position_closed_tp = trade.get("position_closed_tp", 0)
                realized_pnl_tp = trade.get("realized_pnl_tp", 0)
                position_type = trade.get("position_type", "long")

                open_position = position_size - position_closed_tp
                total_returns = realized_pnl_tp
                if open_position > 0:
                    if position_type == "long":
                        unrealized_pnl = (current_price - entry_price) * open_position
                    else:  # short
                        unrealized_pnl = (entry_price - current_price) * open_position
                    total_returns += unrealized_pnl
                return total_returns
        return 0.0  # No active trade for this symbol
