import os, sys
from multiprocessing import Lock
from logger import logger
from datetime import datetime
from pytz import timezone

IST = timezone("Asia/Kolkata")
from openalgo import api

# For getting get_oa_symbol and get_br_symbol from openalgo directory
ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_PATH)

from database.token_db import get_oa_symbol, get_br_symbol


# --- Centralized Trade Manager ---
class TradeManager:
    """
    Processes trade signals from workers and executes trades while ensuring
    active trades remain below a configured threshold.
    """

    def __init__(
        self,
        risk_manager,
        trade_executor,
        max_active_trades,
        shared_state,
        config,
        margin_dict,
    ):
        self.risk_manager = risk_manager
        self.trade_executor = trade_executor
        self.max_active_trades = max_active_trades
        self.shared_state = shared_state
        self.TRADING_ACTIVE = True
        self.config = config
        self.margin_dict = margin_dict
        self.lock = Lock()
        self.slippage = config["trading_setting"]["slippage"]
        self.commission = config["trading_setting"]["commission"]
        self.trade_tracker = {}
        self.openalgo_client = api(
            api_key=os.getenv("APP_KEY"),
            host=os.getenv("HOST_SERVER", "http://127.0.0.1:5000"),
        )

        logger.info("TradeManager Initialized")

    def process_signal(self, signal):
        """Process a trade signal"""
        lock = self.lock
        logger.debug(f"TradeTracker: {self.trade_tracker}")
        try:
            # Track Prices for Getting Returns
            self.risk_manager.track_prices(signal)
            returns = self.risk_manager.get_current_global_returns()
            returns_stock = self.risk_manager.get_current_returns_for_symbol(
                symbol=signal["symbol"], current_price=signal["price"]
            )
            logger.info(
                f"Current Prices - {self.risk_manager.price_tracker}, Current Global Returns - {returns}, {signal['symbol']} Returns -  {returns_stock}"
            )

            # logger.info(f"Processing_signal: {signal}")
            if isinstance(signal["time"], str):
                signal["time"] = datetime.fromtimestamp(int(signal["time"]), tz=IST)
            if isinstance(signal["time"], int):
                signal["time"] = datetime.fromtimestamp(signal["time"], tz=IST)
            # signal['time'] = datetime.fromtimestamp(signal['time'], tz=IST)

            # Force Exit if the Global Returns are breaching threshold
            if self.config["risk_management"]["max_loss_cap_stock"] is not None:
                if (
                    returns_stock < self.config["risk_management"]["max_loss_cap_stock"]
                    and self.config["risk_management"]["force_exit_on_cap"]
                ):
                    logger.info(
                        f"Max Loss Threshold Breached - Force Exiting Position in {signal['symbol']}"
                    )
                    trade_record = self.risk_manager.active_trades.get(
                        self.trade_tracker.get(signal["symbol"], -1), {}
                    )
                    if len(trade_record.keys()) > 0:
                        signal["type"] = "exit"
                        signal["position_to_close"] = (
                            trade_record["position_size"]
                            - trade_record["position_closed_tp"]
                        )
                        signal["current_position"] = trade_record["position_type"]
                        with lock:
                            self.risk_manager.force_exit_triggered_symbols[
                                signal["symbol"]
                            ] = signal
            if self.config["risk_management"]["max_profit_cap_stock"] is not None:
                if (
                    returns_stock
                    > self.config["risk_management"]["max_profit_cap_stock"]
                    and self.config["risk_management"]["force_exit_on_cap"]
                ):
                    logger.info(
                        f"Max Profit Threshold Attained - Force Exiting Position in {signal['symbol']}"
                    )
                    trade_record = self.risk_manager.active_trades.get(
                        self.trade_tracker.get(signal["symbol"], -1), {}
                    )
                    if len(trade_record.keys()) > 0:
                        signal["type"] = "exit"
                        signal["position_to_close"] = (
                            trade_record["position_size"]
                            - trade_record["position_closed_tp"]
                        )
                        signal["current_position"] = trade_record["position_type"]
                        with lock:
                            self.risk_manager.force_exit_triggered_symbols[
                                signal["symbol"]
                            ] = signal

            if signal["type"]:
                # Convert the time to a datetime object
                if signal["current_position"] == "long":
                    # Log Entry Price, Entry Time, Position Size and Save the Trade as an Entry Trade
                    if signal["type"] == "entry":
                        # print(signal)
                        # Check if trading is halted
                        with lock:
                            self.halt_trading, self.halt_trading_reason = (
                                self.risk_manager.halt_trading_check()
                            )
                        if self.halt_trading:
                            logger.info(
                                f"Trading stopped. Reason: {self.halt_trading_reason}"
                            )
                        # Evaluate if we will take this trade or not
                        assessment = self.risk_manager.assess_trade(
                            signal["symbol"], signal["price"], signal["stop_loss"]
                        )
                        if not assessment["eligible"]:
                            logger.info(
                                f"{self.__class__.__name__}: Trade entry for {signal['symbol']} rejected: {assessment.get('reason')}"
                            )
                        else:
                            # Call Executor Here
                            position_size = self.risk_manager.calculate_position_size(
                                signal["price"],
                                signal["stop_loss"],
                                self.margin_dict.get(signal["symbol"], 1),
                                signal["symbol"],
                            )
                            signal["position_size"] = position_size

                            # Get some Trade Information first
                            self.symbol = signal["symbol"]
                            self.entry_price = signal["price"]
                            self.entry_time = signal["time"]
                            self.position_size = position_size
                            self.position_closed_tp = 0
                            self.realized_pnl_tp = 0
                            self.realized_pnl = 0
                            self.take_profit_price = signal["take_profit_price"]

                            # Create a Trade Record for the Trade
                            trade_record = {
                                "trade_id": None,  # Created while registering trade entry
                                "symbol": signal["symbol"],
                                "allocated": assessment["allocated"],
                                "entry_price": signal["price"],
                                "entry_time": signal["time"],
                                "position_size": assessment["units"],
                                "position_type": signal["current_position"],
                                "stop_loss": signal["stop_loss"],
                                "entry_time": signal["time"],
                                "take_profit_price": signal["take_profit_price"],
                                "position_closed_tp": 0,
                                "realized_pnl_tp": 0,
                                "realized_pnl": 0,
                                "capital": assessment["allocated"],
                                "exit_price": None,
                                "exit_time": None,
                                "margin": self.margin_dict.get(signal["symbol"], 1),
                                "status": None,
                                "order_id": [],
                            }

                            # Trade execution using openalgo
                            response = self.openalgo_client.placeorder(
                                strategy="Python",
                                symbol=get_oa_symbol(signal["symbol"], "NSE"),
                                action="BUY",
                                exchange="NSE",
                                price_type="MARKET",
                                product="MIS",
                                quantity=position_size,
                            )
                            if response["status"] == "success":
                                # Might Need to update or create some new keys based on actual Trade Execution stats - Like Actual Entry price etc
                                trade_record["status"] = response["status"]
                                self.trade_id = self.risk_manager.register_trade_entry(
                                    trade_record
                                )
                                self.trade_tracker[signal["symbol"]] = self.trade_id
                                trade_record["order_id"].append(response["orderid"])
                            else:
                                logger.info(
                                    f"{self.__class__.__name__}: Trade entry for {signal['symbol']} failed: {response.get('reason')}"
                                )
                    # If a take profit is there - Calculate the current profit and save the trade as a take profit trade
                    elif signal["type"] == "take_profit":
                        # Get Trade Record from the Active Trades
                        trade_record = self.risk_manager.active_trades[
                            self.trade_tracker[signal["symbol"]]
                        ]
                        # Execute Take Profit Trade
                        trade_record["take_profit_price_estimated"] = signal["price"]
                        trade_record["take_profit_time_estimated"] = signal["time"]
                        trade_record["position_closed_tp"] = (
                            signal["position_to_close"] * trade_record["position_size"]
                        )  # % to close * total position size
                        trade_record["realized_pnl_tp_estimated"] = (
                            signal["price"] - trade_record["entry_price"]
                        ) * trade_record["position_closed_tp"]

                        # Trade execution using openalgo
                        response = self.openalgo_client.placeorder(
                            strategy="Python",
                            symbol=get_oa_symbol(signal["symbol"], "NSE"),
                            action="SELL",
                            exchange="NSE",
                            price_type="MARKET",
                            product="MIS",
                            quantity=trade_record["position_closed_tp"],
                        )
                        if response["status"] == "success":
                            # Might Need to update or create some new keys based on actual Trade Execution stats - Like Actual Entry price etc
                            trade_record["status"] = response["status"]
                            trade_record["take_profit_price"] = signal["price"]
                            trade_record["take_profit_time"] = signal["time"]
                            trade_record["realized_pnl_tp"] = (
                                signal["price"] - trade_record["entry_price"]
                            ) * trade_record["position_closed_tp"]
                            trade_record["order_id"].append(response["orderid"])
                            self.trade_id = (
                                self.risk_manager.register_partial_trade_exit(
                                    trade_record
                                )
                            )
                        else:
                            logger.info(
                                f"{self.__class__.__name__}: Partial Trade Exit for {signal['symbol']} failed: {response.get('reason')}"
                            )
                            raise Exception("Not Yet Figured Out !!!!")

                    elif (
                        signal["type"] == "exit"
                        and signal["symbol"] in self.trade_tracker
                    ):
                        # Get Trade Record from the Active Trades
                        trade_record = self.risk_manager.active_trades[
                            self.trade_tracker[signal["symbol"]]
                        ]

                        trade_record["realized_pnl_estimated"] = (
                            (signal["price"] - trade_record["entry_price"])
                            * trade_record["position_size"]
                            * signal["position_to_close"]
                        )
                        trade_record["exit_price_estimated"] = signal["price"]
                        trade_record["exit_time_estimated"] = signal["time"]
                        # Trade execution using openalgo
                        response = self.openalgo_client.placeorder(
                            strategy="Python",
                            symbol=get_oa_symbol(signal["symbol"], "NSE"),
                            action="SELL",
                            exchange="NSE",
                            price_type="MARKET",
                            product="MIS",
                            quantity=trade_record["position_size"]
                            - trade_record["position_closed_tp"],
                        )
                        if response["status"] == "success":
                            trade_record["status"] = response["status"]
                            trade_record["realized_pnl"] = (
                                signal["price"] - trade_record["entry_price"]
                            ) * (
                                trade_record["position_size"]
                                - trade_record["position_closed_tp"]
                            )
                            trade_record["exit_price"] = signal["price"]
                            trade_record["exit_time"] = signal["time"]
                            trade_record["order_id"].append(response["orderid"])
                            self.risk_manager.register_full_trade_exit(trade_record)
                            del self.trade_tracker[signal["symbol"]]
                        else:
                            logger.info(
                                f"{self.__class__.__name__}: Full Trade Exit for {signal['symbol']} failed: {response.get('reason')}"
                            )
                            raise Exception("Not Yet Figured Out !!!!")

                # Short Position
                if signal["current_position"] == "short":
                    if signal["type"] == "entry":
                        # Check if trading is halted
                        # with self.lock:
                        self.halt_trading, self.halt_trading_reason = (
                            self.risk_manager.halt_trading_check()
                        )
                        if self.halt_trading:
                            logger.info(
                                f"Trading stopped. Reason: {self.halt_trading_reason}"
                            )
                        # Evaluate if we will take this trade or
                        assessment = self.risk_manager.assess_trade(
                            signal["symbol"], signal["price"], signal["stop_loss"]
                        )
                        if not assessment["eligible"]:
                            logger.info(
                                f"{self.__class__.__name__}: Trade entry for {signal['symbol']} rejected: {assessment.get('reason')}"
                            )
                        else:
                            # Call Executor Here
                            position_size = self.risk_manager.calculate_position_size(
                                signal["price"],
                                signal["stop_loss"],
                                self.margin_dict.get(signal["symbol"], 1),
                                signal["symbol"],
                            )
                            signal["position_size"] = position_size

                            # Get some Trade Information first
                            self.symbol = signal["symbol"]
                            self.entry_price = signal["price"]
                            self.entry_time = signal["time"]
                            self.position_size = position_size
                            self.position_closed_tp = 0
                            self.realized_pnl_tp = 0
                            self.realized_pnl = 0
                            self.take_profit_price = signal["take_profit_price"]

                            # Create a Trade Record for the Trade
                            trade_record = {
                                "trade_id": None,  # Created while registering trade entry
                                "symbol": signal["symbol"],
                                "allocated": assessment["allocated"],
                                "entry_price": signal["price"],
                                "entry_time": signal["time"],
                                "position_size": assessment["units"],
                                "position_type": signal["current_position"],
                                "stop_loss": signal["stop_loss"],
                                "entry_time": signal["time"],
                                "take_profit_price": signal["take_profit_price"],
                                "position_closed_tp": 0,
                                "realized_pnl_tp": 0,
                                "realized_pnl": 0,
                                "capital": assessment["allocated"],
                                "exit_price": None,
                                "exit_time": None,
                                "margin": self.margin_dict.get(signal["symbol"], 1),
                                "status": None,
                                "order_id": [],
                            }

                            # Trade execution using openalgo
                            response = self.openalgo_client.placeorder(
                                strategy="Python",
                                symbol=get_oa_symbol(signal["symbol"], "NSE"),
                                action="SELL",
                                exchange="NSE",
                                price_type="MARKET",
                                product="MIS",
                                quantity=position_size,
                            )
                            if response["status"] == "success":
                                # Might Need to update or create some new keys based on actual Trade Execution stats - Like Actual Entry price etc
                                trade_record["status"] = response["status"]
                                self.trade_id = self.risk_manager.register_trade_entry(
                                    trade_record
                                )
                                self.trade_tracker[signal["symbol"]] = self.trade_id
                                trade_record["order_id"].append(response["orderid"])
                            else:
                                logger.info(
                                    f"{self.__class__.__name__}: Trade entry for {signal['symbol']} failed: {response.get('reason')}"
                                )
                                raise Exception("Not Yet Figured Out !!!!")
                    elif signal["type"] == "take_profit":
                        # Get Trade Record from the Active Trades
                        trade_record = self.risk_manager.active_trades[
                            self.trade_tracker[signal["symbol"]]
                        ]
                        # Execute Take Profit Trade
                        trade_record["take_profit_price_estimated"] = signal["price"]
                        trade_record["take_profit_time_estimated"] = signal["time"]
                        trade_record["position_closed_tp"] = (
                            signal["position_to_close"] * trade_record["position_size"]
                        )  # % to close * total position size
                        trade_record["realized_pnl_tp_estimated"] = (
                            trade_record["entry_price"] - signal["price"]
                        ) * trade_record["position_closed_tp"]

                        # Trade execution using openalgo
                        response = self.openalgo_client.placeorder(
                            strategy="Python",
                            symbol=get_oa_symbol(signal["symbol"], "NSE"),
                            action="BUY",
                            exchange="NSE",
                            price_type="MARKET",
                            product="MIS",
                            quantity=trade_record["position_closed_tp"],
                        )
                        if response["status"] == "success":
                            # Might Need to update or create some new keys based on actual Trade Execution stats - Like Actual Entry price etc
                            trade_record["status"] = response["status"]
                            trade_record["take_profit_price"] = signal["price"]
                            trade_record["take_profit_time"] = signal["time"]
                            trade_record["realized_pnl_tp"] = (
                                trade_record["entry_price"] - signal["price"]
                            ) * trade_record["position_closed_tp"]
                            trade_record["order_id"].append(response["orderid"])
                            self.trade_id = (
                                self.risk_manager.register_partial_trade_exit(
                                    trade_record
                                )
                            )
                        else:
                            logger.info(
                                f"{self.__class__.__name__}: Partial Trade Exit for {signal['symbol']} failed: {response.get('reason')}"
                            )
                            raise Exception("Not Yet Figured Out !!!!")
                    elif (
                        signal["type"] == "exit"
                        and signal["symbol"] in self.trade_tracker
                    ):
                        # Get Trade Record from the Active Trades
                        trade_record = self.risk_manager.active_trades[
                            self.trade_tracker[signal["symbol"]]
                        ]
                        # Execute Exit Trade
                        trade_record["realized_pnl_estimated"] = (
                            (trade_record["entry_price"] - signal["price"])
                            * trade_record["position_size"]
                            * signal["position_to_close"]
                        )
                        trade_record["exit_price_estimated"] = signal["price"]
                        trade_record["exit_time_estimated"] = signal["time"]

                        # Trade execution using openalgo
                        response = self.openalgo_client.placeorder(
                            strategy="Python",
                            symbol=get_oa_symbol(signal["symbol"], "NSE"),
                            action="BUY",
                            exchange="NSE",
                            price_type="MARKET",
                            product="MIS",
                            quantity=trade_record["position_size"]
                            - trade_record["position_closed_tp"],
                        )
                        if response["status"] == "success":
                            trade_record["status"] = response["status"]
                            trade_record["realized_pnl"] = (
                                trade_record["entry_price"] - signal["price"]
                            ) * (
                                trade_record["position_size"]
                                - trade_record["position_closed_tp"]
                            )
                            trade_record["exit_price"] = signal["price"]
                            trade_record["exit_time"] = signal["time"]
                            trade_record["order_id"].append(response["orderid"])
                            self.risk_manager.register_full_trade_exit(trade_record)
                            del self.trade_tracker[signal["symbol"]]
                        else:
                            logger.info(
                                f"{self.__class__.__name__}: Full Trade Exit for {signal['symbol']} failed: {response.get('reason')}"
                            )
                            raise Exception("Not Yet Figured Out !!!!")
                logger.info(f"Processed Signal {signal}")
            else:
                logger.info(f"No Valid Signal {signal}")
        except Exception as e:
            logger.error(
                f"Error Processing {signal} with Exception {str(e)}", exc_info=True
            )
