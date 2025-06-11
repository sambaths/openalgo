import os, sys
from multiprocessing import Lock
from logger import logger
from datetime import datetime
from pytz import timezone
import uuid

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
        shared_lock,
        config,
        margin_dict,
        manager=None,
    ):
        self.risk_manager = risk_manager
        self.trade_executor = trade_executor
        self.max_active_trades = max_active_trades
        self.TRADING_ACTIVE = True
        self.config = config
        self.margin_dict = margin_dict
        self.lock = Lock()
        self.shared_lock = shared_lock  # Use shared lock for cross-process synchronization
        self.slippage = config["trading_setting"]["slippage"]
        self.commission = config["trading_setting"]["commission"]
        self.trade_tracker = {}
        self.manager = manager  # Store manager reference for creating managed objects
        self.shared_state = shared_state
        
        # Check if simulation mode is enabled
        self.is_simulation_mode = os.getenv("simulation_type", "").lower() in ["live", "historical"]
        self.is_live_simulate_mode = self.is_simulation_mode == "live"
        if self.is_simulation_mode:
            logger.info("🎭 SIMULATION MODE ENABLED - All trades will be simulated, no real orders will be placed")
        if self.is_live_simulate_mode:
            self.openalgo_client = None  # Don't initialize API client in simulation mode
        else:
            self.openalgo_client = api(
                api_key=os.getenv("APP_KEY"),
                host=os.getenv("HOST_SERVER", "http://127.0.0.1:5000"),
            )

        logger.debug("TradeManager Initialized")

    def _generate_dummy_order_response(self, action, symbol, quantity):
        """
        Generate a dummy successful order response for simulation mode
        
        Args:
            action: BUY or SELL
            symbol: Trading symbol
            quantity: Order quantity
            
        Returns:
            Dict: Dummy successful order response
        """
        dummy_order_id = f"SIM_{uuid.uuid4().hex[:8].upper()}"
        return {
            "status": "success",
            "orderid": dummy_order_id,
            "message": f"Simulated {action} order placed successfully",
            "data": {
                "order_id": dummy_order_id,
                "symbol": symbol,
                "action": action,
                "quantity": quantity,
                "status": "COMPLETE",
                "exchange": "NSE",
                "product": "MIS",
                "price_type": "MARKET"
            }
        }

    def _generate_dummy_order_status_response(self, order_id):
        """
        Generate a dummy order status response for simulation mode
        
        Args:
            order_id: The order ID to check
            
        Returns:
            Dict: Dummy order status response showing completion
        """
        return {
            "status": "success",
            "data": {
                "order_id": order_id,
                "order_status": "COMPLETE",
                "filled_quantity": "100",
                "average_price": "100.50",
                "message": "Simulated order completed successfully"
            }
        }

    def _place_order_with_simulation_check(self, strategy, symbol, action, exchange, price_type, product, quantity):
        """
        Place order with simulation mode check
        
        Args:
            All the standard placeorder parameters
            
        Returns:
            Dict: Order response (real or simulated)
        """
        if self.is_live_simulate_mode:
            logger.debug(f"🎭 SIMULATION: Placing {action} order for {quantity} shares of {symbol}")
            return self._generate_dummy_order_response(action, symbol, quantity)
        else:
            return self.openalgo_client.placeorder(
                strategy=strategy,
                symbol=symbol,
                action=action,
                exchange=exchange,
                price_type=price_type,
                product=product,
                quantity=quantity if action.lower() == 'BUY' else -1 * quantity,
            )

    def _get_order_status_with_simulation_check(self, order_id, strategy):
        """
        Get order status with simulation mode check
        
        Args:
            order_id: Order ID to check
            strategy: Strategy name
            
        Returns:
            Dict: Order status response (real or simulated)
        """
        if self.is_live_simulate_mode:
            logger.debug(f"🎭 SIMULATION: Checking status for order {order_id} - returning completed status")
            return self._generate_dummy_order_status_response(order_id)
        else:
            return self.openalgo_client.orderstatus(
                order_id=order_id,
                strategy=strategy
            )

    def _check_order_completion(self, response):
        """
        Check if order is completed, handling both real and simulation modes
        
        Args:
            response: Order response from placeorder
            
        Returns:
            Tuple: (order_completed: bool, order_id: str)
        """
        if self.is_live_simulate_mode:
            # In simulation mode, all orders are immediately completed
            return True, response.get("orderid")
        
        # Real mode - check order status
        order_completed = False
        order_id = response.get("orderid")
        
        if order_id:
            try:
                order_status_response = self._get_order_status_with_simulation_check(order_id, "Python")
                logger.debug(f"Order Status for Order ID {order_id}: {order_status_response}")
                
                # Check if order is actually completed
                if order_status_response.get("data", {}).get("order_status", "").lower() in ["complete", "executed", "filled"]:
                    order_completed = True
                    logger.debug(f"Order {order_id} is completed/executed")
                else:
                    logger.warning(f"Order {order_id} is not completed yet. Status: {order_status_response.get('data', {}).get('order_status', 'Unknown')}")
                    
            except Exception as e:
                logger.error(f"Failed to get order status for Order ID {order_id}: {e}")
        
        return order_completed, order_id

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
            logger.debug(
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
                            get_oa_symbol(signal["symbol"], "NSE"), signal["price"], signal["stop_loss"]
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
                                self.margin_dict.get(get_oa_symbol(signal["symbol"], "NSE"), 1),
                                get_oa_symbol(signal["symbol"], "NSE"),
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

                            # Trade execution using openalgo (or simulation)
                            response = self._place_order_with_simulation_check(
                                strategy="Python",
                                symbol=get_oa_symbol(signal["symbol"], "NSE"),
                                action="BUY",
                                exchange="NSE",
                                price_type="MARKET",
                                product="MIS",
                                quantity=position_size,
                            )

                            logger.debug(f"Response for Entry Trade: {response}")
                            
                            # Check order completion (handles both real and simulation modes)
                            order_completed, order_id = self._check_order_completion(response)
                            
                            # Only proceed if order is actually completed
                            if order_completed:
                                # Might Need to update or create some new keys based on actual Trade Execution stats - Like Actual Entry price etc
                                trade_record["status"] = "completed"
                                self.trade_id = self.risk_manager.register_trade_entry(
                                    trade_record
                                )
                                self.trade_tracker[signal["symbol"]] = self.trade_id
                                trade_record["order_id"].append(order_id)
                                
                                # Clear any incomplete entry trade for this symbol
                                self.clear_incomplete_trade(signal["symbol"], "entry")
                                
                                # Clear any previous failed trade record for successful trade
                                self.clear_failed_trade(signal["symbol"])
                            else:
                                logger.info(f"{self.__class__.__name__}: Trade entry for {signal['symbol']} failed - order not completed")
                                
                                # Track incomplete entry trade
                                self.track_incomplete_trade(
                                    signal=signal,
                                    trade_type="entry", 
                                    order_id=order_id,
                                    reason="entry_order_not_completed"
                                )
                                
                                # Track failed trade with proper structure
                                self.track_failed_trade(
                                    signal=signal,
                                    trade_type="entry",
                                    order_id=order_id,
                                    response=response,
                                    reason="entry_order_not_completed"
                                )
                                
                                if signal["symbol"] in self.trade_tracker:
                                    del self.trade_tracker[signal["symbol"]]
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

                        # Trade execution using openalgo (or simulation)
                        response = self._place_order_with_simulation_check(
                            strategy="Python",
                            symbol=get_oa_symbol(signal["symbol"], "NSE"),
                            action="SELL",
                            exchange="NSE",
                            price_type="MARKET",
                            product="MIS",
                            quantity=trade_record["position_closed_tp"],
                        )
                        logger.debug(f"Response for Take Profit Trade: {response}")
                        
                        # Check order completion (handles both real and simulation modes)
                        order_completed, order_id = self._check_order_completion(response)
                        
                        # Only proceed if order is actually completed
                        if order_completed:
                            # Might Need to update or create some new keys based on actual Trade Execution stats - Like Actual Entry price etc
                            trade_record["status"] = "completed"
                            trade_record["take_profit_price"] = signal["price"]
                            trade_record["take_profit_time"] = signal["time"]
                            trade_record["realized_pnl_tp"] = (
                                signal["price"] - trade_record["entry_price"]
                            ) * trade_record["position_closed_tp"]
                            trade_record["order_id"].append(order_id)
                            self.trade_id = (
                                self.risk_manager.register_partial_trade_exit(
                                    trade_record
                                )
                            )
                            
                            # Clear any incomplete take_profit trade for this symbol
                            self.clear_incomplete_trade(signal["symbol"], "take_profit")
                            
                            # Clear any previous failed trade record for successful trade
                            self.clear_failed_trade(signal["symbol"])
                        else:
                            logger.info(f"{self.__class__.__name__}: Partial Trade Exit for {signal['symbol']} failed - order not completed")
                            
                            # Track incomplete take_profit trade
                            self.track_incomplete_trade(
                                signal=signal,
                                trade_type="take_profit",
                                order_id=order_id,
                                reason="take_profit_order_not_completed"
                            )
                            
                            # Track failed trade with proper structure
                            self.track_failed_trade(
                                signal=signal,
                                trade_type="take_profit",
                                order_id=order_id,
                                response=response,
                                reason="take_profit_order_not_completed"
                            )
                    elif (
                        signal["type"] == "exit"
                        and signal["symbol"] in self.trade_tracker
                    ):
                        # Get Trade Record from the Active Trades
                        trade_id = self.trade_tracker[signal["symbol"]]
                        
                        # Double-check that this trade still exists in active trades
                        # to prevent duplicate processing by multiple processes
                        if trade_id not in self.risk_manager.active_trades:
                            logger.warning(f"Trade {trade_id} for {signal['symbol']} already processed or doesn't exist in active trades. Skipping exit.")
                            if signal["symbol"] in self.trade_tracker:
                                del self.trade_tracker[signal["symbol"]]
                            return
                            
                        trade_record = self.risk_manager.active_trades[trade_id]

                        trade_record["realized_pnl_estimated"] = (
                            (signal["price"] - trade_record["entry_price"])
                            * trade_record["position_size"]
                            * signal["position_to_close"]
                        )
                        trade_record["exit_price_estimated"] = signal["price"]
                        trade_record["exit_time_estimated"] = signal["time"]
                        # Trade execution using openalgo (or simulation)
                        response = self._place_order_with_simulation_check(
                            strategy="Python",
                            symbol=get_oa_symbol(signal["symbol"], "NSE"),
                            action="SELL",
                            exchange="NSE",
                            price_type="MARKET",
                            product="MIS",
                            quantity=trade_record["position_size"]
                            - trade_record["position_closed_tp"],
                        )
                        
                        logger.debug(f"Response for Exit Trade: {response}")
                        
                        # Check order completion (handles both real and simulation modes)
                        order_completed, order_id = self._check_order_completion(response)
                        
                        # Only proceed if order is actually completed
                        if order_completed:
                            # Final check before registering exit to prevent race conditions
                            if trade_id not in self.risk_manager.active_trades:
                                logger.warning(f"Trade {trade_id} for {signal['symbol']} was already processed by another process. Skipping duplicate exit.")
                                if signal["symbol"] in self.trade_tracker:
                                    del self.trade_tracker[signal["symbol"]]
                                return
                                
                            trade_record["status"] = "completed"
                            trade_record["realized_pnl"] = (
                                signal["price"] - trade_record["entry_price"]
                            ) * (
                                trade_record["position_size"]
                                - trade_record["position_closed_tp"]
                            )
                            trade_record["exit_price"] = signal["price"]
                            trade_record["exit_time"] = signal["time"]
                            trade_record["order_id"].append(order_id)
                            
                            # Use try-except to handle potential duplicate registration
                            try:
                                self.risk_manager.register_full_trade_exit(trade_record)
                                logger.info(f"Successfully registered trade exit for {signal['symbol']} (trade_id: {trade_id})")
                            except Exception as e:
                                logger.error(f"Failed to register trade exit for {signal['symbol']} (trade_id: {trade_id}): {e}")
                                # Continue processing even if registration fails
                            
                            # Only remove from trade_tracker if exit was successful
                            if signal["symbol"] in self.trade_tracker:
                                del self.trade_tracker[signal["symbol"]]
                            
                            # Clear any incomplete exit trade for this symbol
                            self.clear_incomplete_trade(signal["symbol"], "exit")
                            
                            # Clear any previous failed trade record for successful trade
                            self.clear_failed_trade(signal["symbol"])
                        else:
                            logger.info(f"{self.__class__.__name__}: Full Trade Exit for {signal['symbol']} failed - order not completed")
                            
                            # Track incomplete exit trade
                            self.track_incomplete_trade(
                                signal=signal,
                                trade_type="exit",
                                order_id=order_id,
                                reason="exit_order_not_completed"
                            )
                            
                            # Track failed trade with proper structure
                            self.track_failed_trade(
                                signal=signal,
                                trade_type="exit",
                                order_id=order_id,
                                response=response,
                                reason="exit_order_not_completed"
                            )
                            
                            del self.trade_tracker[signal["symbol"]]
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
                            get_oa_symbol(signal["symbol"], "NSE"), signal["price"], signal["stop_loss"]
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
                                self.margin_dict.get(get_oa_symbol(signal["symbol"], "NSE"), 1),
                                get_oa_symbol(signal["symbol"], "NSE"),
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

                            # Trade execution using openalgo (or simulation)
                            response = self._place_order_with_simulation_check(
                                strategy="Python",
                                symbol=get_oa_symbol(signal["symbol"], "NSE"),
                                action="SELL",
                                exchange="NSE",
                                price_type="MARKET",
                                product="MIS",
                                quantity=position_size,
                            )
                            logger.debug(f"Response for Entry Trade (Short): {response}")
                            
                            # Check order completion (handles both real and simulation modes)
                            order_completed, order_id = self._check_order_completion(response)
                            
                            # Only proceed if order is actually completed
                            if order_completed:
                                # Might Need to update or create some new keys based on actual Trade Execution stats - Like Actual Entry price etc
                                trade_record["status"] = "completed"
                                self.trade_id = self.risk_manager.register_trade_entry(
                                    trade_record
                                )
                                self.trade_tracker[signal["symbol"]] = self.trade_id
                                trade_record["order_id"].append(order_id)
                                
                                # Clear any incomplete entry trade for this symbol
                                self.clear_incomplete_trade(signal["symbol"], "entry")
                                
                                # Clear any previous failed trade record for successful trade
                                self.clear_failed_trade(signal["symbol"])
                            else:
                                logger.info(f"{self.__class__.__name__}: Trade entry for {signal['symbol']} failed - order not completed")
                                
                                # Track incomplete entry trade
                                self.track_incomplete_trade(
                                    signal=signal,
                                    trade_type="entry", 
                                    order_id=order_id,
                                    reason="entry_order_not_completed"
                                )
                                
                                # Track failed trade with proper structure
                                self.track_failed_trade(
                                    signal=signal,
                                    trade_type="entry",
                                    order_id=order_id,
                                    response=response,
                                    reason="entry_order_not_completed"
                                )
                                
                                if signal["symbol"] in self.trade_tracker:
                                    del self.trade_tracker[signal["symbol"]]
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

                        # Trade execution using openalgo (or simulation)
                        response = self._place_order_with_simulation_check(
                            strategy="Python",
                            symbol=get_oa_symbol(signal["symbol"], "NSE"),
                            action="BUY",
                            exchange="NSE",
                            price_type="MARKET",
                            product="MIS",
                            quantity=trade_record["position_closed_tp"],
                        )
                        logger.debug(f"Response for Take Profit Trade: {response}")
                        
                        # Check order completion (handles both real and simulation modes)
                        order_completed, order_id = self._check_order_completion(response)
                        
                        # Only proceed if order is actually completed
                        if order_completed:
                            # Might Need to update or create some new keys based on actual Trade Execution stats - Like Actual Entry price etc
                            trade_record["status"] = "completed"
                            trade_record["take_profit_price"] = signal["price"]
                            trade_record["take_profit_time"] = signal["time"]
                            trade_record["realized_pnl_tp"] = (
                                trade_record["entry_price"] - signal["price"]
                            ) * trade_record["position_closed_tp"]
                            trade_record["order_id"].append(order_id)
                            self.trade_id = (
                                self.risk_manager.register_partial_trade_exit(
                                    trade_record
                                )
                            )
                            
                            # Clear any incomplete take_profit trade for this symbol
                            self.clear_incomplete_trade(signal["symbol"], "take_profit")
                            
                            # Clear any previous failed trade record for successful trade
                            self.clear_failed_trade(signal["symbol"])
                        else:
                            logger.info(f"{self.__class__.__name__}: Partial Trade Exit for {signal['symbol']} failed - order not completed")
                            
                            # Track incomplete take_profit trade
                            self.track_incomplete_trade(
                                signal=signal,
                                trade_type="take_profit",
                                order_id=order_id,
                                reason="take_profit_order_not_completed"
                            )
                            
                            # Track failed trade with proper structure
                            self.track_failed_trade(
                                signal=signal,
                                trade_type="take_profit",
                                order_id=order_id,
                                response=response,
                                reason="take_profit_order_not_completed"
                            )
                    elif (
                        signal["type"] == "exit"
                        and signal["symbol"] in self.trade_tracker
                    ):
                        # Get Trade Record from the Active Trades
                        trade_id = self.trade_tracker[signal["symbol"]]
                        
                        # Double-check that this trade still exists in active trades
                        # to prevent duplicate processing by multiple processes
                        if trade_id not in self.risk_manager.active_trades:
                            logger.warning(f"Trade {trade_id} for {signal['symbol']} already processed or doesn't exist in active trades. Skipping exit.")
                            if signal["symbol"] in self.trade_tracker:
                                del self.trade_tracker[signal["symbol"]]
                            return
                            
                        trade_record = self.risk_manager.active_trades[trade_id]

                        trade_record["realized_pnl_estimated"] = (
                            (trade_record["entry_price"] - signal["price"])
                            * trade_record["position_size"]
                            * signal["position_to_close"]
                        )
                        trade_record["exit_price_estimated"] = signal["price"]
                        trade_record["exit_time_estimated"] = signal["time"]

                        # Trade execution using openalgo (or simulation)
                        response = self._place_order_with_simulation_check(
                            strategy="Python",
                            symbol=get_oa_symbol(signal["symbol"], "NSE"),
                            action="BUY",
                            exchange="NSE",
                            price_type="MARKET",
                            product="MIS",
                            quantity=trade_record["position_size"]
                            - trade_record["position_closed_tp"],
                        )
                        logger.debug(f"Response for Exit Trade: {response}")
                        
                        # Check order completion (handles both real and simulation modes)
                        order_completed, order_id = self._check_order_completion(response)
                        
                        # Only proceed if order is actually completed
                        if order_completed:
                            # Final check before registering exit to prevent race conditions
                            if trade_id not in self.risk_manager.active_trades:
                                logger.warning(f"Trade {trade_id} for {signal['symbol']} was already processed by another process. Skipping duplicate exit.")
                                if signal["symbol"] in self.trade_tracker:
                                    del self.trade_tracker[signal["symbol"]]
                                return
                                
                            trade_record["status"] = "completed"
                            trade_record["realized_pnl"] = (
                                trade_record["entry_price"] - signal["price"]
                            ) * (
                                trade_record["position_size"]
                                - trade_record["position_closed_tp"]
                            )
                            trade_record["exit_price"] = signal["price"]
                            trade_record["exit_time"] = signal["time"]
                            trade_record["order_id"].append(order_id)
                            
                            # Use try-except to handle potential duplicate registration
                            try:
                                self.risk_manager.register_full_trade_exit(trade_record)
                                logger.info(f"Successfully registered trade exit for {signal['symbol']} (trade_id: {trade_id})")
                            except Exception as e:
                                logger.error(f"Failed to register trade exit for {signal['symbol']} (trade_id: {trade_id}): {e}")
                                # Continue processing even if registration fails
                            
                            # Only remove from trade_tracker if exit was successful
                            if signal["symbol"] in self.trade_tracker:
                                del self.trade_tracker[signal["symbol"]]
                            
                            # Clear any incomplete exit trade for this symbol
                            self.clear_incomplete_trade(signal["symbol"], "exit")
                            
                            # Clear any previous failed trade record for successful trade
                            self.clear_failed_trade(signal["symbol"])
                        else:
                            logger.info(f"{self.__class__.__name__}: Full Trade Exit for {signal['symbol']} failed - order not completed")
                            
                            # Track incomplete exit trade
                            self.track_incomplete_trade(
                                signal=signal,
                                trade_type="exit",
                                order_id=order_id,
                                reason="exit_order_not_completed"
                            )
                            
                            # Track failed trade with proper structure
                            self.track_failed_trade(
                                signal=signal,
                                trade_type="exit",
                                order_id=order_id,
                                response=response,
                                reason="exit_order_not_completed"
                            )
                            
                            del self.trade_tracker[signal["symbol"]]
                logger.debug(f"Processed Signal {signal}")
            else:
                logger.debug(f"No Valid Signal {signal}")
        except Exception as e:
            logger.error(
                f"Error Processing {signal} with Exception {str(e)}", exc_info=True
            )

    def track_failed_trade(self, signal, trade_type, order_id=None, response=None, reason="order_failed"):
        """
        Track failed trades in shared_state with proper structure and comprehensive information
        
        Args:
            signal: The original signal that failed
            trade_type: 'entry', 'exit', or 'take_profit'
            order_id: The order ID if available
            response: The API response from the failed order
            reason: Reason for failure
        """
        symbol = signal["symbol"]
        failed_trade_data = {
            "signal": {
                "symbol": signal["symbol"],
                "type": signal["type"],
                "current_position": signal["current_position"],
                "price": signal["price"],
                "time": signal["time"].isoformat() if hasattr(signal["time"], 'isoformat') else str(signal["time"]),
            },
            "trade_type": trade_type,
            "order_id": order_id,
            "response": response,
            "timestamp": datetime.now(tz=IST).isoformat(),
            "reason": reason,
            "attempt_count": 1,
            "active_trades_count": len(self.trade_tracker)  # Current count when failure occurred
        }
        
        with self.shared_lock:
            # Initialize failed trades dictionary if it doesn't exist
            if "failed_trades" not in self.shared_state:
                self.shared_state["failed_trades"] = self.manager.dict() if hasattr(self, 'manager') else {}
            
            # Get current failed trades and update
            failed_trades = dict(self.shared_state["failed_trades"])
            
            # If this symbol already has a failed trade, increment attempt count
            if symbol in failed_trades:
                if isinstance(failed_trades[symbol], dict) and "attempt_count" in failed_trades[symbol]:
                    failed_trade_data["attempt_count"] = failed_trades[symbol]["attempt_count"] + 1
            
            failed_trades[symbol] = failed_trade_data
            self.shared_state["failed_trades"] = failed_trades
            
        logger.error(f"Tracked failed {trade_type} trade for {symbol}: {failed_trade_data}")

    def clear_failed_trade(self, symbol):
        """
        Clear failed trade record when a trade succeeds
        
        Args:
            symbol: The symbol to clear
        """
        with self.shared_lock:
            if "failed_trades" in self.shared_state:
                failed_trades = dict(self.shared_state["failed_trades"])
                if symbol in failed_trades:
                    del failed_trades[symbol]
                    self.shared_state["failed_trades"] = failed_trades
                    logger.info(f"Cleared failed trade record for {symbol} - trade succeeded")

    def safe_get_shared_state(self, key, default=None):
        """
        Safely get a value from shared_state with multiprocessing lock
        
        Args:
            key: The key to retrieve from shared_state
            default: Default value if key doesn't exist
            
        Returns:
            The value from shared_state or default
        """
        with self.shared_lock:
            return self.shared_state.get(key, default)
    
    def safe_set_shared_state(self, key, value):
        """
        Safely set a value in shared_state with multiprocessing lock
        
        Args:
            key: The key to set in shared_state
            value: The value to set
        """
        with self.shared_lock:
            self.shared_state[key] = value
    
    def safe_update_shared_state(self, key, update_func, default=None):
        """
        Safely update a value in shared_state atomically with multiprocessing lock
        This allows you to read, modify, and write in a single atomic operation
        
        Args:
            key: The key to update in shared_state
            update_func: Function that takes current value and returns new value
            default: Default value if key doesn't exist
            
        Returns:
            The new value after update
        """
        with self.shared_lock:
            current_value = self.shared_state.get(key, default)
            new_value = update_func(current_value)
            self.shared_state[key] = new_value
            return new_value
    
    def safe_check_and_update_shared_state(self, key, condition_func, update_func, default=None):
        """
        Safely check a condition and update shared_state atomically if condition is met
        
        Args:
            key: The key to check/update in shared_state
            condition_func: Function that takes current value and returns True/False
            update_func: Function that takes current value and returns new value
            default: Default value if key doesn't exist
            
        Returns:
            Tuple of (was_updated: bool, new_value)
        """
        with self.shared_lock:
            current_value = self.shared_state.get(key, default)
            if condition_func(current_value):
                new_value = update_func(current_value)
                self.shared_state[key] = new_value
                return True, new_value
            else:
                return False, current_value

    def track_incomplete_trade(self, signal, trade_type, order_id=None, reason="order_not_completed"):
        """
        Track incomplete trades in shared_state for communication with MarketDataHandler
        
        Args:
            signal: The original signal that failed to complete
            trade_type: 'entry', 'exit', or 'take_profit'
            order_id: The order ID if available
            reason: Reason for incompletion
        """
        symbol = signal["symbol"]
        incomplete_trade_data = {
            "signal": signal.copy(),
            "order_id": order_id,
            "timestamp": datetime.now(tz=IST).isoformat(),
            "reason": reason,
            "attempt_count": 1
        }
        
        with self.shared_lock:
            # Initialize incomplete trade dictionaries if they don't exist
            if f"incomplete_{trade_type}_trades" not in self.shared_state:
                self.shared_state[f"incomplete_{trade_type}_trades"] = self.manager.dict() if hasattr(self, 'manager') else {}
            
            # Store the incomplete trade
            incomplete_trades = dict(self.shared_state[f"incomplete_{trade_type}_trades"])
            incomplete_trades[symbol] = incomplete_trade_data
            self.shared_state[f"incomplete_{trade_type}_trades"] = incomplete_trades
            
        logger.info(f"Tracked incomplete {trade_type} trade for {symbol}: {incomplete_trade_data}")

    def clear_incomplete_trade(self, symbol, trade_type):
        """
        Clear incomplete trade from shared_state when trade completes
        
        Args:
            symbol: The symbol to clear
            trade_type: 'entry', 'exit', or 'take_profit'
        """
        with self.shared_lock:
            incomplete_trades_key = f"incomplete_{trade_type}_trades"
            if incomplete_trades_key in self.shared_state:
                incomplete_trades = dict(self.shared_state[incomplete_trades_key])
                if symbol in incomplete_trades:
                    del incomplete_trades[symbol]
                    self.shared_state[incomplete_trades_key] = incomplete_trades
                    logger.info(f"Cleared incomplete {trade_type} trade for {symbol}")

    def increment_incomplete_trade_attempt(self, symbol, trade_type):
        """
        Increment attempt count for incomplete trade
        
        Args:
            symbol: The symbol to increment
            trade_type: 'entry', 'exit', or 'take_profit'
            
        Returns:
            New attempt count
        """
        with self.shared_lock:
            incomplete_trades_key = f"incomplete_{trade_type}_trades"
            if incomplete_trades_key in self.shared_state:
                incomplete_trades = dict(self.shared_state[incomplete_trades_key])
                if symbol in incomplete_trades:
                    incomplete_trades[symbol]["attempt_count"] += 1
                    incomplete_trades[symbol]["last_attempt"] = datetime.now(tz=IST).isoformat()
                    self.shared_state[incomplete_trades_key] = incomplete_trades
                    return incomplete_trades[symbol]["attempt_count"]
        return 0
