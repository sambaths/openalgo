from multiprocessing import Lock, Manager, Queue
from threading import Thread
from datetime import datetime
import os, requests
import json
from risk_manager import RiskManager
from trade_manager import TradeManager
from worker import Worker
from broker_simulator import SimulatedWebSocket
from fyers_utils import FyersBroker
from utils import validate_shared_state, monitor_manager_health, safe_update_shared_state, safe_get_shared_state

from dotenv import load_dotenv

load_dotenv()


import logging
from logger import logger

logger.setLevel(getattr(logging, os.environ["DEBUG_LEVEL"].upper(), None))

import threading
import time
import queue

from db import setup, DBHandler, MarketData, StockSignals
from strategy_manager import StrategyManager, create_strategy_manager

# Import termination handler
from websocket_termination_handler import create_termination_handler
from enhanced_broker_simulator import EnhancedSimulatedWebSocket


class OrderStatusManager:
    def __init__(self, shared_state, shared_lock, poll_interval=1):
        self.shared_state = shared_state
        self.shared_lock = shared_lock
        self.poll_interval = poll_interval
        self.running = True
        self.api_url = os.getenv(
            "OPENALGO_ORDERSTATUS_URL", "http://127.0.0.1:5000/api/v1/orderstatus/"
        )
        self.api_key = os.getenv("APP_KEY")

    def poll_order_status(self):
        while self.running:
            try:
                # Get all order IDs to check (should be updated by workers/trade manager)
                with self.shared_lock:
                    order_ids = list(self.shared_state.get("active_order_ids", []))
                
                status_dict = {}
                for order_id in order_ids:
                    payload = {"apikey": self.api_key, "orderid": order_id}
                    headers = {"Content-Type": "application/json"}
                    try:
                        response = requests.post(
                            self.api_url, json=payload, headers=headers, timeout=5
                        )
                        if response.status_code == 200:
                            status_dict[order_id] = response.json()
                        else:
                            status_dict[order_id] = {
                                "status": "error",
                                "code": response.status_code,
                            }
                    except Exception as e:
                        status_dict[order_id] = {"status": "error", "message": str(e)}
                
                with self.shared_lock:
                    self.shared_state["order_status_dict"] = status_dict
            except Exception as e:
                logger.error(f"OrderStatusManager polling error: {e}")
            time.sleep(self.poll_interval)

    def start(self):
        self.thread = threading.Thread(target=self.poll_order_status, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if hasattr(self, "thread"):
            self.thread.join(timeout=2)


# --- Data Dispatcher Definition ---
class DataDispatcher:
    """
    Routes incoming market data to the correct worker queue.
    """

    def __init__(self, symbols, worker_assignment):
        """
        Args:
            symbols (list): All symbols.
            worker_assignment (dict): Mapping from symbol to worker index.
        """
        self.symbols = symbols
        self.worker_assignment = worker_assignment
        self.worker_queues = {}  # Map: worker index -> multiprocessing.Queue
        logger.debug(f"DataDispatcher initialized")

    def register_worker_queue(self, worker_idx, queue):
        self.worker_queues[worker_idx] = queue
        logger.debug(f"Worker queue registered for worker {worker_idx}")

    def dispatch(self, data):
        """
        Dispatch a data bar to the proper worker.
        """
        symbol = data.get("symbol")
        worker_idx = self.worker_assignment.get(symbol)
        logger.debug(f"Dispatching data for {symbol} to worker {worker_idx}")
        if worker_idx is not None and worker_idx in self.worker_queues:
            self.worker_queues[worker_idx].put(data)


class Driver:
    """
    The main driver that sets up workers, dispatches market data, and processes trade signals.
    """

    def __init__(self, config):
        self.config = config
        self.symbols = self.config["trading_setting"]["symbols"]
        self.num_workers = min(len(self.symbols), 6)
        self.lock = Lock()
        self.workers = []
        self.worker_queues = {}
        self.running = True

        # Partition symbols among workers (round-robin)
        self.worker_assignment = {}
        self.worker_symbols = {i: [] for i in range(self.num_workers)}
        for idx, symbol in enumerate(self.symbols):
            symbol = f"NSE:{symbol}-EQ"
            worker_idx = idx % self.num_workers
            self.worker_symbols[worker_idx].append(symbol)
            self.worker_assignment[symbol] = worker_idx

        # Set up shared state with proper Manager
        self.manager = Manager()
        self.shared_state = self.manager.dict()
        self.shared_lock = self.manager.Lock()  # Shared lock for synchronization
        
        # Initialize shared state variables
        self.shared_state["active_trades"] = 0
        self.shared_state["active_order_ids"] = self.manager.list()  # Use manager.list()
        self.shared_state["order_status_dict"] = self.manager.dict()  # Use manager.dict()
        self.shared_state["failed_trades"] = self.manager.dict()  # Track failed trades across processes
        
        # Initialize incomplete trade tracking dictionaries
        self.shared_state["incomplete_entry_trades"] = self.manager.dict()
        self.shared_state["incomplete_exit_trades"] = self.manager.dict()
        self.shared_state["incomplete_takeprofit_trades"] = self.manager.dict()
        
        self.trade_signal_queue = self.manager.Queue()

        # Retrieve margin information
        payload = {"apikey": os.getenv("APP_KEY"), "symbols": self.symbols}
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{os.getenv('HOST_SERVER')}/api/v1/intradaymargin/", json=payload
        )
        logger.debug(f"{self.__class__.__name__}: Margin response: {response.json()}")
        self.shared_state["margin_dict"] = response.json()["data"]
        logger.debug(f"{self.__class__.__name__}: Margin dictionary: {response.json()["data"]}")
        # Get funds from OpenAlgo API and validate
        logger.debug("Getting funds information from OpenAlgo API...")
        try:
            funds_payload = {"apikey": os.getenv("APP_KEY")}
            funds_response = requests.post(
                f"{os.getenv('HOST_SERVER')}/api/v1/funds/", 
                json=funds_payload, 
                headers=headers,
                timeout=10
            )
            
            if funds_response.status_code == 200:
                funds_data = funds_response.json()
                api_funds = float(funds_data.get("data", {}).get("availablecash", 0))
                logger.info(f"Available funds from API: ₹{api_funds:,.2f}")
            else:
                logger.error(f"Failed to get funds from API: {funds_response.status_code}")
                api_funds = 0
        except Exception as e:
            logger.error(f"Error getting funds from API: {e}")
            api_funds = 0

        # Determine if we're using live data
        LIVE_DATA = os.environ.get(
            "LIVE_DATA", str(self.config.get("LIVE_DATA", False))
        ).lower() in ["true", "1"]

        # Get starting capital from config
        config_capital = config["capital_management"]["starting_capital"]
        logger.debug(f"Starting capital from config: ₹{config_capital:,.2f}")

        # Apply funds validation logic
        if LIVE_DATA:
            logger.info("Live trading mode detected")
            if api_funds == 0:
                logger.critical("❌ CRITICAL: Live trading mode but available funds is 0!")
                logger.critical("❌ Cannot proceed with live trading without funds")
                logger.critical("❌ Terminating system immediately for safety")
                raise SystemExit("Live trading terminated: Zero funds available")
            
            # Use minimum of API funds or config
            final_capital = min(api_funds, config_capital)
            logger.debug(f"Live mode: Using minimum of API funds (₹{api_funds:,.2f}) and config (₹{config_capital:,.2f})")
            logger.info(f"Final capital for live trading: ₹{final_capital:,.2f}")
            
        else:
            logger.info("Simulated trading mode detected")
            if api_funds == 0:
                # In simulation, if API funds is 0, use config value
                final_capital = config_capital
                logger.info(f"Simulation mode: API funds is 0, using config capital: ₹{final_capital:,.2f}")
            else:
                # Use minimum of API funds or config
                final_capital = min(api_funds, config_capital)
                logger.debug(f"Simulation mode: Using minimum of API funds (₹{api_funds:,.2f}) and config (₹{config_capital:,.2f})")
                logger.info(f"Final capital for simulation: ₹{final_capital:,.2f}")

        # Validate minimum capital requirement
        MIN_CAPITAL_REQUIRED = 10000
        if final_capital < MIN_CAPITAL_REQUIRED:
            logger.critical("❌ CRITICAL: Insufficient capital for trading!")
            logger.critical(f"❌ Final capital: ₹{final_capital:,.2f}")
            logger.critical(f"❌ Minimum required: ₹{MIN_CAPITAL_REQUIRED:,.2f}")
            logger.critical("❌ System cannot operate safely with such low capital")
            logger.critical("❌ Shutting down immediately to prevent losses")
            
            if LIVE_DATA:
                logger.critical("❌ Please add more funds to your trading account")
            else:
                logger.critical("❌ Please increase starting_capital in config file")
            
            raise SystemExit(f"Insufficient capital: ₹{final_capital:,.2f} < ₹{MIN_CAPITAL_REQUIRED:,.2f}")

        # Store LIVE_DATA as instance variable for use in other methods
        self.LIVE_DATA = LIVE_DATA

        # Store final capital
        starting_capital = final_capital
        logger.debug(f"✅ Capital validation successful: ₹{starting_capital:,.2f}")
        logger.info(f"Using Starting Capital: ₹{starting_capital:,.2f} ({'Live' if LIVE_DATA else 'Simulated'} mode)")

        # Initialize risk manager
        self.risk_manager = RiskManager(
            total_capital=starting_capital,
            trade_counter=0,
            risk_per_trade=config["risk_management"]["risk_per_trade"],
            max_allocation=config["risk_management"]["max_allocation"],
            max_active_trades=config["risk_management"].get("max_active_trades", 5),
            config=self.config,
            margin_dict=response.json()["data"],
        )

        # Initialize trade manager
        self.trade_manager = TradeManager(
            risk_manager=self.risk_manager,
            trade_executor=None,
            max_active_trades=config["risk_management"].get("max_active_trades", 5),
            shared_state=self.shared_state,
            shared_lock=self.shared_lock,
            config=self.config,
            margin_dict=self.shared_state["margin_dict"],
            manager=self.manager,
        )

        # Create worker queues and workers
        self.worker_queues = {i: Queue() for i in range(self.num_workers)}
        for i in range(self.num_workers):
            worker = Worker(
                worker_id=i,
                symbols=self.worker_symbols[i],
                in_queue=self.worker_queues[i],
                trade_signal_queue=self.trade_signal_queue,
                config=config,
                margin_dict=self.shared_state["margin_dict"],
                capital_manager=self.risk_manager,
                trade_manager=None,  # self.trade_manager,
                shared_state=self.shared_state,
                shared_lock=self.shared_lock,
            )
            self.workers.append(worker)

        # Initialize dispatcher
        self.dispatcher = DataDispatcher(self.symbols, self.worker_assignment)
        for i, q in self.worker_queues.items():
            self.dispatcher.register_worker_queue(i, q)

        self.order_status_manager = OrderStatusManager(self.shared_state, self.shared_lock)
        
        # Validate shared state and start health monitoring
        if validate_shared_state(self.shared_state, self.shared_lock, "driver"):
            logger.debug("Shared state validation passed for Driver")
            self.health_monitor_thread = monitor_manager_health(self.shared_state, self.shared_lock)
        else:
            logger.error("Shared state validation failed for Driver - this may cause issues!")

    def start_workers(self):
        """Start all worker processes"""
        for worker in self.workers:
            worker.start()

    def run_data_feed(self):
        """
        Sets up the live or simulated data feed and dispatches market data.
        Enhanced with termination handling.
        """
        if self.LIVE_DATA or os.environ["SIMULATION_TYPE"] == "live":
            def on_message(data):
                self.dispatcher.dispatch(data)

            websocket = FyersBroker(
                data_handler=None, symbols=[f"NSE:{s}-EQ" for s in self.symbols]
            )
            websocket._on_ws_message = on_message
            websocket.connect_websocket()
        else:
            def on_message(ws, data):
                self.dispatcher.dispatch(json.loads(data))

            websocket = SimulatedWebSocket(handler=None)
            websocket.on_message = on_message
            websocket.connect_websocket()
            websocket.subscribe(symbols=[f"NSE:{s}-EQ" for s in self.symbols])

    def process_trade_signals(self):
        """
        Central loop that processes trade signals coming from workers.
        Also starts the order status polling manager.
        """
        logger.debug("Processing trade signals")
        self.order_status_manager.start()
        
        # Initialize time tracking for periodic status printing
        last_time_print = time.time()
        time_print_interval = 30  # Print every 30 seconds
        
        try:
            while True:
                try:
                    signal = self.trade_signal_queue.get()
                    
                    # Print current signal time every 30 seconds
                    current_time = time.time()
                    if current_time - last_time_print >= time_print_interval:
                        signal_time_str = signal.get('time', 'Unknown')
                        if hasattr(signal.get('time'), 'strftime'):
                            signal_time_str = signal['time'].strftime('%Y-%m-%d %H:%M:%S')
                        logger.info(f"📅 Current Signal Time: {signal_time_str} | System Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                        last_time_print = current_time
                    
                    with self.lock:
                        logger.debug(f"Processing trade signal: {signal}")
                        self.trade_manager.process_signal(signal)
                except Exception:
                    continue
        finally:
            self.order_status_manager.stop()

    def run(self):
        """
        Start the workers, trade signal processing, and market data feed.
        """
        try:
            self.start_workers()
            Thread(target=self.run_data_feed, daemon=True).start()
            self.process_trade_signals()
        except KeyboardInterrupt:
            logger.info("[DRIVER] Interrupting Trading.")
            self.shutdown()

    def shutdown(self):
        """Gracefully shut down all workers and clean up resources"""
        logger.info("[DRIVER] Shutting down workers...")
        self.running = False

        # Terminate all worker processes
        for i, worker in enumerate(self.workers):
            try:
                logger.info(f"[DRIVER] Terminating worker {i}...")
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)

                    # If worker didn't terminate gracefully, force kill it
                    if worker.is_alive():
                        logger.warning(
                            f"[DRIVER] Worker {i} didn't terminate gracefully, force killing..."
                        )
                        worker.kill()
                        worker.join(timeout=1)
            except Exception as e:
                logger.error(f"Error terminating worker {i}: {str(e)}")

        # Close all queues
        for i, queue in self.worker_queues.items():
            try:
                logger.info(f"[DRIVER] Closing queue for worker {i}...")
                queue.close()
                queue.join_thread()
            except Exception as e:
                logger.error(f"Error closing queue for worker {i}: {str(e)}")

        logger.info("[DRIVER] Shutdown complete.")
