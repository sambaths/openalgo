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

from dotenv import load_dotenv

load_dotenv()


import logging
from logger import logger

logger.setLevel(getattr(logging, os.environ["DEBUG_LEVEL"].upper(), None))

import threading
import time


class OrderStatusManager:
    def __init__(self, shared_state, poll_interval=1):
        self.shared_state = shared_state
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
        logger.info(f"DataDispatcher initialized")

    def register_worker_queue(self, worker_idx, queue):
        self.worker_queues[worker_idx] = queue
        logger.info(f"Worker queue registered for worker {worker_idx}")

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
        self.num_workers = 8
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

        # Set up shared state
        manager = Manager()
        self.shared_state = manager.dict()
        self.shared_state["active_trades"] = 0
        self.trade_signal_queue = manager.Queue()
        self.shared_state["active_order_ids"] = []  # List of all order IDs to track
        self.shared_state["order_status_dict"] = {}  # Dict of order_id -> status

        # Retrieve margin information
        payload = {"apikey": os.getenv("APP_KEY"), "symbols": self.symbols}
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{os.getenv('HOST_SERVER')}/api/v1/intradaymargin/", json=payload
        )
        self.shared_state["margin_dict"] = response.json()["data"]

        logger.info(f"Using Starting Capital from Config file")
        starting_capital = config["capital_management"]["starting_capital"]

        # Initialize risk manager
        self.risk_manager = RiskManager(
            total_capital=starting_capital,
            trade_counter=0,
            risk_per_trade=config["risk_management"]["risk_per_trade"],
            max_allocation=config["risk_management"]["max_allocation"],
            max_active_trades=config["risk_management"].get("max_active_trades", 5),
            config=self.config,
        )

        # Initialize trade manager
        self.trade_manager = TradeManager(
            risk_manager=self.risk_manager,
            trade_executor=None,
            max_active_trades=config["risk_management"].get("max_active_trades", 5),
            shared_state=self.shared_state,
            config=self.config,
            margin_dict=self.shared_state["margin_dict"],
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
            )
            self.workers.append(worker)

        # Initialize dispatcher
        self.dispatcher = DataDispatcher(self.symbols, self.worker_assignment)
        for i, q in self.worker_queues.items():
            self.dispatcher.register_worker_queue(i, q)

        self.order_status_manager = OrderStatusManager(self.shared_state)

    def start_workers(self):
        """Start all worker processes"""
        for worker in self.workers:
            worker.start()
        print("workers started")

    def run_data_feed(self):
        """
        Sets up the live or simulated data feed and dispatches market data.
        """
        LIVE_DATA = os.environ.get(
            "LIVE_DATA", str(self.config.get("LIVE_DATA", False))
        ).lower() in ["true", "1"]
        if LIVE_DATA:

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
        print("Processing trade signals")
        self.order_status_manager.start()
        try:
            while True:
                try:
                    signal = self.trade_signal_queue.get(timeout=1)
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
