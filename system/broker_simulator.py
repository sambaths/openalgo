import websocket
import threading, time, json


class SimulatedWebSocket:
    """
    Simulated WebSocket client mimicking the FyerWebSocket interface.
    Connects to a simulated WebSocket server and continuously streams data,
    passing received messages to the handler's data_queue.
    """

    def __init__(self, handler):
        self.handler = handler
        self.uri = "ws://localhost:8080"  # URL of the simulated WebSocket server
        self.ws = None
        self.running = False

    def on_message(self, ws, message):
        # print(message)
        try:
            data = json.loads(message)
        except Exception:
            data = message
        # Pass the data to the handler's data_queue
        self.handler.data_queue.put(data)

    def on_error(self, ws, error):
        print("WebSocket error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        self.running = False
        print("WebSocket closed.")

    def on_open(self, ws):
        print("Connected to simulated WebSocket server!")

    def connect_websocket(self):
        """
        Establish the connection to the simulated WebSocket server.
        This method runs the WebSocket in a background thread.
        """
        self.ws = websocket.WebSocketApp(
            self.uri,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.running = True
        # Start the WebSocket run_forever loop in a separate thread.
        self.thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self.thread.start()
        # Give the connection time to establish
        time.sleep(1)

    def subscribe(self, symbols, keep_running=True):
        """
        Mimics subscription behavior.
        In a real WebSocket client, you might send a subscription message here.
        For simulation, we simply print the symbols and keep the method blocking.
        """
        print("Subscribed to symbols:", symbols)
        # Block and keep the connection alive, just like the original implementation.
        try:
            while keep_running and self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.disconnect()

    def disconnect(self):
        """Close the WebSocket connection."""
        if self.ws:
            self.ws.close()
        self.running = False
