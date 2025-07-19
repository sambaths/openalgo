import os
import requests
import json

from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("APP_KEY")
# BROKER_API_KEY = os.getenv("BROKER_API_KEY")
SYMBOLS = ["SBIN", "HDFCBANK", "RELIANCE"]

# Set the broker API key in the environment for the BrokerData class
# os.environ["BROKER_API_KEY"] = BROKER_API_KEY

def test_margin_api():
    payload = {
        "apikey": API_KEY,
        "symbols": SYMBOLS
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post("http://127.0.0.1:5000/api/v1/intradaymargin/", json=payload)
    # print("Headers:", response.headers)
    # print("Text:", response.text)
    # print("JSON:", response.json())
    # print("Status Code:", response.status_code)
    print("Response JSON:", response.json())
    assert response.status_code == 200
    assert "data" in response.json()

if __name__ == "__main__":
    test_margin_api()
    