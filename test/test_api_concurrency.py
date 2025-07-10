import os
import time
import requests
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("OPENALGO_BENCHMARK_URL", "http://127.0.0.1:5000/api/v1/orderstatus/")
API_KEY = os.getenv("APP_KEY")
ORDER_ID = os.getenv("OPENALGO_ORDER_ID", "25053100001145") 
print(API_URL, API_KEY, ORDER_ID)
# Number of concurrent processes and total requests
NUM_PROCESSES = int(os.getenv("OPENALGO_BENCHMARK_PROCESSES", cpu_count()))
NUM_REQUESTS = int(os.getenv("OPENALGO_BENCHMARK_REQUESTS", 100))

payload = {
    "apikey": API_KEY,
    "orderid": ORDER_ID,
    'strategy': 'test'
    # Add other fields as required by your OrderStatusSchema
}

headers = {"Content-Type": "application/json"}

def make_request(_):
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=10)
        return response.status_code, response.elapsed.total_seconds()
    except Exception as e:
        return "error", str(e)

def main():
    print(f"Benchmarking {NUM_REQUESTS} requests with {NUM_PROCESSES} processes to {API_URL}...")
    start = time.time()
    with Pool(processes=NUM_PROCESSES) as pool:
        results = pool.map(make_request, range(NUM_REQUESTS))
    end = time.time()

    # Analyze results
    success = sum(1 for r in results if r[0] == 200)
    errors = [r for r in results if r[0] != 200]
    avg_time = sum(float(r[1]) for r in results if r[0] == 200) / max(success, 1)

    print(f"Total requests: {NUM_REQUESTS}")
    print(f"Successful responses: {success}")
    print(f"Failed responses: {len(errors)}")
    print(f"Average response time (successes): {avg_time:.3f} seconds")
    print(f"Total elapsed time: {end - start:.2f} seconds")
    if errors:
        print("Sample errors:", errors[:5])

if __name__ == "__main__":
    main()