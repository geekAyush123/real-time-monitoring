import psutil
import time
import json
import random
from collections import deque

# --- Configuration ---
SIMULATION_DURATION = 20  # seconds
METRICS_INTERVAL = 1  # seconds
AGGREGATION_WINDOW = 5  # seconds

# --- In-Memory Cache (Simulating Redis) ---
cache = {
    "metrics": deque(maxlen=60),  # Store last 60 aggregated points
    "anomalies": deque(maxlen=10),
    "top_cpu": deque(maxlen=10),
}

# --- 1. Producer Logic ---
def get_system_metrics(machine_id):
    """Gathers system metrics using psutil."""
    cpu_usage = psutil.cpu_percent(interval=None)
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.percent

    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
        try:
            processes.append(proc.info)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    top_cpu_processes = sorted(processes, key=lambda p: p.get('cpu_percent', 0), reverse=True)[:5]

    return {
        'machine_id': machine_id,
        'timestamp': time.time(),
        'cpu_usage': cpu_usage,
        'memory_usage': memory_usage,
        'top_cpu_processes': top_cpu_processes,
    }

# --- 2. Simplified Streaming/Processing Logic ---
def process_and_aggregate(metrics_buffer):
    """
    Simplified aggregation logic, mimicking Spark.
    Averages metrics over the aggregation window.
    """
    if not metrics_buffer:
        return None

    # Calculate averages
    avg_cpu = sum(m['cpu_usage'] for m in metrics_buffer) / len(metrics_buffer)
    avg_mem = sum(m['memory_usage'] for m in metrics_buffer) / len(metrics_buffer)
    
    # Get the latest process list from the window
    latest_procs = metrics_buffer[-1]['top_cpu_processes']

    aggregated_metric = {
        "timestamp": int(time.time()),
        "avg_cpu_usage": round(avg_cpu, 2),
        "avg_memory_usage": round(avg_mem, 2),
    }
    
    # --- 3. Anomaly Detection ---
    if avg_cpu > 90.0:
        anomaly = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "metric": "CPU",
            "value": avg_cpu,
            "message": "High CPU usage detected"
        }
        cache["anomalies"].append(anomaly)

    if avg_mem > 90.0:
        anomaly = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "metric": "Memory",
            "value": avg_mem,
            "message": "High Memory usage detected"
        }
        cache["anomalies"].append(anomaly)

    # --- 4. Storing in Cache ---
    cache["metrics"].append(aggregated_metric)
    cache["top_cpu"].append({
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "processes": latest_procs
    })
    
    return aggregated_metric

# --- 5. Main Simulation Loop ---
def main():
    """
    Runs the entire pipeline in a single, simplified script.
    """
    print("--- Starting Single-File Pipeline Test ---")
    machine_id = f"test_machine_{random.randint(1, 100)}"
    
    raw_metrics_buffer = []
    start_time = time.time()
    last_aggregation_time = start_time

    while time.time() - start_time < SIMULATION_DURATION:
        # 1. Produce metric
        metric = get_system_metrics(machine_id)
        raw_metrics_buffer.append(metric)
        print(f"[{time.strftime('%H:%M:%S')}] Produced: CPU {metric['cpu_usage']:.1f}%, MEM {metric['memory_usage']:.1f}%")

        # 2. Process and aggregate if window is full
        if time.time() - last_aggregation_time >= AGGREGATION_WINDOW:
            aggregated = process_and_aggregate(raw_metrics_buffer)
            if aggregated:
                print(f"[{time.strftime('%H:%M:%S')}] *** Aggregated: Avg CPU {aggregated['avg_cpu_usage']}%, Avg MEM {aggregated['avg_memory_usage']}% ***")
            
            # Reset for next window
            raw_metrics_buffer = []
            last_aggregation_time = time.time()

        time.sleep(METRICS_INTERVAL)

    # --- 6. Display Final Cache Contents (Simulating Dashboard) ---
    print("\n--- Simulation Finished. Final Cache State: ---")
    print("\n--- Aggregated Metrics (last 10) ---")
    for m in list(cache['metrics'])[-10:]:
        print(m)
        
    print("\n--- Top CPU Processes (latest) ---")
    if cache['top_cpu']:
        latest_top_cpu = cache['top_cpu'][-1]
        for p in latest_top_cpu['processes']:
            print(f"  - {p.get('name', 'N/A'):<25} | CPU: {p.get('cpu_percent', 0):.2f}%")

    print("\n--- Anomalies ---")
    if not cache["anomalies"]:
        print("No anomalies detected.")
    for a in cache["anomalies"]:
        print(a)

if __name__ == "__main__":
    main()
