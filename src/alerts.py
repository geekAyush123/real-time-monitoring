import time
from cache import Cache
from plyer import notification

def check_alerts():
    """
    Checks for new anomalies and sends desktop notifications.
    """
    cache = Cache()
    notified_anomalies = set()

    while True:
        anomalies = cache.get_anomalies(count=1) # Check the very latest
        if anomalies:
            latest_anomaly = anomalies[0]
            anomaly_id = f"{latest_anomaly['timestamp']}-{latest_anomaly['metric']}"

            if anomaly_id not in notified_anomalies:
                notification.notify(
                    title="System Alert!",
                    message=f"{latest_anomaly['message']}\n{latest_anomaly['metric']}: {float(latest_anomaly['value']):.2f}%",
                    timeout=10
                )
                notified_anomalies.add(anomaly_id)
        
        time.sleep(10) # Check every 10 seconds

if __name__ == "__main__":
    print("Starting alert checker...")
    check_alerts()
