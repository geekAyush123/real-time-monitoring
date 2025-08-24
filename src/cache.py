import redis

class Cache:
    def __init__(self, host='localhost', port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def get_latest_metrics(self, count=5):
        """
        Retrieves the latest 'count' minutes of aggregated metrics.
        """
        keys = sorted(self.r.keys("metrics:*"), reverse=True)[:count]
        metrics = []
        for key in keys:
            data = self.r.hgetall(key)
            # Extract timestamp from key format: metrics:machine_id:timestamp
            parts = key.split(':', 2)
            if len(parts) >= 3:
                data['timestamp'] = parts[2]  # Get the timestamp part
                data['machine_id'] = parts[1]  # Get the machine_id part
                metrics.append(data)
        return metrics

    def get_top_processes(self, metric_type='cpu', count=5):
        """
        Retrieves the top 'count' processes for a given metric type.
        """
        keys = sorted(self.r.keys(f"top_{metric_type}:*"), reverse=True)
        if not keys:
            return []
        latest_key = keys[0]
        return self.r.zrevrange(latest_key, 0, count - 1, withscores=True)

    def get_anomalies(self, count=5):
        """
        Retrieves the latest 'count' anomalies.
        """
        keys = sorted(self.r.keys("anomaly:*"), reverse=True)[:count]
        anomalies = []
        for key in keys:
            data = self.r.hgetall(key)
            # Extract timestamp from key format: anomaly:timestamp:metric_type
            # Key format is like: anomaly:2025-08-24 10:06:00:mem
            parts = key.split(':', 1)  # Split only on the first colon
            if len(parts) >= 2:
                # Remove 'anomaly:' prefix and split the rest to get timestamp and metric type
                remaining = parts[1]  # This should be "2025-08-24 10:06:00:mem"
                last_colon_index = remaining.rfind(':')
                if last_colon_index > 0:
                    data['timestamp'] = remaining[:last_colon_index]  # Get timestamp part
                    data['metric_type'] = remaining[last_colon_index+1:]  # Get metric type
                else:
                    data['timestamp'] = remaining
                    data['metric_type'] = 'unknown'
                anomalies.append(data)
        return anomalies

if __name__ == '__main__':
    cache = Cache()
    print("Latest Metrics:", cache.get_latest_metrics())
    print("Top CPU Processes:", cache.get_top_processes('cpu'))
    print("Anomalies:", cache.get_anomalies())
