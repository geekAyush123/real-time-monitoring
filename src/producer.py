import psutil
import time
import json
from kafka import KafkaProducer
import random

# Kafka configuration
KAFKA_TOPIC = 'system_metrics'
KAFKA_BROKER = 'localhost:9092'

def get_system_metrics(machine_id):
    """
    Gathers system metrics using psutil.
    """
    # CPU Usage
    cpu_usage = psutil.cpu_percent(interval=1)

    # Memory Usage
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.percent

    # Disk I/O
    disk_io = psutil.disk_io_counters()
    disk_io_read = disk_io.read_bytes
    disk_io_write = disk_io.write_bytes

    # Network Traffic
    net_io = psutil.net_io_counters()
    net_io_sent = net_io.bytes_sent
    net_io_recv = net_io.bytes_recv

    # Top processes
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
        try:
            processes.append({
                'pid': proc.info['pid'],
                'name': proc.info['name'],
                'cpu_percent': proc.info['cpu_percent'],
                'memory_percent': proc.info['memory_percent']
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    # Sort processes by CPU and Memory
    top_cpu_processes = sorted(processes, key=lambda p: p['cpu_percent'], reverse=True)[:5]
    top_mem_processes = sorted(processes, key=lambda p: p['memory_percent'], reverse=True)[:5]

    return {
        'machine_id': machine_id,
        'timestamp': time.time(),
        'cpu_usage': cpu_usage,
        'memory_usage': memory_usage,
        'disk_io': {'read': disk_io_read, 'write': disk_io_write},
        'network_io': {'sent': net_io_sent, 'recv': net_io_recv},
        'top_cpu_processes': top_cpu_processes,
        'top_mem_processes': top_mem_processes
    }

def create_producer():
    """
    Creates a Kafka producer with retry logic.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            request_timeout_ms=30000,
            api_version_auto_timeout_ms=30000,
            acks='all'
        )
        print("Kafka producer created successfully.")
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

def on_send_success(record_metadata):
    print(f"Message sent to topic '{record_metadata.topic}' partition {record_metadata.partition} at offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Error sending message: {excp}")

def main():
    """
    Main function to produce and send system metrics to Kafka.
    """
    producer = create_producer()
    if not producer:
        print("Failed to create Kafka producer. Exiting.")
        return

    machine_id = f"machine_{random.randint(1, 100)}"
    print(f"Starting producer for {machine_id}...")

    while True:
        try:
            metrics = get_system_metrics(machine_id)
            future = producer.send(KAFKA_TOPIC, metrics)
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
            
            # Flush messages to send them immediately
            producer.flush() 
            
            print(f"Attempted to send metrics: CPU {metrics['cpu_usage']}%")
            time.sleep(2)  # Send metrics every 2 seconds
        except KeyboardInterrupt:
            print("Stopping producer.")
            break
        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
            time.sleep(5)

    producer.close()

if __name__ == "__main__":
    main()
