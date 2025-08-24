#!/usr/bin/env python3
"""
Simple Kafka consumer to replace PySpark streaming for Windows compatibility.
This consumer processes messages from Kafka and stores them in Redis and SQLite.
"""

import json
import sqlite3
import redis
from kafka import KafkaConsumer
from datetime import datetime
import time
import logging

# Configuration
KAFKA_TOPIC = "system_metrics"
KAFKA_BROKER = "localhost:9092"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
DB_PATH = 'data/system_metrics.db'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_redis_connection():
    """Get Redis connection"""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def get_db_connection():
    """Get SQLite connection"""
    return sqlite3.connect(DB_PATH)

def process_message(message_value, r, conn):
    """Process a single message from Kafka"""
    try:
        # Parse JSON data
        data = json.loads(message_value)
        
        # Extract fields
        machine_id = data.get('machine_id', 'unknown')
        timestamp = data.get('timestamp', time.time())
        cpu_usage = data.get('cpu_usage', 0.0)
        memory_usage = data.get('memory_usage', 0.0)
        disk_io = data.get('disk_io', {})
        network_io = data.get('network_io', {})
        top_cpu_processes = data.get('top_cpu_processes', [])
        top_mem_processes = data.get('top_mem_processes', [])
        
        # Create timestamp string
        dt = datetime.fromtimestamp(timestamp)
        ts_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Store in Redis with TTL (10 minutes)
        redis_key = f"metrics:{machine_id}:{ts_str}"
        r.hset(redis_key, mapping={
            "machine_id": machine_id,
            "timestamp": ts_str,
            "cpu_usage": str(cpu_usage),
            "memory_usage": str(memory_usage),
            "disk_io_read": str(disk_io.get('read', 0)),
            "disk_io_write": str(disk_io.get('write', 0)),
            "network_io_sent": str(network_io.get('sent', 0)),
            "network_io_recv": str(network_io.get('recv', 0))
        })
        r.expire(redis_key, 600)  # 10 minutes TTL
        
        # Store in SQLite
        c = conn.cursor()
        
        # Insert raw metrics
        c.execute('''
            INSERT OR REPLACE INTO raw_metrics 
            (machine_id, timestamp, cpu_usage, memory_usage, disk_io_read, disk_io_write, net_io_sent, net_io_recv)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (machine_id, ts_str, cpu_usage, memory_usage, 
              disk_io.get('read', 0), disk_io.get('write', 0),
              network_io.get('sent', 0), network_io.get('recv', 0)))
        
        # Store aggregated metrics (simple 1-minute aggregation)
        minute_ts = dt.replace(second=0).strftime('%Y-%m-%d %H:%M:%S')
        c.execute('''
            INSERT OR REPLACE INTO aggregated_metrics 
            (timestamp, avg_cpu_usage, avg_memory_usage, avg_disk_io_read, avg_disk_io_write, avg_net_io_sent, avg_net_io_recv)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (minute_ts, cpu_usage, memory_usage,
              disk_io.get('read', 0), disk_io.get('write', 0),
              network_io.get('sent', 0), network_io.get('recv', 0)))
        
        # Process top CPU processes
        for proc in top_cpu_processes[:5]:  # Top 5
            if proc and isinstance(proc, dict):
                proc_name = proc.get('name', 'unknown')
                cpu_percent = proc.get('cpu_percent', 0.0)
                mem_percent = proc.get('memory_percent', 0.0)
                
                # Store in Redis
                r.zadd(f"top_cpu:{minute_ts}", {proc_name: cpu_percent})
                r.expire(f"top_cpu:{minute_ts}", 600)
                
                # Store in SQLite
                c.execute('''
                    INSERT OR REPLACE INTO top_processes (timestamp, process_name, cpu_usage, memory_usage)
                    VALUES (?, ?, ?, ?)
                ''', (minute_ts, proc_name, cpu_percent, mem_percent))
        
        # Anomaly detection
        if cpu_usage > 80:  # Lowered threshold for testing
            metric, value, msg = "CPU", cpu_usage, f"High CPU usage detected: {cpu_usage:.1f}%"
            anomaly_key = f"anomaly:{minute_ts}:cpu"
            r.hset(anomaly_key, mapping={"metric": metric, "value": str(value), "message": msg})
            r.expire(anomaly_key, 600)
            c.execute('INSERT OR REPLACE INTO anomalies (timestamp, metric_type, value, message) VALUES (?, ?, ?, ?)', 
                     (minute_ts, metric, value, msg))
            logger.warning(f"ANOMALY: {msg}")
        
        if memory_usage > 80:  # Lowered threshold for testing
            metric, value, msg = "Memory", memory_usage, f"High Memory usage detected: {memory_usage:.1f}%"
            anomaly_key = f"anomaly:{minute_ts}:mem"
            r.hset(anomaly_key, mapping={"metric": metric, "value": str(value), "message": msg})
            r.expire(anomaly_key, 600)
            c.execute('INSERT OR REPLACE INTO anomalies (timestamp, metric_type, value, message) VALUES (?, ?, ?, ?)', 
                     (minute_ts, metric, value, msg))
            logger.warning(f"ANOMALY: {msg}")
        
        conn.commit()
        logger.info(f"Processed metrics for {machine_id} at {ts_str} - CPU: {cpu_usage:.1f}%, Memory: {memory_usage:.1f}%")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        logger.error(f"Message content: {message_value}")

def main():
    """Main consumer loop"""
    logger.info("Starting simple Kafka consumer...")
    
    # Initialize connections
    try:
        r = get_redis_connection()
        r.ping()  # Test Redis connection
        logger.info("Redis connection established")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return
    
    # Create Kafka consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',  # Start from latest messages
            value_deserializer=lambda x: x.decode('utf-8')
        )
        logger.info(f"Kafka consumer created for topic: {KAFKA_TOPIC}")
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return
    
    # Process messages
    try:
        conn = get_db_connection()
        logger.info("Starting message processing...")
        
        for message in consumer:
            process_message(message.value, r, conn)
            
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()
