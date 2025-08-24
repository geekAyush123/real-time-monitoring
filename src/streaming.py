from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, explode, split, first
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, ArrayType, DoubleType
import redis
import sqlite3

# Configuration
KAFKA_TOPIC = "system_metrics"
KAFKA_BROKER = "localhost:9092"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
DB_PATH = 'data/system_metrics.db'

# Schema for Kafka messages
schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("cpu_usage", FloatType(), True),
    StructField("memory_usage", FloatType(), True),
    StructField("disk_io", StructType([
        StructField("read", LongType(), True),
        StructField("write", LongType(), True)
    ]), True),
    StructField("network_io", StructType([
        StructField("sent", LongType(), True),
        StructField("recv", LongType(), True)
    ]), True),
    StructField("top_cpu_processes", ArrayType(StructType([
        StructField("pid", LongType(), True),
        StructField("name", StringType(), True),
        StructField("cpu_percent", FloatType(), True),
        StructField("memory_percent", FloatType(), True)
    ])), True),
    StructField("top_mem_processes", ArrayType(StructType([
        StructField("pid", LongType(), True),
        StructField("name", StringType(), True),
        StructField("cpu_percent", FloatType(), True),
        StructField("memory_percent", FloatType(), True)
    ])), True)
])

def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def process_batch(df, epoch_id):
    if df.count() == 0:
        return
    
    r = get_redis_connection()
    conn = get_db_connection()
    c = conn.cursor()

    collected_rows = df.collect()

    for row in collected_rows:
        ts = row['window']['start'].strftime('%Y-%m-%d %H:%M:%S')
        # Store in Redis with TTL
        r.hset(f"metrics:{ts}", mapping={
            "avg_cpu_usage": row['avg_cpu_usage'],
            "avg_memory_usage": row['avg_memory_usage']
        })
        r.expire(f"metrics:{ts}", 600) # 10 minutes TTL

        # Store in SQLite
        c.execute('''
            INSERT OR REPLACE INTO aggregated_metrics 
            (timestamp, avg_cpu_usage, avg_memory_usage, avg_disk_io_read, avg_disk_io_write, avg_net_io_sent, avg_net_io_recv)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (ts, row['avg_cpu_usage'], row['avg_memory_usage'], row['avg_disk_io_read'], row['avg_disk_io_write'], row['avg_net_io_sent'], row['avg_net_io_recv']))

        # Top processes
        if row['top_cpu_processes']:
            for proc in row['top_cpu_processes']:
                # Store in Redis
                r.zadd(f"top_cpu:{ts}", {proc['name']: proc['cpu_percent']})
                r.expire(f"top_cpu:{ts}", 600)
                # Store in SQLite
                c.execute('''
                    INSERT OR REPLACE INTO top_processes (timestamp, process_name, cpu_usage, memory_usage)
                    VALUES (?, ?, ?, ?)
                ''', (ts, proc['name'], proc['cpu_percent'], proc['memory_percent']))

        # Anomaly detection
        if row['avg_cpu_usage'] > 90:
            metric, value, msg = "CPU", row['avg_cpu_usage'], "High CPU usage detected"
            r.hset(f"anomaly:{ts}:cpu", mapping={"metric": metric, "value": str(value), "message": msg})
            r.expire(f"anomaly:{ts}:cpu", 600)
            c.execute('INSERT OR REPLACE INTO anomalies (timestamp, metric_type, value, message) VALUES (?, ?, ?, ?)', (ts, metric, value, msg))

        if row['avg_memory_usage'] > 90:
            metric, value, msg = "Memory", row['avg_memory_usage'], "High Memory usage detected"
            r.hset(f"anomaly:{ts}:mem", mapping={"metric": metric, "value": str(value), "message": msg})
            r.expire(f"anomaly:{ts}:mem", 600)
            c.execute('INSERT OR REPLACE INTO anomalies (timestamp, metric_type, value, message) VALUES (?, ?, ?, ?)', (ts, metric, value, msg))

    conn.commit()
    conn.close()


def main():
    spark = SparkSession.builder \
        .appName("SystemMonitoring") \
        .config("spark.jars", "jars/spark-sql-kafka-0-10_2.12-3.3.0.jar,jars/kafka-clients-2.8.1.jar,jars/commons-pool2-2.11.1.jar,jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse JSON data
    metrics_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Tumbling window aggregation
    aggregated_df = metrics_df.withWatermark("timestamp", "2 minutes") \
        .groupBy(window(col("timestamp").cast("timestamp"), "1 minute")) \
        .agg(
            avg("cpu_usage").alias("avg_cpu_usage"),
            avg("memory_usage").alias("avg_memory_usage"),
            avg("disk_io.read").alias("avg_disk_io_read"),
            avg("disk_io.write").alias("avg_disk_io_write"),
            avg("network_io.sent").alias("avg_net_io_sent"),
            avg("network_io.recv").alias("avg_net_io_recv"),
            # Collect top processes for the window
            first("top_cpu_processes").alias("top_cpu_processes"),
            first("top_mem_processes").alias("top_mem_processes")
        )

    # Write to our sink
    query = aggregated_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
