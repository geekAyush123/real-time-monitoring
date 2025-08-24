# Real-Time System Activity Monitoring & Analytics Platform

This project is a comprehensive platform for monitoring and analyzing system activity in real-time. It uses a distributed architecture with Python, Kafka, PySpark, Redis, and Streamlit to track, process, and visualize system metrics.

## Features

- **Real-Time Monitoring**: Tracks CPU, memory, disk, and network usage every second.
- **Distributed Processing**: Uses Kafka and PySpark for scalable, high-throughput stream processing.
- **Real-Time Dashboard**: An interactive Streamlit dashboard visualizes metrics and alerts.
- **Caching**: Redis provides fast access to recent metrics for the dashboard.
- **Persistent Storage**: SQLite stores historical data for trend analysis.
- **Anomaly Detection**: Identifies and alerts on high resource usage (CPU > 90%, Memory > 90%).
- **Predictive Analytics**: A simple machine learning model predicts future resource usage.
- **Desktop Notifications**: Sends alerts for critical system events.

## Architecture

1.  **`producer.py`**: A Python script using `psutil` to collect system metrics and send them to a Kafka topic. You can run multiple instances to simulate monitoring multiple machines.
2.  **Kafka**: Acts as a message broker, ingesting high-volume logs from producers.
3.  **`streaming.py`**: A PySpark Structured Streaming application that consumes logs from Kafka, performs aggregations (e.g., 1-minute averages), detects anomalies, and stores results in Redis and SQLite.
4.  **Redis**: A fast in-memory cache that stores the last 10 minutes of aggregated data for quick retrieval by the dashboard.
5.  **SQLite**: A lightweight database for long-term storage of historical metrics.
6.  **`dashboard.py`**: A Streamlit web application that queries Redis and SQLite to display real-time charts, top processes, and alerts.
7.  **`alerts.py`**: A script that checks for anomalies and sends desktop notifications.

## How to Run

### Prerequisites

-   Docker and Docker Compose
-   Python 3.8+ and pip
-   Java (for PySpark)

### 1. Setup Environment

Clone the repository and install the required Python packages:

```bash
git clone https://github.com/geekAyush123/real-time-monitoring.git
cd real-time-monitoring
pip install -r requirements.txt
```

### 2. Start Infrastructure

Run the `docker-compose.yml` file to start Kafka and Redis:

```bash
docker-compose up -d
```

This will start:
- Zookeeper on port `2181`
- Kafka on port `9092`
- Redis on port `6379`

The Kafka container is configured to automatically create the `system_metrics` topic.

### 3. Initialize Database

Run the `database.py` script to create the SQLite database and tables:

```bash
python src/database.py
```

### 4. Start the Components

Open multiple terminal windows to run each component of the platform.

**Terminal 1: Start the Stream Processor**

This will listen for messages from Kafka and process them.

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 src/streaming.py
```

**Terminal 2: Start the Producer**

This will start sending your local machine's metrics to Kafka. You can open more terminals and run this command to simulate multiple producers.

```bash
python src/producer.py
```

**Terminal 3: Start the Alerting Service (Optional)**

```bash
python src/alerts.py
```

**Terminal 4: Start the Dashboard**

```bash
streamlit run src/dashboard.py
```

Now, open your web browser and go to the URL provided by Streamlit (usually `http://localhost:8501`) to see the real-time dashboard.

## Scaling

-   **Kafka**: To handle more producers, you can increase the number of partitions for the `system_metrics` topic. This can be done by modifying the `KAFKA_CREATE_TOPICS` environment variable in `docker-compose.yml` or by using the `kafka-topics.sh` script.
-   **PySpark**: You can scale the stream processing by increasing the number of executors in your Spark cluster. When running locally, you can simulate this by adjusting the `--num-executors` and other configuration flags in the `spark-submit` command. For a production setup, you would run this on a proper Spark cluster (like YARN or Kubernetes).

## Tech Stack

- **Python**: Core programming language
- **Apache Kafka**: Message streaming platform
- **Apache Spark**: Distributed computing framework
- **Redis**: In-memory data store for caching
- **SQLite**: Lightweight database for persistence
- **Streamlit**: Web framework for the dashboard
- **Docker**: Containerization platform
- **psutil**: System monitoring library

## Project Structure

```
├── src/
│   ├── producer.py          # System metrics producer
│   ├── streaming.py         # Spark streaming processor
│   ├── dashboard.py         # Streamlit dashboard
│   ├── gradio_dashboard.py  # Alternative Gradio dashboard
│   ├── database.py          # Database initialization
│   ├── alerts.py            # Alert system
│   ├── cache.py             # Redis cache utilities
│   ├── simple_consumer.py   # Simple Kafka consumer
│   └── test_pipeline.py     # Pipeline testing
├── data/
│   └── system_metrics.db    # SQLite database
├── jars/                    # Spark JAR dependencies
├── docker-compose.yml       # Infrastructure setup
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Contributing

Feel free to fork this repository and submit pull requests for any improvements!


