import sqlite3

def create_db():
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('data/system_metrics.db')
    c = conn.cursor()

    # Create table for raw metrics
    c.execute('''
        CREATE TABLE IF NOT EXISTS raw_metrics (
            machine_id TEXT,
            timestamp DATETIME,
            cpu_usage REAL,
            memory_usage REAL,
            disk_io_read REAL,
            disk_io_write REAL,
            net_io_sent REAL,
            net_io_recv REAL,
            PRIMARY KEY (machine_id, timestamp)
        )
    ''')

    # Create table for aggregated metrics
    c.execute('''
        CREATE TABLE IF NOT EXISTS aggregated_metrics (
            timestamp DATETIME PRIMARY KEY,
            avg_cpu_usage REAL,
            avg_memory_usage REAL,
            avg_disk_io_read REAL,
            avg_disk_io_write REAL,
            avg_net_io_sent REAL,
            avg_net_io_recv REAL
        )
    ''')

    # Create table for top processes
    c.execute('''
        CREATE TABLE IF NOT EXISTS top_processes (
            timestamp DATETIME,
            process_name TEXT,
            cpu_usage REAL,
            memory_usage REAL,
            PRIMARY KEY (timestamp, process_name)
        )
    ''')
    
    # Create table for anomalies
    c.execute('''
        CREATE TABLE IF NOT EXISTS anomalies (
            timestamp DATETIME PRIMARY KEY,
            metric_type TEXT,
            value REAL,
            message TEXT
        )
    ''')

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_db()
    print("Database and tables created successfully.")
