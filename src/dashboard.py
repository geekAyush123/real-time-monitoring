import streamlit as st
import pandas as pd
import time
from cache import Cache
import sqlite3
from sklearn.linear_model import LinearRegression
import numpy as np

# Page configuration
st.set_page_config(page_title="Real-Time System Monitor", layout="wide")

# Initialize cache and db connection
cache = Cache()

def get_db_connection():
    return sqlite3.connect('data/system_metrics.db')

def predict_usage(metric_name):
    """
    Predicts future usage using linear regression.
    """
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT timestamp, {metric_name} FROM aggregated_metrics ORDER BY timestamp DESC LIMIT 100", conn)
    conn.close()

    if len(df) < 2:
        return None

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['time_delta'] = (df['timestamp'] - df['timestamp'].min()).dt.total_seconds()
    
    X = df[['time_delta']]
    y = df[metric_name]

    model = LinearRegression()
    model.fit(X, y)

    future_time = df['time_delta'].max() + 300 # 5 minutes
    prediction = model.predict(np.array([[future_time]]))
    return prediction[0]

def main():
    st.title("Real-Time System Activity Monitor")

    # Placeholders for real-time updates
    cpu_chart_placeholder = st.empty()
    mem_chart_placeholder = st.empty()
    disk_chart_placeholder = st.empty()
    net_chart_placeholder = st.empty()
    
    col1, col2 = st.columns(2)
    with col1:
        top_cpu_placeholder = st.empty()
    with col2:
        top_mem_placeholder = st.empty()
        
    alerts_placeholder = st.empty()
    
    # Predictive Analytics
    st.sidebar.header("Predictions (next 5 mins)")
    cpu_pred = predict_usage('avg_cpu_usage')
    mem_pred = predict_usage('avg_memory_usage')
    if cpu_pred:
        st.sidebar.write(f"Predicted CPU Usage: {cpu_pred:.2f}%")
    if mem_pred:
        st.sidebar.write(f"Predicted Memory Usage: {mem_pred:.2f}%")

    # Historical Data
    st.sidebar.header("Historical Data")
    time_filter = st.sidebar.selectbox("Time Range", ["Last 5 mins", "Last 15 mins", "Last 1 hour"])

    while True:
        # Fetch data
        metrics = cache.get_latest_metrics(count=300) # last 5 minutes of data (1 point per minute)
        top_cpu = cache.get_top_processes('cpu')
        top_mem = cache.get_top_processes('mem')
        anomalies = cache.get_anomalies()

        if metrics:
            df = pd.DataFrame(metrics)
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)  # Make timezone-naive
            df['cpu_usage'] = pd.to_numeric(df['cpu_usage'])
            df['memory_usage'] = pd.to_numeric(df['memory_usage'])


            # Filter based on selection - Skip for now to avoid timezone issues
            # now = pd.Timestamp.now()
            # time_delta = pd.Timedelta(minutes=5)
            # if time_filter == "Last 15 mins":
            #     time_delta = pd.Timedelta(minutes=15)
            # elif time_filter == "Last 1 hour":
            #     time_delta = pd.Timedelta(hours=1)
            
            # df = df[df['timestamp'] > now - time_delta]
            
            # For now, just show the latest data we have
            df = df.tail(50)  # Show last 50 data points
            
            # Charts
            cpu_chart_placeholder.line_chart(df.set_index('timestamp')['cpu_usage'])
            mem_chart_placeholder.line_chart(df.set_index('timestamp')['memory_usage'])
            
            # Disk and Network need to be fetched from db for historical view
            conn = get_db_connection()
            hist_df = pd.read_sql("SELECT * FROM aggregated_metrics ORDER BY timestamp DESC LIMIT 100", conn)
            conn.close()
            hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp'])
            
            disk_chart_placeholder.line_chart(hist_df.set_index('timestamp')[['avg_disk_io_read', 'avg_disk_io_write']])
            net_chart_placeholder.line_chart(hist_df.set_index('timestamp')[['avg_net_io_sent', 'avg_net_io_recv']])

        if top_cpu:
            df_cpu = pd.DataFrame(top_cpu, columns=['Process', 'CPU %'])
            top_cpu_placeholder.bar_chart(df_cpu.set_index('Process'))
        
        if top_mem:
            df_mem = pd.DataFrame(top_mem, columns=['Process', 'Memory %'])
            top_mem_placeholder.bar_chart(df_mem.set_index('Process'))

        if anomalies:
            alerts_placeholder.warning("High Usage Alerts:")
            for alert in anomalies:
                try:
                    value = float(alert['value'])
                    alerts_placeholder.write(f"- {alert['message']} ({alert['metric']}: {value:.2f}%) at {pd.to_datetime(alert['timestamp']).strftime('%H:%M:%S')}")
                except (ValueError, KeyError):
                    alerts_placeholder.write(f"- {alert.get('message', 'Unknown alert')} at {alert.get('timestamp', 'Unknown time')}")

        time.sleep(2)

if __name__ == "__main__":
    main()
