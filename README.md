# Analytic_stream

`Analytic_stream` is a comprehensive local analytics pipeline designed to demonstrate the ingestion, processing, and streaming of market data. It operates in 5-minute intervals and streams the data to a live dashboard. This project integrates multiple technologies to provide a robust framework for validating, storing, and visualizing real-time time-series data in a seamless and efficient manner.

---


## Features

- **Real-time Data Streaming**: Implements Kafka and Apache Flink for efficient streaming of market price data.
- **Data Integrity Assurance**: Ensures data validity through schema enforcement.
- **Flexible Deployment**: Adapts to lightweight storage solutions and real-time dashboard integration.
- **Analytics and Visualization**: Provides actionable insights through real-time data visualization.

---

## Getting Started

### Prerequisites

- [Python 3.8+]
- [Kafka]

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/dray19/Analytic_stream.git
    cd Analytic_stream
    ```

2. Install backend dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Ensure Kafka is running locally or in docker.
```bash
   docker compose up -d
   ## check if its running
   Check docker ps
```

4. Ensure Flink is setup.
```bash
   FLINK_HOME=$(./venv/bin/find_flink_home.py)
   export PATH=$PATH:$FLINK_HOME/bin
   ## if a version comes up everything is working
   flink --version
```

5. Create a topic in Kafka.
```bash
   kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic market-events
   ### check 
   kafka-topics --list --bootstrap-server localhost:9092
```

6. Activate the Python virtual environment. Open separate terminal windows to run Flink and the Kafka producer(s) and consumer(s), enabling live streaming of 5-minute data to the dashboard. 
```bash
   flink run --python flink_job.py --target local --jarfile flink-sql-connector-kafka-4.0.1-2.0.jar
   python -m consumer.consumer_dashboard_csv
   python -m producer.producer
   ### Using streamlit as the dashboard
   streamlit run dashboard_app.py
```

---

## Usage and Workflow

1. **Data Ingestion**
   - Stream raw time-series data into Kafka topics.

2. **Data Validation**
   - Validate incoming data against predefined schemas to ensure quality and integrity.

3. **Stream Processing with Flink**
   - Apache Flink consumes Kafka events, normalizes nested records, and performs real-time aggregation and windowing.
   - Processed results are published to a downstream Kafka topic for analytics and dashboard updates.

4. **Real-time Dashboard**
   - Visualize actionable insights via a modern dashboard built with Streamlit.

---