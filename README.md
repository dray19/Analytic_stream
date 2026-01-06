# Analytic_stream

`Analytic_stream` is an end-to-end local analytics pipeline designed as a demo showing how market data is ingested, processed, and streamed in 5-minute intervals to a live dashboard. It integrates several technologies to provide an adaptable framework for validating, storing, and visualizing real-time time-series data as it arrives.

---

## Features

- **Real-time Streaming with Kafka**: Stream market price data efficiently.
- **Data Validation**: Enforces schemas to ensure data integrity.
- **Flexible Consumption**: Supports lightweight storage systems or real-time dashboards.
- **Analytics and Visualization**: Gain insights with real-time data visualization.

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

4. Create a topic in Kafka.
```bash
   kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic market-events
   ### check 
   kafka-topics --list --bootstrap-server localhost:9092
```

5. Activate the Python virtual environment. Open separate terminal windows to run the Kafka producer(s) and consumer(s), enabling live streaming of 5-minute data to the dashboard. 
```bash
   python -m consumer.consumer_stats
   python -m consumer.consumer_dashboard_csv
   python -m producer.producer
   ### Using streamlit as the dashboard
   streamlit run dashboard_app.py
```

---

## Usage

1. **Data Ingestion**:
   - Stream time-series data into Kafka topics.
2. **Data Validation**:
   - Schemas ensure that incoming data conforms to predefined standards.
3. **Real-time Insight**:
   - Access the lightweight storage/database or explore the dashboard visualization in real time.

---