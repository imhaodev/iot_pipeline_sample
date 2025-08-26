# Operation Guide - End-to-End IoT Data Pipeline

This document provides step-by-step instructions to launch and operate the IoT data pipeline. Please ensure you have the complete source code and project directory structure.

## Requirements

- Docker
- Docker Compose
- Python 3.8+ and pip (to run producer script)
- Git (for version control)

## Project Setup

### 1. Clone and Setup Environment

```bash
# Clone the repository (if using Git)
git clone <repository-url>
cd iot_pipeline

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Environment Configuration

The project includes:
- `.gitignore` - Excludes unnecessary files from version control
- `requirements.txt` - Lists all Python dependencies
- `.env` - Environment variables (configure as needed)

## Step 1: Start the Entire System

Open terminal at the project root directory and run the following command. This command will build the necessary images and start all background services.

```bash
docker-compose up -d --build
```

⏳ Please wait about 2-3 minutes for all containers to start completely.

## Step 2: Initialize Database & Topic (One-time setup)

Execute the following commands to create the necessary schemas and topics for the pipeline.

```bash
# 1. Create Kafka Topic
docker exec -it kafka kafka-topics --create --topic iot-telemetry --bootstrap-server localhost:9092

# 2. Create Cassandra Schema
docker exec -it cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS iot_platform WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
docker exec -it cassandra cqlsh -e "USE iot_platform; CREATE TABLE IF NOT EXISTS raw_sensor_data (sensor_id text, date text, timestamp timestamp, temperature float, PRIMARY KEY ((sensor_id, date), timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);"

# 3. Create PostgreSQL Schema
docker exec -it postgres_analytics psql -U admin -d analytics_db -c "CREATE TABLE IF NOT EXISTS daily_avg_temperature (sensor_id TEXT, report_date DATE, avg_temperature FLOAT, PRIMARY KEY (sensor_id, report_date));"
```

## Step 3: Run Real-time Stream (Data Collection)

You need to open 2 separate terminal windows.

### Terminal 1: Run Producer to Generate Data

```bash
# Dependencies are already installed from requirements.txt
# If you haven't installed them yet, run:
# pip install -r requirements.txt

# Start sending data
python producer/iot_producer.py
```

This window will continuously print out the sensor data being sent. Keep it running.

### Terminal 2: Run Spark Streaming to Process Data

```bash
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
  /opt/bitnami/spark/apps/pyspark_streaming_to_cassandra.py
```


**Note for Windows users (Git Bash):** If you encounter `no such file or directory` error, add two forward slashes `//` at the beginning of the python file path (e.g.: `//opt/bitnami/spark/apps/...`).

This window will print logs "Batch ... written to Cassandra". Raw data is being written to Cassandra. Keep it running.

## Step 4: Run Batch ETL Stream (Data Aggregation)

1. **Access Airflow UI:** Open browser to http://localhost:8081
   - User: `admin`
   - Password: `admin`

2. **Activate and Run DAG:**
   - Find the DAG named `iot_cassandra_to_postgres_etl_v2` and enable it using the toggle switch.
   - Click on the DAG name to enter.
   - Click the Play button (▶) in the top right corner to trigger the DAG.
   - To run ETL for today, select "Trigger DAG w/ config" and enter in the JSON box:

```json
{ "logical_date": "2025-08-05T00:00:00Z" }
```

(Note: Replace `2025-08-05` with your current date).

3. **Monitor:**
   - In the "Grid" tab, you can see the task status. When the task turns green, it has succeeded.

## Step 5: View Visual Results

1. **Access Metabase UI:** Open browser to http://localhost:3001.

2. **Configure Metabase (First time):**
   - Create your admin account.
   - When prompted, connect to the database with the following information:
     - Database Type: `PostgreSQL`
     - Host: `postgres_analytics`
     - Port: `5432`
     - Database Name: `analytics_db`
     - Username: `admin`
     - Password: `password`

3. **Create Charts:**
   - From the homepage, click "+ New" → "Question".
   - Select "Raw Data" → "Analytics DB" → "Daily Avg Temperature".
   - Click the "Visualize" button to view results.
   - Select chart type as "Line" to view temperature trends by day.

## Other Management Interfaces

- **Spark Master UI:** http://localhost:8080 (Monitor Spark jobs)
- **Airflow UI:** http://localhost:8081 (Manage ETL workflows)

## Project Structure

```
iot_pipeline/
├── .gitignore              # Git ignore rules
├── requirements.txt        # Python dependencies
├── README.md              # This documentation
├── docker-compose.yml     # Docker services configuration
├── .env                   # Environment variables
├── producer/
│   └── iot_producer.py    # Kafka data producer
├── spark_apps/
│   ├── pyspark_streaming_to_cassandra.py  # Real-time processing
│   └── etl_cassandra_to_postgres.py       # Batch ETL
└── airflow/
    ├── Dockerfile         # Airflow container setup
    └── dags/             # Airflow DAG definitions
```

## Development Notes

- All Python dependencies are managed in `requirements.txt`
- The `.gitignore` file excludes logs, cache files, and sensitive data
- Virtual environment `iot_venv/` is excluded from version control
- Use `pip install -r requirements.txt` to install all dependencies

## Troubleshooting

- If containers fail to start, check Docker logs: `docker-compose logs <service-name>`
- For Python dependency issues, ensure you're using Python 3.8+
- If Kafka connection fails, verify all containers are running: `docker-compose ps`