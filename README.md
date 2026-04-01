# 🚗 Digital Twin Observability Platform

A real-time data engineering platform that simulates vehicle telemetry data, processes it using Kafka and Spark Structured Streaming, and builds an observability layer to monitor data quality, anomalies, and pipeline health.

---

## 🚀 Project Highlights

- Built an end-to-end real-time streaming pipeline using Kafka and Spark
- Designed a **Digital Twin simulation** for vehicle telemetry data
- Implemented **data observability layer** (data quality + alerts + pipeline health)
- Created APIs using FastAPI and a dashboard using Streamlit
- Containerized entire system using Docker for reproducibility

---

## 🏗️ Architecture

Simulator → Kafka → Spark Structured Streaming → Parquet → FastAPI → Streamlit UI

![Architecture](images/architecture.png)

---

## ⚙️ Tech Stack

- Apache Kafka (event streaming)
- Apache Spark Structured Streaming
- Docker (containerized environment)
- FastAPI (data serving layer)
- Streamlit (dashboard)
- Parquet (data lake storage)

---

## 🔥 Features

- Real-time vehicle telemetry ingestion
- Digital twin simulation (vehicle state tracking)
- Data quality checks:
  - Null detection
  - High engine temperature alerts
  - Overspeed detection
- Pipeline observability:
  - Batch processing time monitoring
  - Alerting for anomalies
- API layer for serving processed data
- Live dashboard for monitoring system state

---

## 🧠 Observability Layer (Core Highlight)

Unlike typical streaming projects, this system includes built-in observability:

- 📊 Data Quality Metrics per batch
- 🚨 Alert generation (high temp, nulls, overspeed)
- ⏱️ Pipeline latency tracking
- 🚗 Active vehicles tracking by location

---

## 📂 Data Flow

- **Raw Events** → Kafka topic (`vehicle_telemetry`)
- **Processed Data** → `/data/vehicle_telemetry`
- **Aggregations** → `/data/vehicle_activity`
- **Observability Metrics** → `/data/observability`

---

## ⚠️ Challenges Faced

### 🔧 Spark + Docker Filesystem Issues
Running Spark in cluster mode with local filesystem caused write failures due to executor/driver mismatch.  
Resolved by switching to **local mode execution**, ensuring consistent filesystem access.

---

### 🧩 Kafka Networking in Docker
Configuring Kafka listeners for both internal containers and external access required proper setup of:
- `KAFKA_ADVERTISED_LISTENERS`
- Multiple ports (internal vs external)

---

### ⚠️ Data Consistency Between Spark & API
Initial setup had mismatched volumes, causing API to not see newly written data.  
Resolved by using a **shared `/data` volume across all services**.

---

### 🔁 Streaming Debugging Complexity
Debugging streaming pipelines required:
- Checking Kafka consumers
- Validating offsets
- Monitoring Spark UI
- Handling checkpoint behavior

---

## 🚀 How to Run

### 1. Start all services

```bash
docker-compose -f infrastructure/docker-compose.yml up --build

### 2. To Debug or check logs of each services 

docker logs -f <service name>

### Some commands to check if Kafka consumer topic is working well or not

docker exec -it kafka bash
kafka-console-consumer --bootstrap-server kafka:9092 --topic vehicle_telemetry

### Streamlit UI Runs on 
http://localhost:8501