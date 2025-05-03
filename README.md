# Instacart ETL Pipeline

This project demonstrates a real-time **ETL (Extract, Transform, Load)** data pipeline using modern open-source technologies, designed to simulate a real-world streaming data environment.

---

## 🚀 Project Overview

This ETL pipeline simulates processing order data for a grocery delivery platform (like Instacart). It streams real-time order events using Kafka, processes them using Flink, orchestrates the workflow using Airflow, and stores transformed data in Snowflake.

---

## 🧱 Tech Stack

| Component     | Technology Used                         |
|--------------|------------------------------------------|
| **Data Streaming** | Apache Kafka (Dockerized)              |
| **Stream Processing** | Apache Flink (Dockerized, PyFlink)     |
| **Orchestration** | Apache Airflow (Dockerized)            |
| **Storage**       | PostgreSQL (for metadata), Snowflake |
| **Environment**   | Docker, Docker Compose               |

---

## 🗂️ Directory Structure

```bash
instacart-etl/
├── dags/                       # Airflow DAGs
├── flink_jobs/                # Flink Python jobs
├── jobs/                      # Python Kafka producer & consumer
├── Dockerfile, docker-compose.yml
├── requirements.txt
└── .env, .gitignore
