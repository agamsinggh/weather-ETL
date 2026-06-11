**Weather ETL Pipeline**
----------------------------------------------
A production-style automated ETL pipeline that collects real-time weather data hourly, transforms it, and stores it in a relational database — fully containerized and monitored via a live dashboard.

**Architecture**
---
wttr.in REST API → Apache Airflow → PostgreSQL → Metabase

**Tech Stack**
--
Apache Airflow --> Workflow orchestration and scheduling

PostgreSQL --> Relational data storage

Python --> ETL business logic

Docker & Docker Compose --> Service containerization

Metabase --> BI and data visualization

**Features**
--
Fully automated hourly data ingestion via Airflow DAG
Modular ETL design — Extract, Transform, Load as independent Airflow tasks
Idempotent schema setup — table created automatically on first run
Containerized with Docker Compose — single command setup
Live Metabase dashboard with time-series temperature visualization
Task-level logging and monitoring via Airflow UI

**DAG Overview**
--
The pipeline runs hourly with 3 tasks in sequence: extract_task → transform_task → load_task

**extract_task** — Hits the wttr.in API and pulls current temperature for Delhi

**transform_task** — Converts Celsius to Fahrenheit, passes data between tasks via XCom

**load_task** — Inserts the transformed record into PostgreSQL via Airflow's PostgresHook



**Local Setup**
-
Prerequisites: Docker Desktop (Apple Silicon supported)

Clone the repository

Run echo "AIRFLOW_UID=$(id -u)" > .env

Run docker compose up -d

Open Airflow at http://localhost:8080 (airflow / airflow)

Open Metabase at http://localhost:3000

**Running the Pipeline**
-
Open Airflow UI at http://localhost:8080

Toggle the weather_etl DAG to ON

Trigger manually or let it run on its hourly schedule
Monitor task-level logs in the Graph view
