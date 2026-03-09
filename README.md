# SaaS Subscription Lakehouse
**Apache Kafka · Apache Spark · Debezium · Delta Lake · dbt · Apache Airflow · PostgreSQL · Docker**

---

## Overview
This project implements a real-time SaaS analytics platform using modern data stack built with open-source technologies.
The platform ingests **subscription, payment, and invoice events** in real time, processes them using **Spark Structured Streaming**, stores them in **Delta Lake**, and transforms them with **dbt** to power business analytics such as:

- Monthly Recurring Revenue (MRR)
- Revenue by plan
- Churn tracking
- Daily active subscriptions

The entire platform is **fully containerized using Docker Compose** and orchestrated with **Apache Airflow**, creating a reproducible local data platform.

---

## Architecture

The system follows a **Lakehouse Medallion Architecture**.

```
Data Generator
        │
        ▼
PostgreSQL (Source DB)
        │
        ▼
Debezium CDC Connector
        │
        ▼
Kafka Topics
        │
        ▼
Spark Structured Streaming
        │
        ├── Bronze Layer (Raw Events)
        │
        ├── Silver Layer (Cleaned Data)
        │
        ▼
Delta Lake Storage
        │
        ▼
dbt Transformations
        │
        ├── Staging Models
        ├── Core Models
        └── Mart Models (Gold Layer)
        │
        ▼
Airflow Orchestration
```
---

## Components of the Platform

### PostgreSQL

PostgreSQL acts as the **source transactional database** representing a SaaS billing system.

Entities include:

- Users
- Plans
- Subscriptions
- Payments
- Invoices

The database is configured with logical replication (WAL) to enable Change Data Capture (CDC) using Debezium.

---

### Data Generator

To simulate real SaaS platform activity, the project includes a **synthetic data generator built with Python and Faker**.

The generator continuously creates realistic SaaS events such as:

- New user registrations
- Subscription creations and updates
- Payment transactions
- Invoice generation

These events populate the PostgreSQL database with transactional data that is captured using **Debezium CDC** and streamed into Kafka for downstream processing.

Generator scripts include:
```
users.py
subscriptions.py
payments.py
invoices.py
```

This enables the platform to simulate a **live SaaS environment for testing real-time data pipelines**.

---

### Debezium

Debezium captures **real-time changes from PostgreSQL WAL logs** and publishes them as events to Kafka topics.

This enables **CDC-based streaming ingestion** instead of batch extraction.

Captured events include:

- User creation
- Subscription updates
- Payment transactions
- Invoice generation

These events are automatically streamed into Kafka topics for downstream processing.

---

### Apache Kafka

Kafka acts as the **real-time event streaming platform**.

Topics include:

```
users
subscriptions
payments
invoices
```

Kafka enables asynchronous event streaming between the transactional system and the data lake.

---

### Apache Spark Structured Streaming

Spark processes Kafka events in real time and writes them into Delta Lake.

Streaming pipelines include:

#### Bronze Pipelines

```
bronze_users.py
bronze_subscriptions.py
bronze_payments.py
bronze_invoices.py
```

Bronze tables store **raw event data from Kafka**.

---

#### Silver Pipelines

```
silver_users.py
silver_subscriptions.py
silver_payments.py
silver_invoices.py
```

Silver tables provide **cleaned and structured datasets** for analytics.

---

### Delta Lake

Delta Lake provides the **Lakehouse storage layer**.

Benefits include:

- ACID transactions
- Schema enforcement
- Time travel
- Efficient analytics queries

Example storage paths:

```
/delta/bronze/users
/delta/bronze/subscriptions
/delta/silver/users_current
/delta/silver/subscriptions_current
```

---

### dbt Transformations

dbt transforms raw datasets into **analytics-ready models**.

The project follows a layered transformation approach.

---

#### Staging Models

Standardize source datasets.

```
staging_users
staging_subscriptions
staging_payments
staging_invoices
```

---

#### Core Models

Business logic transformations.

```
table_users
table_plans
table_subscription_snapshot
table_subscription_revenue
```

---

#### Mart Models

Final analytics tables used for reporting.

```
monthly_recurring_revenue
revenue_by_plan
daily_active_subscriptions
churn_events
```

These models power downstream dashboards and business insights.

---

### Apache Airflow

Airflow orchestrates the entire pipeline.

Airflow DAGs manage:

- Spark streaming jobs
- dbt transformations
- pipeline execution workflows

This ensures reliable pipeline scheduling and monitoring.

---

## Repository Structure

```
saas-subscription-lakehouse/
│
├── docker-compose.yaml
├── README.md
├── .gitignore
├── run_commands.txt
│
├── postgres/
│   └── init/
│       ├── 01_schema.sql
│       ├── 02_plans.sql
│       └── 03_constraints.sql
│
├── data-generator/
│   └── services/
│       ├── generate_users.py
│       ├── generate_subscriptions.py
│       ├── generate_payments.py
│       └── generate_invoices.py
│
├── kafka/
│   └── create_topics.sh
│
├── spark/
│   ├── bronze_users.py
│   ├── bronze_subscriptions.py
│   ├── bronze_payments.py
│   ├── bronze_invoices.py
│   │
│   ├── silver_users.py
│   ├── silver_subscriptions.py
│   ├── silver_payments.py
│   └── silver_invoices.py
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   └── models/
│        ├── staging/
│        ├── core/
│        └── marts/
│
├── airflow/
│   └── dags/
│       └── lakehouse_pipeline.py
│
└── powerbi/
    └── dashboards.pbix
```

---

## Running the Project

### 1. Start the Platform

Start all services using Docker Compose.

```
docker-compose up -d
```

This launches:

- PostgreSQL (source database)
- Zookeeper (Kafka coordination)
- Apache Kafka (event streaming)
- Kafka Connect with Debezium (CDC ingestion)
- Apache Spark cluster (master + worker)
- Apache Airflow (pipeline orchestration)

---

### 2. Start Spark Streaming Pipelines

Example:

```
docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/bronze_xxxx.py
```

Run all Bronze and Silver pipelines.

---

### 3. Run dbt Transformations

Navigate to dbt project:

```
cd dbt_project
```

Run transformations:

```
dbt run
```

Run tests:

```
dbt test
```

---

## Example Analytics Query:

Monthly Recurring Revenue:

```sql
SELECT *
FROM monthly_recurring_revenue;
```
---

## Future Improvements

Potential enhancements include:

- Build interactive Power BI dashboards for subscription analytics and revenue insights.
- Add data quality validation using Great Expectations.
- Deploy the pipeline to AWS or GCP cloud infrastructure.
- Implement CI/CD pipelines for dbt models.
- Enable real-time dashboard updates using streaming data.

---

## Author

**Hariharan Nadanasabapathi**



