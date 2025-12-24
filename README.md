# 📦 Olist Streaming Pipeline
### ITI Graduation Project

*A Kappa Architecture-based pipeline for real-time and historical analytics on Olist marketplace data. Tech stack includes Kafka, Spark Streaming, Iceberg, Snowflake, Kibana, and Power BI.*

## 👤 My Contribution
- Configured and managed Apache Iceberg integration with Snowflake, including metadata synchronization
- Implemented automated Iceberg metadata refresh in Snowflake
- Designed and modeled the Data Warehouse schema using fact and dimension tables
- Built and managed Snowflake Streams and Tasks for incremental data ingestion, automatic refresh of tables, and simulating real-time streaming data for testing
- Developed MERGE-based loading logic for efficient updates



## **📘 Table of Contents**
📌 Project Overview

🏗️ Architecture

🗂️ Data Modeling

⚡ Streaming Pipeline

❄️ Iceberg Metadata & Versioning

📊 Analytics Layer

📊 Business Impact & Recommendations

🚀 Airflow Orchestration

📁 Repository Structure

▶️ How to Run

🤝 Contributing

📜 License


## 📌 Project Overview
This project implements a real-time and historical analytics pipeline using a modern Kappa Architecture. It processes Olist marketplace data using:

Kafka for event streaming

Spark Structured Streaming for data processing

Apache Iceberg on AWS S3 as the lakehouse storage layer

Airflow for orchestration

Snowflake as the analytics warehouse

Kibana for real-time visualization

Power BI for BI reporting

The pipeline delivers both live operational dashboards and deep historical analytics, making it ideal for marketplace, e‑commerce, and data‑intensive platforms.



## 🏗️ Architecture

Below is the high-level architecture describing ingestion, real-time serving, Iceberg storage, and BI analytics.
<img width="1484" height="648" alt="archticture" src="https://github.com/user-attachments/assets/747cab34-015c-4657-bcfb-61d8aaaf9f44" />

## 🗂️ Data Modeling

Dimensions:
dim_customer
dim_seller
dim_product
dim_date

Facts:
fact_order_line
fact_payment

Modeling Notes:

Order-level grain using item detail

SCD Type 1

Snowflake Streams + Tasks handle incremental loading, capturing new Iceberg changes and applying them through scheduled MERGE operations

Below is a photo of our DWH schema
<img width="3662" height="1676" alt="drawSQL-image-export-2025-12-05" src="https://github.com/user-attachments/assets/edad3a18-6c5d-4ccd-a43b-0d94355bb287" />

## ⚡ Streaming Pipeline

**Kafka Producer**:

 -Reads CSV files incrementally

 -Sends JSON events to Kafka topic

**Spark Structured Streaming**:

 -Consumes Kafka messages

 -Applies transformations

 -Writes to two sinks:

  Elasticsearch → real-time dashboards

  Iceberg → historical analytics

## ❄️ Iceberg Metadata & Versioning

   -Iceberg enables:

   -Time Travel

  -ACID operations

  -Schema evolution

 -Snapshot-based versioning

Snowflake Metadata Refresh Procedure:

```sql
CALL REFRESH_ICEBERG_METADATA_SP(
  'table_name',
  'external_volume_path'
);
```
This ensures Snowflake always reads the latest Iceberg metadata JSON.

## 📊 Analytics Layer
**Real-Time Dashboards (Kibana):**
Most used payment methods

High-demand product categories

High-demand regions

Live monitoring of spikes or drops in orders on the platform

Track business growth (e.g., total revenue, orders, customers over time)

Track top-selling categories live

View order status distributions instantly

<img width="929" height="526" alt="kibana" src="https://github.com/user-attachments/assets/c4405f66-8f7b-4f6d-8269-80315e2deda0" />


**Historical Analysis (Snowflake + Power BI):**

Delivery efficiency

Highest-demanding product categories over time

Customer purchasing behavior (e.g., churn rate, repeat purchases)

Delivery performance over time

Tracking revenue and Financial KPIs

Demand forecasting and inventory management planning with sellers

Key Power BI Dashboards:

Executive Summary: High-level KPIs & Volumes (e.g., monthly orders, delivery efficiency, top categories, top sellers/customers, maps)

Sales & Revenue: Monthly revenue, freight value, average order value, revenue by payment type, installments

Customer Behavior: Repeat rate, churn, inactive customers, insights by state, high-value customers, growth per year

Product Analysis: Most selling categories, revenue/price per category/product

Logistics & Fulfillment: Average delivery time, on-time rate, undelivered orders, duration by day, freight by state, category shipping analysis

Undelivered Orders Drillthrough: End-to-end analysis, seller/carrier verification, customer orders analysis


<img width="1332" height="739" alt="power_bi" src="https://github.com/user-attachments/assets/bdae9e00-cb71-4257-add8-3befabf3470d" />

## 📊Business Impact & Recommendations

Faster delivery → higher order volume & satisfaction

Prioritize top-performing sellers

Re-engage inactive customers

Focus inventory & promotions on top categories

Reduce freight costs in high-volume states (SP, RJ, MG)

## 🚀 Airflow Orchestration

Airflow is used to orchestrate:

-Kafka Producer startup and monitoring

-Spark Consumer/Streaming Job triggers

-Iceberg metadata updates

-Dimension upserts

-Fact table refreshes

DAGs follow dependency chaining to ensure end‑to‑end flow from producer → consumer → Iceberg → Snowflake.

<img width="889" height="435" alt="dag" src="https://github.com/user-attachments/assets/5051d327-db3f-4f86-beab-bf9291fe6532" />


## ▶️ How to Run
**1. Start Docker Services**
```Bash
docker compose up -d
```
**2. Start Kafka Producer**
```Bash
python producer/producer.py
```
**3. Start Spark Streaming Job**

1-To elastic
```Bash
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
 Transformation Code\Spark_Consumer_To_Elastic.py
```
2- To s3
```Bash
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  Transformation Code\Spark_Consumer_To_S3.py
```



## 🤝 Contributing

Pull requests are welcome.

## 📜 License

MIT License

