## **📘 Table of Contents**
📌 Project Overview

🏗️ Architecture

🗂️ Data Modeling

⚡ Streaming Pipeline

❄️ Iceberg Metadata & Versioning

📊 Analytics Layer

🚀 Airflow Orchestration

📁 Repository Structure

▶️ How to Run

🛠️ Future Enhancements

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

No SCD needed for order status (already time-stamped)

No SCD for city/state (low-cardinality, no business impact)

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

``` SQL
CALL REFRESH_ICEBERG_METADATA_SP(
  'table_name',
  'external_volume_path'
); 





