## ** Olist Data Pipeline – Project README
## 📌 Overview

This repository contains the implementation of a Kappa Architecture that processes Olist marketplace data using Kafka, Apache Spark, Iceberg, Airflow, and Snowflake.

The project ingests raw streaming data, transforms it in real time, stores it in Iceberg for optimized metadata handling, and then orchestrates daily/real-time workflows using Airflow. Snowflake is used as the BI and reporting layer.

