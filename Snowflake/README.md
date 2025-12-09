## **❄️ Snowflake Setup & Workflow**

This folder contains all Snowflake SQL scripts used in the Olist Kappa Pipeline project. The scripts are organized by purpose: Iceberg table setup, Data Warehouse modeling, streams, tasks, and stored procedures.

Follow the steps below to set up and populate your Snowflake environment.

## 1️⃣ Iceberg Table Setup – The Foundation ❄️

File: iceberg_setup.sql
Purpose: Prepare Snowflake to access your Iceberg data on S3.

What to do:

Create External Volume – Connect Snowflake to your S3 bucket.

Create Iceberg Catalog – Manage Iceberg table metadata.

Create External Iceberg Table – Point Snowflake to the Iceberg data for querie
