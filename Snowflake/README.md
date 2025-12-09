 ## **❄️ Snowflake Setup & Workflow**

This folder contains all Snowflake SQL scripts used in the Olist Kappa Pipeline project. The scripts are organized by purpose: Iceberg table setup, Data Warehouse modeling, streams, tasks, and stored procedures.

Follow the steps below to set up and populate your Snowflake environment.

## **1️⃣ Iceberg Table Setup – The Foundation**
 
File: iceberg_setup.sql
Purpose: Prepare Snowflake to access your Iceberg data on S3.

What to do:

1. Create External Volume – Connect Snowflake to your S3 bucket.

2. Create Iceberg Catalog – Manage Iceberg table metadata.

3. Create External Iceberg Table – Point Snowflake to the Iceberg data for querie

## NOTE :After following these steps you can now try
```SQL
SELECT *
FROM OLIST_TABLE
```

## **2️⃣ Data Warehouse Modeling – Give Structure to the Chaos**
Folder: Data Warehouse Modeling
Files: dim_customer.sql,dim_seller.sql,dim_product.sql,dim_date.sql,fact_order_line.sql,fact_payment.sql
What it does:

Creates dimension tables (dim_customer, dim_seller, dim_product, dim_date).

Creates fact tables (fact_order_line, fact_payment) with business-ready grain.

Ensures keys & relationships are correctly set for analytics.
## NOTE :Run after iceberg – your warehouse is hungry for structured data and run each file in its excution order

## 3️⃣ **Streams – Eyes on the Data**
File: streams.sql
What it does:

Sets up Snowflake streams on Iceberg tables.

Tracks new rows to feed your incremental pipeline.

Ignites the real-time magic of your Kappa architecture.

Run once tables exist and Iceberg data is ready.


## **4️⃣ Stored Procedures – Automate & Refresh**

File: stored_procedure.sql
What it does:

Includes procedures like REFRESH_ICEBERG_METADATA_SP.

Keeps Iceberg tables fresh and ready for queries.

Powers the incremental workflow behind the scenes.

## NOTE: Run after streams, before tasks.


## **5️⃣ Tasks – Your Auto-Pilot**

File: tasks.sql
What it does:

. Automates incremental loads for dimensions and facts.

. Leverages streams & procedures to insert only new data.

. Runs on schedule (e.g., every 15 min) – no manual effort required.

Run last – let Airflow or Snowflake handle the pipeline for you.




Run after Iceberg – your warehouse is hungry for structured data!
