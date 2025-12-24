 ## **❄️ Snowflake Setup & Workflow**
 
 Before starting, you must create an **AWS IAM Role** with the following:

- **Permission:** Access to the S3 bucket where your Iceberg data resides  
- **Trust Relationship:** Allow Snowflake to assume this role  

> This IAM role will be referenced in Snowflake’s External Volume.


This folder contains all Snowflake SQL scripts used in the Olist Kappa Pipeline project. The scripts are organized by purpose: Iceberg table setup, Data Warehouse modeling, streams, tasks, and stored procedures.

Follow the steps below to set up and populate your Snowflake environment.

---

### 1️⃣ Iceberg Table Setup – The Foundation
**File:** `iceberg_setup.sql`  
**Purpose:** Prepare Snowflake to access your Iceberg data on S3  

**Steps:**
1. **Create External Volume** – Connect Snowflake to your S3 bucket  
2. **Create Iceberg Catalog** – Manage Iceberg table metadata  
3. **Create External Iceberg Table** – Point Snowflake to the Iceberg data for queries 

> **Note:** After following these steps, you can test with:

```SQL
SELECT *
FROM OLIST_TABLE
```

## **2️⃣ Data Warehouse Modeling – Give Structure to the Chaos**
**Files:**: Datawarehouse creation,Inserting data into DWH

**What it does:**
- Creates a structured Data Warehouse from raw marketplace data
- Defines **dimension tables**: `dim_customer`, `dim_seller`, `dim_product`, `dim_date`
- Defines **fact tables**: `fact_order_line`, `fact_payment`
- Implements **galaxy schema design** for efficient analytics
- Applies **Slowly Changing Dimension (SCD) Type 1** logic for dimensions
- Uses **Snowflake Streams and Tasks** to incrementally update dimension and fact tables, ensuring **auto-refresh** and simulating real-time streaming data
- Ensures data consistency and integrity with **MERGE-based incremental loading**
- Provides a **single source of truth** for both historical and real-time analytics
- Ensures keys & relationships are correctly set for analytics.
  
> **Note:** Run after Iceberg. Execute each file in order, as the warehouse depends on structured data.

## 3️⃣ **Streams – Eyes on the Data**
**File:** streams.sql

**What it does:**
- Sets up Snowflake Streams on Iceberg tables

- Tracks new rows to feed the incremental pipeline

- Powers the real-time capabilities of the Kappa Architecture

> **Note:** Run once tables exist and Iceberg data is ready.


## **4️⃣ Stored Procedures – Automate & Refresh**

**File:** stored_procedure.sql
**What it does:**

- Includes procedures like REFRESH_ICEBERG_METADATA_SP.

- Keeps Iceberg tables fresh and ready for queries.

- Powers the incremental workflow behind the scenes.

> **Note:** Run after streams, before tasks.


## **5️⃣ Tasks – Your Auto-Pilot**

**File:** tasks.sql
**What it does:**

. Automates incremental loads for dimensions and facts.

. Leverages streams & procedures to insert only new data.

. Runs on schedule (e.g., every 15 min) – no manual effort required.

> **Note:** Run last – let Airflow or Snowflake handle the pipeline for you.

> **💡 Tip:** Always follow the execution order:  
> `Iceberg → DWH Modeling → Streams → Stored Procedures → Tasks`  
> Your warehouse depends on structured data to function properly.
