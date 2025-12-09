-- =====================================================
-- ❄️ Snowflake Tasks for Olist Kappa Architecture
-- =====================================================
-- Notes:
-- 1. Make sure the OLIST_WAREHOUSE and OLIST databases exist.
-- 2. Ensure the streams (olist_iceberg_stream) are created.
-- 3. Tasks are grouped by dimension updates first, then fact loads.
-- 4. Dependencies between tasks are set using AFTER.
-- =====================================================

-- ==============================
-- DIMENSION TASKS
-- ==============================

-- Task: Upsert Customer Dimension
CREATE OR REPLACE TASK task_upsert_dim_customer
  WAREHOUSE = SNOWFLAKE_LEARNING_WH
  WHEN SYSTEM$STREAM_HAS_DATA('OLIST.PUBLIC.olist_iceberg_stream')
AS
MERGE INTO OLIST.PUBLIC.DIM_CUSTOMER tgt
USING (
  SELECT
    customer_unique_id AS customer_unique_id_bk,
    customer_id        AS customer_id_sk,
    customer_city,
    customer_state
  FROM OLIST.PUBLIC.olist_iceberg_stream
  WHERE customer_unique_id IS NOT NULL
) src
ON tgt.customer_unique_id_bk = src.customer_unique_id_bk
WHEN MATCHED THEN
  UPDATE SET
    customer_id_sk = src.customer_id_sk,
    customer_city  = src.customer_city,
    customer_state = src.customer_state
WHEN NOT MATCHED THEN
  INSERT (customer_id_sk, customer_unique_id_bk, customer_city, customer_state)
  VALUES (src.customer_id_sk, src.customer_unique_id_bk, src.customer_city, src.customer_state);

-- Task: Upsert Seller Dimension
CREATE OR REPLACE TASK task_upsert_dim_seller
  WAREHOUSE = SNOWFLAKE_LEARNING_WH
  WHEN SYSTEM$STREAM_HAS_DATA('OLIST.PUBLIC.olist_iceberg_stream')
AS
MERGE INTO OLIST_WAREHOUSE.PUBLIC.DIM_SELLER tgt
USING (
  SELECT
    seller_id   AS seller_id_bk,
    seller_city,
    seller_state
  FROM OLIST.PUBLIC.olist_iceberg_stream
  WHERE seller_id IS NOT NULL
) src
ON tgt.seller_id_bk = src.seller_id_bk
WHEN MATCHED THEN
  UPDATE SET
    seller_city  = src.seller_city,
    seller_state = src.seller_state
WHEN NOT MATCHED THEN
  INSERT (seller_id_bk, seller_city, seller_state)
  VALUES (src.seller_id_bk, src.seller_city, src.seller_state);

-- Task: Upsert Product Dimension
CREATE OR REPLACE TASK task_upsert_dim_product
  WAREHOUSE = SNOWFLAKE_LEARNING_WH
  WHEN SYSTEM$STREAM_HAS_DATA('OLIST.PUBLIC.olist_iceberg_stream')
AS
MERGE INTO OLIST_WAREHOUSE.PUBLIC.DIM_PRODUCT tgt
USING (
  SELECT
    product_id            AS product_id_bk,
    product_category_name
  FROM OLIST.PUBLIC.olist_iceberg_stream
  WHERE product_id IS NOT NULL
) src
ON tgt.product_id_bk = src.product_id_bk
WHEN MATCHED THEN
  UPDATE SET
    product_category_name = src.product_category_name
WHEN NOT MATCHED THEN
  INSERT (product_id_bk, product_category_name)
  VALUES (src.product_id_bk, src.product_category_name);

-- Task: Upsert Date Dimension
CREATE OR REPLACE TASK task_upsert_dim_date
  WAREHOUSE = SNOWFLAKE_LEARNING_WH
  WHEN SYSTEM$STREAM_HAS_DATA('OLIST.PUBLIC.olist_iceberg_stream')
AS
MERGE INTO OLIST.PUBLIC.DIM_DATE tgt
USING (
  SELECT DISTINCT
    DATE_TRUNC('DAY', order_purchase_timestamp)::DATE AS date_key
  FROM OLIST_WAREHOUSE.PUBLIC.olist_iceberg_stream
  WHERE order_purchase_timestamp IS NOT NULL
) src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN
  INSERT (
    date_key, year, quarter_number, quarter_name, month_number, month_name, month_name_short,
    day_of_month, day_of_week_number, day_of_week_name, day_of_week_short,
    week_of_year_iso, week_year_iso, is_weekend, holiday_name
  )
  VALUES (
    src.date_key,
    YEAR(src.date_key),
    QUARTER(src.date_key),
    'Q' || QUARTER(src.date_key),
    MONTH(src.date_key),
    MONTHNAME(src.date_key),
    TO_CHAR(src.date_key, 'Mon'),
    DAY(src.date_key),
    DAYOFWEEK(src.date_key) + 1,
    DAYNAME(src.date_key),
    LEFT(DAYNAME(src.date_key), 3),
    WEEKOFYEAR(src.date_key),
    YEAROFWEEK(src.date_key),
    CASE WHEN DAYOFWEEK(src.date_key) IN (1,7) THEN 1 ELSE 0 END,
    CASE TO_CHAR(src.date_key, 'MM-DD')
        WHEN '01-01' THEN 'New Year'
        WHEN '04-21' THEN 'Tiradentes'
        WHEN '05-01' THEN 'Labor Day'
        WHEN '09-07' THEN 'Independence Day'
        WHEN '10-12' THEN 'Nossa Sra. Aparecida'
        WHEN '11-02' THEN 'Finados'
        WHEN '11-15' THEN 'Republic Proclamation'
        WHEN '12-25' THEN 'Christmas'
        ELSE NULL
    END
  );

-- ==============================
-- FACT TASKS
-- ==============================

-- Task: Load Fact Payments
CREATE OR REPLACE TASK task_load_fact_payments
  WAREHOUSE = SNOWFLAKE_LEARNING_WH
  AFTER task_upsert_dim_customer
AS
INSERT INTO OLIST_WAREHOSE.PUBLIC.FACT_PAYMENTS (
  payment_sequential, payment_type, payment_installment, customer_id_sk_fk, order_id
)
SELECT
  t.payment_sequential,
  t.payment_type,
  t.payment_installments,
  c.customer_id_sk,
  t.order_id
FROM OLIST.PUBLIC.olist_iceberg_stream t
JOIN OLIST_WAREHOUSE.PUBLIC.DIM_CUSTOMER c
  ON t.customer_unique_id = c.customer_unique_id_bk
WHERE t.payment_sequential IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM OLIST_WAREHOUSE.PUBLIC.FACT_PAYMENTS f
    WHERE f.order_id = t.order_id
      AND f.payment_sequential = t.payment_sequential
  );

-- Task: Load Fact Order Line
CREATE OR REPLACE TASK task_load_fact_order_line
  WAREHOUSE = SNOWFLAKE_LEARNING_WH
  AFTER task_upsert_dim_customer,
        task_upsert_dim_seller,
        task_upsert_dim_product,
        task_upsert_dim_date
AS
INSERT INTO OLIST_WAREHOUSE.PUBLIC.FACT_ORDER_LINE (
  customer_id_sk_fk,
  seller_id_sk_fk,
  product_id_sk_fk,
  order_id,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  orders_delivered_customer_date,
  order_estimated_delivery_date,
  days_passed_for_delivery,
  is_delayed,
  is_success,
  freight_value,
  price,
  payment_values,
  order_status
)
SELECT
  c.customer_id_sk,
  s.seller_id_sk,
  p.product_id_sk,
  t.order_id,
  t.order_purchase_timestamp,
  t.order_approved_at,
  t.order_delivered_carrier_date,
  t.orders_delivered_customer_date,
  t.order_estimated_delivery_date,
  t.days_passed_for_delivery,
  t.is_delayed,
  t.is_success,
  t.freight_value,
  t.price,
  t.payment_values,
  t.order_status
FROM OLIST.PUBLIC.olist_iceberg_stream t
LEFT JOIN OLIST_WAREHOUSE.PUBLIC.DIM_CUSTOMER c 
  ON t.customer_unique_id = c.customer_unique_id_bk
LEFT JOIN OLIST_WAREHOUSE.PUBLIC.DIM_SELLER s
  ON t.seller_id = s.seller_id_bk
LEFT JOIN OLIST_WAREHOUSE.PUBLIC.DIM_PRODUCT p
  ON t.product_id = p.product_id_bk
LEFT JOIN OLIST_WAREHOUSE.PUBLIC.DIM_DATE dd_purchase
  ON DATE_TRUNC('DAY', t.order_purchase_timestamp) = dd_purchase.date_key
LEFT JOIN OLIST_WAREHOUSE.PUBLIC.DIM_DATE dd_carrier
  ON DATE_TRUNC('DAY', t.order_delivered_carrier_date) = dd_carrier.date_key
LEFT JOIN OLIST_WAREHOUSE.PUBLIC.DIM_DATE dd_customer
  ON DATE_TRUNC('DAY', t.orders_delivered_customer_date) = dd_customer.date_key
LEFT JOIN OLIST_WAREHOUSE.PUBLIC.DIM_DATE dd_estimated
  ON DATE_TRUNC('DAY', t.order_estimated_delivery_date) = dd_estimated.date_key
WHERE t.order_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 
    FROM OLIST_WAREHOUSE.PUBLIC.FACT_ORDER_LINE f
    WHERE f.order_id = t.order_id
  );

-- ===============================================================
-- 4️⃣ Enable All Tasks
-- ===============================================================

ALTER TASK task_upsert_dim_customer RESUME;
ALTER TASK task_upsert_dim_seller RESUME;
ALTER TASK task_upsert_dim_product RESUME;
ALTER TASK task_upsert_dim_date RESUME;
ALTER TASK task_load_fact_payments RESUME;
ALTER TASK task_load_fact_order_line RESUME;
