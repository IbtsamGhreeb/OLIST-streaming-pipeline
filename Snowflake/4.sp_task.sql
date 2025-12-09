CREATE OR REPLACE TASK REFRESH_OLIST_METADATA_TASK
  WAREHOUSE = snowflake_learning_wh
  SCHEDULE = '15 MINUTE' 
AS
  CALL REFRESH_ICEBERG_METADATA_SP(
    'olist.public.OLIST_TABLE', 
    's3://unique-staging-bucket/BrazilianProject/'
  );


--turn the task on
ALTER TASK REFRESH_OLIST_METADATA_TASK RESUME;
