--before doing all that you should note to create an iam role for snowflake
--1 Run the external volume

CREATE OR REPLACE EXTERNAL VOLUME s3_iceberg_volume
  STORAGE_LOCATIONS = (
    (
      NAME                  = 's3_iceberg',
      STORAGE_PROVIDER      = 'S3',
      STORAGE_BASE_URL      = 's3://unique-staging-bucket/brazilianstore/',
      STORAGE_AWS_ROLE_ARN  = 'arn:aws:iam::<your-role>:role/SnowS3'
    )
  );

--1 Run the catalog

CREATE OR REPLACE CATALOG INTEGRATION s3_iceberg_catalog
  CATALOG_SOURCE = 'OBJECT_STORE'
  TABLE_FORMAT = 'ICEBERG'
  ENABLED = TRUE;
/*OBJECT_STORE means catalog of iceberg stored in AWS s3*/

--2 Run the external volume

CREATE OR REPLACE EXTERNAL VOLUME s3_iceberg_volume
  STORAGE_LOCATIONS = (
    (
      NAME                  = 's3_iceberg',
      STORAGE_PROVIDER      = 'S3',
      STORAGE_BASE_URL      = 's3://unique-staging-bucket/brazilianstore/',
      STORAGE_AWS_ROLE_ARN  = 'arn:aws:iam::<your-role>:role/SnowS3'
    )
  );
/*after running external volume you should get the external id using this command and go to the bucket you gave permission 
for in aws and paste this id in the trust relationship policy*/
desc external volume
/* now you have permission to make the iceberg table*/

--3 Run the iceberg table 
CREATE OR REPLACE ICEBERG TABLE olist_table
EXTERNAL_VOLUME = s3_iceberg_volume
CATALOG = s3_iceberg_catalog
METADATA_FILE_PATH = 'default/iceberg_config/metadata/v3.metadata.json';
