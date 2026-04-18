# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------


sql_query = f"""
CREATE OR REPLACE TEMPORARY VIEW temp_dim_customer AS
SELECT 
    CAST(1 AS long) AS PK_CUSTOMER,
    '020df259-6c91-ef11-8a69-7c1e5279f191' AS BK_CUSTOMER,
    12345 AS CUSTOMER_NUMBER,
    'Mr' AS TITLE,
    'John' AS FORENAMES,
    'J' AS INITIALS,
    'Do' AS SURNAME,
    'Male' AS GENDER,
    CAST('1980-01-01' AS DATE) AS DATE_OF_BIRTH,
    44 AS AGE,
    'AB123456C' AS NI_NUMBER,
    'Building Name' AS BUILDING_NAME,
    '123' AS BUILDING_NUMBER,
    'Barton way' AS STREET_NAME,
    'Leeds' AS CITY,
    'West Yorkshire' AS COUNTY,
    'UK' AS COUNTRY,
    'Region' AS REGION,
    'LS27 0GL' AS POSTCODE,
    'john.doe@example.com' AS EMAIL_ADDRESS,
    'Email' AS PREFERRED_CONTACT_METHOD,
    '1234567890' AS TELEPHONE_HOME,
    '0987654321' AS TELEPHONE_MOBILE,
    '1122334455' AS TELEPHONE_WORK,
    'TypeCode' AS CUSTOMER_TYPE_CODE,
    '2024-12-04 00:00:00' AS CUSTOMER_START_DATE,
    CAST(NULL AS TIMESTAMP) AS CUSTOMER_END_DATE,
    1 AS LOAD_ID,
    CAST('2024-12-04 00:00:00' AS TIMESTAMP) AS LOAD_DATETIME,
    CAST('2024-12-04 00:00:00' AS TIMESTAMP) AS ROW_START_DATETIME,
    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT"""


# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------



# Load the derived data
derived_data = spark.table(f"{env_var}_catalog.data_quality.dim_customer_test_data")

# Perform assertions
expected_data = spark.table("temp_dim_customer")

# Compare derived_data and expected_data
assert derived_data.exceptAll(expected_data).count() == 0 and expected_data.exceptAll(derived_data).count() == 0

print("Joins test case passed")

# COMMAND ----------


