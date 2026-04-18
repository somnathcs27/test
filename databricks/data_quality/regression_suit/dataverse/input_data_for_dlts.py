# Databricks notebook source
# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

# DBTITLE 1,dataverse_contact
sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.dataverse_contact_test_data AS
SELECT 
    '020df259-6c91-ef11-8a69-7c1e5279f191' AS CUSTOMER_ID,
    12345 AS CUSTOMER_NUMBER, 
    1 AS GENDER, 
    1 AS TITLE, 
    'John' AS FORENAMES,
    'J' AS INITIALS , 
    'Do' AS SURNAME, 
    CAST('1980-01-01 00:00:00' AS TIMESTAMP) AS DATE_OF_BIRTH,
    44 AS AGE ,
    'AB123456C' AS NI_NUMBER, 
    'Building Name' AS BUILDING_NAME, 
    '123' AS BUILDING_NUMBER,
    'Barton way' AS STREET_NAME,
    'Leeds' AS CITY, 
    'West Yorkshire' AS COUNTY , 
    1 AS COUNTRY, 
    'Region' AS REGION,
    'LS27 0GL' AS POSTCODE, 
    'john.doe@example.com' AS EMAIL_ADDRESS, 
    2 AS PREFERRED_CONTACT_METHOD, 
    '1234567890' AS TELEPHONE_HOME, 
    '0987654321' AS TELEPHONE_MOBILE, 
    '1122334455' AS TELEPHONE_WORK,
    'TypeCode' AS CUSTOMER_TYPE_CODE,
    '2024-12-04 00:00:00' AS CUSTOMER_START_DATE, 
    CAST(NULL AS TIMESTAMP) AS CUSTOMER_END_DATE, 
    1 AS LOAD_ID , 
    CAST('2024-12-04 00:00:00' AS TIMESTAMP) AS LOAD_DATETIME, 
    CAST('2024-12-04 00:00:00' AS TIMESTAMP) AS ROW_START_DATETIME, 
    CAST(NULL AS TIMESTAMP)  AS ROW_END_DATETIME, 
    1 AS ROW_IS_CURRENT"""

# Execute the SQL query
spark.sql(sql_query)



# COMMAND ----------

# DBTITLE 1,dataverse_country
sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.dataverse_country_test_data AS
SELECT 
    1 AS COUNTRY_ID,
    'UK' AS COUNTRY_NAME,
    1 AS LOAD_ID,
    current_timestamp() AS LOAD_DATETIME,
    current_timestamp() AS ROW_START_DATETIME,
    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT"""

# Execute the SQL query
spark.sql(sql_query)



# COMMAND ----------

# DBTITLE 1,dataverse_global_option_set_metadata
sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.dataverse_global_option_set_metadata_test_data AS
SELECT
    'lbs_title' AS OPTION_SET_NAME,
    1 AS OPTION, 
    true AS IS_USER_LOCALIZED_LABEL, 
    1033 AS LOCALIZED_LABEL_LANGUAGE_CODE,
    'Mr' AS LOCALIZED_LABEL,
    'lbs_title' AS GLOBAL_OPTION_SET_NAME,
    'contact' AS ENTITY_NAME,
    12345 AS LOAD_ID,
    current_timestamp() AS LOAD_DATETIME, 
    current_timestamp() AS ROW_START_DATETIME, 
    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME, 
    1 AS ROW_IS_CURRENT"""

# Execute the SQL query
spark.sql(sql_query)


# COMMAND ----------

# DBTITLE 1,dataverse_option_set_metadata
sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.dataverse_option_set_metadata_test_data AS
SELECT
    'contact' AS ENTITY_NAME, 
    'preferredcontactmethodcode' AS OPTION_SET_NAME, 
    2 AS OPTION, 
    false AS IS_USER_LOCALIZED_LABEL, 
    1033 AS LOCALIZED_LABEL_LANGUAGE_CODE, 
    'Email' AS LOCALIZED_LABEL, 
    1 AS LOAD_ID, 
    current_timestamp() AS LOAD_DATETIME, 
    current_timestamp() AS ROW_START_DATETIME,
    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME, 
    1 AS ROW_IS_CURRENT

UNION ALL 

SELECT
    'contact' AS ENTITY_NAME, 
    'gendercode' AS OPTION_SET_NAME, 
    1 AS OPTION, 
    false AS IS_USER_LOCALIZED_LABEL, 
    1033 AS LOCALIZED_LABEL_LANGUAGE_CODE, 
    'Male' AS LOCALIZED_LABEL, 
    1 AS LOAD_ID, 
    current_timestamp() AS LOAD_DATETIME, 
    current_timestamp() AS ROW_START_DATETIME,
    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME, 
    1 AS ROW_IS_CURRENT"""

# Execute the SQL query
spark.sql(sql_query)
