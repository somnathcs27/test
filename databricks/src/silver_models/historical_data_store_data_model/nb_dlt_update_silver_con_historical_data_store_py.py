# Databricks notebook source
import dlt

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name="historical_data_store_historical_balances_data", 
    comment="Stores historical balances data for a mortgage from the mortgage historical data store"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def historical_data_store_historical_balances_data():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.id AS STRING) AS ENCODED_KEY,
        CAST(MXT.part_id AS STRING) AS PART_ID,
        CAST(MXT.sort_code AS STRING) AS SORT_CODE,
        CAST(MXT.account_number AS STRING) AS ACCOUNT_NUMBER,
        CAST(MXT.account_suffix AS INTEGER) AS ACCOUNT_SUFFIX,
        CAST(MXT.part_number AS SMALLINT) AS PART_NUMBER,
        CAST(MXT.balance AS DECIMAL(38, 2)) AS BALANCE,
        TO_DATE(MXT.date_of_balance, 'MM/dd/yyyy') AS DATE_OF_BALANCE,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    
    FROM {env_var}_catalog.bronze_historical_data_store_con.historical_balances AS MXT
    
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="historical_data_store_historical_transaction_method_mapping_data", 
    comment="Stores historical transaction method mapping data for a mortgage from the mortgage historical data store"
)
@dlt.expect("Check that CODE is not null", "CODE IS NOT NULL")
def historical_data_store_historical_transaction_method_mapping_data():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.code AS STRING) AS CODE,
        CAST(MXT.libra_description AS STRING) AS LIBRA_DESCRIPTION,
        CAST(MXT.mambu_channel_id AS INTEGER) AS MAMBU_CHANNEL_ID,   
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    
    FROM {env_var}_catalog.bronze_historical_data_store_con.historical_transaction_method_mapping AS MXT
    
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="historical_data_store_historical_transaction_type_mapping_data", 
    comment="Stores historical transaction type mapping data for a mortgage from the mortgage historical data store"
)
@dlt.expect("Check that CODE is not null", "CODE IS NOT NULL")
def historical_data_store_historical_transaction_type_mapping_data():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.code AS STRING) AS CODE,
        CAST(MXT.description AS STRING) AS DESCRIPTION,
        CAST(MXT.mambu_type AS STRING) AS MAMBU_TYPE, 
        CAST(MXT.display_name AS STRING) AS DISPLAY_NAME,     
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    
    FROM {env_var}_catalog.bronze_historical_data_store_con.historical_transaction_type_mapping AS MXT
    
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="historical_data_store_historical_transactions_data", 
    comment="Stores historical transactions data for a mortgage from the mortgage historical data store"
)
@dlt.expect("Check that transaction_id is not null", "transaction_id IS NOT NULL")
def historical_data_store_historical_transactions_data():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.transaction_id AS STRING) AS TRANSACTION_ID,
        CAST(MXT.part_id AS STRING) AS PART_ID,
        CAST(MXT.sort_code AS STRING) AS SORT_CODE, 
        CAST(MXT.account_number AS STRING) AS ACCOUNT_NUMBER, 
        CAST(MXT.account_suffix AS INTEGER) AS ACCOUNT_SUFFIX,
        CAST(MXT.part_number AS INTEGER) AS PART_NUMBER,   
        CAST(MXT.type_code AS STRING) AS TYPE_CODE,      
        CAST(MXT.method_code AS STRING) AS METHOD_CODE,   
        CAST(MXT.type AS STRING) AS TYPE,      
        CAST(MXT.method AS STRING) AS METHOD,
        CAST(MXT.amount AS DECIMAL(38, 2)) AS AMOUNT, 
        TO_DATE(MXT.booking_date, 'MM/dd/yyyy') AS BOOKING_DATE,
        TO_DATE(MXT.value_date, 'MM/dd/yyyy') AS VALUE_DATE,    
        CAST(MXT.sender_sort_code AS STRING) AS SENDER_SORT_DATE,      
        CAST(MXT.sender_account_number AS STRING) AS SENDER_ACCOUNT_NUMBER,
        CAST(MXT.cost_centre AS STRING) AS COST_CENTRE,
        CAST(MXT.direct_debit_collection_id AS STRING) AS DIRECT_DEBIT_COLLECTION_ID,
        TO_TIMESTAMP(MXT.update_time, 'dd-MMM-yy HH:mm:ss') AS UPDATE_TIME,
        CAST(MXT.update_id AS STRING) AS UPDATE_ID,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        CAST(MXT.notes AS STRING) AS NOTES,
        CAST(MXT.contra_id AS STRING) AS CONTRA_ID,
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    
    FROM {env_var}_catalog.bronze_historical_data_store_con.historical_transactions AS MXT
    
    """
    )

    return df