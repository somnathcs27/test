# Databricks notebook source
import dlt

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------
@dlt.table(
    name="hds_transaction", 
    comment="Stores transaction data from the historical data store"
)
@dlt.expect("Check that TRANSACTION_ID is not null", "TRANSACTION_ID IS NOT NULL")
def hds_transaction():

    df_sql = f""" 
    SELECT 
        CAST(TXN.transactionId AS STRING) AS TRANSACTION_ID,
        CAST(TXN.partId AS STRING) AS PART_ID,
        CAST(TXN.sortCode AS STRING) AS SORT_CODE,
        CAST(TXN.accountNumber AS STRING) AS ACCOUNT_NUMBER,
        CAST(TXN.accountSuffix AS STRING) AS ACCOUNT_SUFFIX,
        CAST(TXN.partNumber AS TINYINT) AS PART_NUMBER,
        CAST(TXN.type AS STRING) AS TYPE,
        CAST(TXN.method AS STRING) AS METHOD,
        CAST(TXN.amount AS DECIMAL(38,2)) AS AMOUNT,
        CAST(TXN.bookingDate AS DATE) AS BOOKING_DATE,
        CAST(TXN.valueDate AS DATE) AS VALUE_DATE,
        CAST(TXN.senderSortCode AS STRING) SENDER_SORT_CODE,
        CAST(TXN.senderAccountNumber AS STRING) SENDER_ACCOUNT_NUMBER,
        CAST(TXN.costCentre AS STRING) COST_CENTRE,
        CAST(TXN.directDebitCollectionId AS INT) DIRECT_DEBIT_COLLECTION_ID,
        TO_TIMESTAMP(TXN.updateTime, "yyyy-MM-dd'T'HH:mm:ssXXX") UPDATE_TIME,
        CAST(TXN.updateId AS STRING) AS UPDATE_ID,
        CAST(TXN.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(TXN.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(TXN.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
        
    FROM {env_var}_catalog.bronze_hds_con.transactions AS TXN
    """
    
    df = spark.sql(df_sql)

    return df

# COMMAND ----------
@dlt.table(
    name="hds_balance", 
    comment="Stores balance data from the historical data store"
)
@dlt.expect("Check that BALANCE_ID is not null", "BALANCE_ID IS NOT NULL")
def hds_balance():

    df_sql = f""" 
    SELECT 
        CAST(BAL.id AS STRING) AS BALANCE_ID,
        CAST(BAL.partId AS STRING) AS PART_ID,
        CAST(BAL.sortCode AS STRING) AS SORT_CODE,
        CAST(BAL.accountNumber AS STRING) AS ACCOUNT_NUMBER,
        CAST(BAL.accountSuffix AS STRING) AS ACCOUNT_SUFFIX,
        CAST(BAL.partNumber AS TINYINT) AS PART_NUMBER,
        CAST(BAL.balance AS DECIMAL(38,2)) AS BALANCE,
        CAST(BAL.dateOfBalance AS DATE) AS DATE_OF_BALANCE,
        CAST(BAL.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(BAL.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(BAL.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
 
    FROM {env_var}_catalog.bronze_hds_con.balances AS BAL
    """

    df = spark.sql(df_sql)

    return df
