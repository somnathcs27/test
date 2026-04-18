# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id, md5, xxhash64
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

import mdp_databricks_common.gold_layer_functions as dlf

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')


# COMMAND ----------

@dlt.table(
    name="dim_saving_product",
    comment="Product dimension for savings account data model"
)
def gold_dim_saving_product():
    query = f"""
    SELECT
        ENCODED_KEY AS BK_SAVING_PRODUCT,
        ID AS SAVING_PRODUCT_ID,
        COALESCE(NAME,'Unknown') AS SAVING_PRODUCT_NAME,
        COALESCE(ACTIVATED,'Unknown') AS IS_SAVING_PRODUCT_ACTIVATED,
        COALESCE(PRODUCT_TYPE,'Unknown') AS SAVING_PRODUCT_TYPE,
        COALESCE(CATEGORY,'Unknown') AS SAVING_PRODUCT_CATEGORY,
        COALESCE(DESCRIPTION,'Unknown') AS SAVING_PRODUCT_DESCRIPTION,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.mambu_saving_product
    """

    df = spark.sql(query)
    df = df.withColumn("PK_SAVING_PRODUCT", monotonically_increasing_id() + 1)

    df = df.select(
        "PK_SAVING_PRODUCT",
        "BK_SAVING_PRODUCT",
        "SAVING_PRODUCT_ID",
        "SAVING_PRODUCT_NAME",
        "IS_SAVING_PRODUCT_ACTIVATED",
        "SAVING_PRODUCT_TYPE",
        "SAVING_PRODUCT_CATEGORY",
        "SAVING_PRODUCT_DESCRIPTION",
        "MDP_LOAD_ID",
        "MDP_LOAD_DATETIME",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT"
    )
    
    #insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    return df

# COMMAND ----------

@dlt.table(
    name="dim_saving_transaction_type",
    comment="transaction dimension for savings account data model"
)
def gold_dim_saving_transaction_type():
    query = f"""
    WITH LATEST_TRANSACTIONS AS (
        SELECT 
        TYPE AS BK_SAVING_TRANSACTION_TYPE,
        COALESCE(TYPE,'Unknown') AS SAVING_TRANSACTION_TYPE_DESCRIPTION,
       (CASE WHEN COALESCE(TYPE, 'Unknown') IN ('INTEREST_APPLIED_ADJUSTMENT', 'ADJUSTMENT') THEN 'RECEIPT_REVERSAL'
             WHEN COALESCE(TYPE, 'Unknown') IN ('WITHDRAWAL_ADJUSTMENT')                     THEN 'WITHDRAWAL_REVERSAL'
             WHEN TYPE IS NULL                                                               THEN 'Unknown'
             ELSE 'ORGINAL_TRANSACTION' END) AS SAVING_REVERSAL_TRANSACTION_TYPE_DESCRIPTION,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT,
        ROW_NUMBER() OVER (PARTITION BY TYPE ORDER BY ROW_START_DATETIME DESC) AS ROW_NUM
        FROM {env_var}_catalog.silver_int.mambu_saving_transaction
)
    SELECT 
        BK_SAVING_TRANSACTION_TYPE,
        SAVING_TRANSACTION_TYPE_DESCRIPTION,
        SAVING_REVERSAL_TRANSACTION_TYPE_DESCRIPTION,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM LATEST_TRANSACTIONS
    WHERE ROW_NUM = 1
    """

    df = spark.sql(query)
    df = df.withColumn("PK_SAVING_TRANSACTION_TYPE", monotonically_increasing_id() + 1)

    df = df.select(
        "PK_SAVING_TRANSACTION_TYPE",
        "BK_SAVING_TRANSACTION_TYPE",
        "SAVING_TRANSACTION_TYPE_DESCRIPTION",
        "SAVING_REVERSAL_TRANSACTION_TYPE_DESCRIPTION",
        "MDP_LOAD_ID",
        "MDP_LOAD_DATETIME",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT"
    )

    #insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    return df
