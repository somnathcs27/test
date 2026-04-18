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
    name = "dim_general_ledger_user",
    comment = "the user associated to the general ledger"
)
def gold_dim_general_ledger_user():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
	    USER_ID AS BK_GENERAL_LEDGER_USER,
	    COALESCE(USER_NAME,'Unknown') AS USER_NAME,
        COALESCE(DESCRIPTION,'Unknown') AS USER_DESCRIPTION,
	    START_DATE AS USER_START_DATE,
	    END_DATE AS USER_END_DATE,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_user
    """

    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_USER", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_USER",
        "BK_GENERAL_LEDGER_USER",
	    "USER_NAME",
     	"USER_DESCRIPTION",
	    "USER_START_DATE",
	    "USER_END_DATE",
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
    name = "dim_general_ledger_account_type",
    comment = "the account type associated to the general ledger"
)
def gold_dim_general_ledger_account_type():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
	    ACCOUNT_CODE AS BK_GENERAL_LEDGER_ACCOUNT_TYPE,
	    COALESCE(ACCOUNT_CODE,'Unknown') AS ACCOUNT_CODE,
        COALESCE(ACCOUNT_DESCRIPTION,'Unknown') AS ACCOUNT_DESCRIPTION,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_account_type_description
    """

    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_ACCOUNT_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_ACCOUNT_TYPE",
        "BK_GENERAL_LEDGER_ACCOUNT_TYPE",
	    "ACCOUNT_CODE",
        "ACCOUNT_DESCRIPTION",
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
    name = "dim_general_ledger_supplier",
    comment = "the supplier associated with the general ledger"
)
def gold_dim_general_ledger_supplier():
    # Execute SQL query to select and transform data
    query = f"""
       SELECT   
        SUPPLIER_ID AS BK_GENERAL_LEDGER_SUPPLIER,
        COALESCE(SUPPLIER_NAME,'Unknown') AS SUPPLIER_NAME,
        COALESCE(SUPPLIER_NUMBER,0) AS SUPPLIER_NUMBER,
        END_DATE_ACTIVE AS SUPPLIER_END_DATE,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_suppliers
 
    """

    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_SUPPLIER", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_SUPPLIER",
        "BK_GENERAL_LEDGER_SUPPLIER",
        "SUPPLIER_NAME",
        "SUPPLIER_NUMBER",
        "SUPPLIER_END_DATE",
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
    name = "dim_general_ledger_journal_entry_line",
    comment = "the journal entry line associated with the general ledger"
)
def gold_dim_general_ledger_journal_entry_line():
    # Execute SQL query to select and transform data
    query = f"""
   
    SELECT
        JE_HEADER_ID AS BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER,
        JE_LINE_NUM AS BK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE_NUMBER,
        COALESCE(LINE_DESCRIPTION,'Unknown') AS LINE_DESCRIPTION,
        CASE 
            WHEN LENGTH(REPLACE(ATTRIBUTE2,' ','')) = 4 THEN ATTRIBUTE2 
            ELSE COALESCE(ATTRIBUTE2,'Unknown')
        END AS CASHIER,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_journal_entry_lines
    
    """
    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE", monotonically_increasing_id() +1)
  
    # # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE",
        "BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER",
        "BK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE_NUMBER",
        "LINE_DESCRIPTION",
        "CASHIER",
        "MDP_LOAD_ID",
        "MDP_LOAD_DATETIME",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT"
    )

    #insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    return df
