# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id, md5, xxhash64, lit, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

import mdp_databricks_common.gold_layer_functions as dlf
from mdp_databricks_common.utils.time_utils import get_local_time

#get mdp gold audit log time
RUN_DATE = get_local_time()


# COMMAND ----------


# Returns the SQL cte statement for a supplied custom field value to be used in generation of a final SQL query to load data into the gold layer
def mambu_custom_field_value ( custom_field_name:str ) -> str:

    query = f"""
    cte_{custom_field_name} AS
    (
        SELECT 
            scv_{custom_field_name}.PARENT_KEY AS ACCOUNT_ENCODED_KEY,
            scv_{custom_field_name}.VALUE AS ACCOUNT_{custom_field_name.upper()}
        FROM 
            {env_var}_catalog.silver_con.mambu_custom_field scf_{custom_field_name}
        JOIN 
            {env_var}_catalog.silver_con.mambu_custom_field_value scv_{custom_field_name}
        ON scv_{custom_field_name}.CUSTOM_FIELD_KEY = scf_{custom_field_name}.ENCODED_KEY
        WHERE
            lower(scf_{custom_field_name}.ID) = '{custom_field_name}'
            AND scv_{custom_field_name}.ROW_IS_CURRENT = 1
            AND scf_{custom_field_name}.ROW_IS_CURRENT = 1
    )
    """

    return query


# COMMAND ----------

@dlt.table(
    name="dim_branch",
    comment="Branch dimension for savings account data model"
)
def gold_dim_branch():
    query = f"""
    SELECT
        ENCODED_KEY AS BK_BRANCH,
        ID AS BRANCH_ID,
        COALESCE(NAME,'Unknown') AS BRANCH_NAME,
        COALESCE(STATE,'Unknown') AS BRANCH_STATE,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.mambu_branch
    """

    df = spark.sql(query)
    df = df.withColumn("PK_BRANCH", monotonically_increasing_id() + 1)

    df = df.select(
        "PK_BRANCH",
        "BK_BRANCH",
        "BRANCH_ID",
        "BRANCH_NAME",
        "BRANCH_STATE",
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
    name="dim_client",
    comment="Client dimension for savings account data model"
)
def gold_dim_client():
    query = f"""
    SELECT
        ENCODED_KEY AS BK_CLIENT,
        ID AS CLIENT_ID,
        COALESCE(FIRST_NAME,'Unknown') AS CLIENT_FIRST_NAME,
        COALESCE(MIDDLE_NAME,'Unknown') AS CLIENT_MIDDLE_NAME,
        COALESCE(LAST_NAME,'Unknown') AS CLIENT_LAST_NAME,
        CONCAT(COALESCE(FIRST_NAME,'Unknown'), ' ',  COALESCE(MIDDLE_NAME,'Unknown'), ' ', COALESCE(LAST_NAME,'Unknown')) AS CLIENT_FULL_NAME,
        ACTIVATION_DATE AS CLIENT_ACTIVATION_DATE,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.mambu_client
    """

    df = spark.sql(query)
    df = df.withColumn("PK_CLIENT", monotonically_increasing_id() + 1)

    df = df.select(
        "PK_CLIENT",
        "BK_CLIENT",
        "CLIENT_ID",
        "CLIENT_FIRST_NAME",
        "CLIENT_MIDDLE_NAME",
        "CLIENT_LAST_NAME",
        "CLIENT_FULL_NAME",
        "CLIENT_ACTIVATION_DATE",
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
    name="dim_saving_index_rate_source",
    comment="index rate source dimension for savings account data model"
)
def gold_dim_saving_index_rate_source():
    query = f"""
    SELECT
        ENCODED_KEY AS BK_SAVING_INDEX_RATE_SOURCE,
        ID AS SAVING_INDEX_RATE_SOURCE_ID,
        COALESCE(NAME,'Unknown') AS SAVING_INDEX_RATE_SOURCE_NAME,
        COALESCE(TYPE,'Unknown') AS SAVING_INDEX_RATE_SOURCE_TYPE,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.mambu_index_rate_source
    """

    df = spark.sql(query)
    df = df.withColumn("PK_SAVING_INDEX_RATE_SOURCE", monotonically_increasing_id() + 1)

    df = df.select(
        "PK_SAVING_INDEX_RATE_SOURCE",
        "BK_SAVING_INDEX_RATE_SOURCE",
        "SAVING_INDEX_RATE_SOURCE_ID",
        "SAVING_INDEX_RATE_SOURCE_NAME",
        "SAVING_INDEX_RATE_SOURCE_TYPE",
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
    name="dim_user",
    comment="User dimension for savings account data model"
)
def gold_dim_user():
    query = f"""
    SELECT
        ENCODED_KEY AS BK_USER,
        ID AS USER_ID,
        COALESCE(FIRST_NAME,'Unknown') AS USER_FIRST_NAME,
        COALESCE(LAST_NAME,'Unknown') AS USER_LAST_NAME,
        COALESCE(USER_NAME,'Unknown') AS USER_USERNAME,
        COALESCE(USER_STATE,'Unknown') AS USER_STATE,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.mambu_user
    """

    df = spark.sql(query)
    df = df.withColumn("PK_USER", monotonically_increasing_id() + 1)

    df = df.select(
        "PK_USER",
        "BK_USER",
        "USER_ID",
        "USER_FIRST_NAME",
        "USER_LAST_NAME",
        "USER_USERNAME",
        "USER_STATE",
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
    name="dim_saving_transaction_payment",
    comment="Payment transaction dimension for savings account data model. The actual primary key for this table is encodedkey, however the savingstransactionkey links to the savingstransaction table and for this specific case we are considering savingstransactionkey as Primarykey"
)
def gold_dim_saving_transaction_payment():
    query = f"""
    SELECT
        SAVING_TRANSACTION_KEY AS BK_SAVING_TRANSACTION_PAYMENT,
        COALESCE(CREDITOR_NAME,'Unknown') AS CREDITOR_NAME,
        COALESCE(CREDITOR_ACCOUNT_IBAN,'Unknown') AS CREDITOR_ACCOUNT_IBAN,
        COALESCE(DEBTOR_NAME,'Unknown') AS DEBTOR_NAME,
        COALESCE(DEBTOR_ACCOUNT_IBAN,'Unknown') AS DEBTOR_ACCOUNT_IBAN,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.mambu_payment_detail
    """
    df = spark.sql(query)
    df = df.withColumn("PK_SAVING_TRANSACTION_PAYMENT", monotonically_increasing_id() + 1)

    df = df.select(
        "PK_SAVING_TRANSACTION_PAYMENT",
        "BK_SAVING_TRANSACTION_PAYMENT",
        "CREDITOR_NAME",
        "CREDITOR_ACCOUNT_IBAN",
        "DEBTOR_NAME",
        "DEBTOR_ACCOUNT_IBAN",
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

# MAGIC %md
# MAGIC ## **Generate Mambu Custom fields CTE SQL code block**
# MAGIC

# COMMAND ----------


# create a tuple to store all the explicit custom field values
mambu_custom_fields = ("sortcode", "accountnumber", "accountsuffix")

index = 0
mambu_custom_fields_cte_sql_block = ''

#iterate through building the sql block
for cf in mambu_custom_fields:

  #if this is the first iteration of the loop then we want to add the WITH command
  if index == 0:
    mambu_custom_fields_cte_sql_block = "WITH "

  mambu_custom_fields_cte_sql_block += mambu_custom_field_value(mambu_custom_fields[index])

  # add a trailing comma for all but the last iteration
  if index < len(mambu_custom_fields) - 1:
    mambu_custom_fields_cte_sql_block += ","

  index += 1


# COMMAND ----------

@dlt.table(
    name="dim_saving_account",
    comment="SavingAccount dimension for savings account data model"
)
def gold_dim_saving_account():
    query = f"""
    {mambu_custom_fields_cte_sql_block}
    SELECT
        sc.ENCODED_KEY AS BK_SAVING_ACCOUNT,
        sc.ID AS SAVING_ACCOUNT_ID,
        COALESCE(sc.ACCOUNT_HOLDER_TYPE,'Unknown') AS SAVING_ACCOUNT_HOLDER_TYPE,
        COALESCE(sc.ACCOUNT_STATE,'Unknown') AS SAVING_ACCOUNT_STATE,
        COALESCE(sc.ACCOUNT_TYPE,'Unknown') AS SAVING_ACCOUNT_TYPE,
        COALESCE(sc.NAME,'Unknown') AS SAVING_ACCOUNT_NAME,
        COALESCE(sc.CURRENCY_CODE,'Unknown') AS SAVING_ACCOUNT_CURRENCY_CODE,
        COALESCE(scv_sortcode.ACCOUNT_SORTCODE,'Unknown') AS SAVING_ACCOUNT_SORTCODE,
        COALESCE(scv_accountnumber.ACCOUNT_ACCOUNTNUMBER,'Unknown') AS SAVING_ACCOUNT_NUMBER,
        COALESCE(scv_accountsuffix.ACCOUNT_ACCOUNTSUFFIX,'Unknown') AS SAVING_ACCOUNT_NUMBER_SUFFIX,
        COALESCE(CONCAT(scv_accountnumber.ACCOUNT_ACCOUNTNUMBER,scv_accountsuffix.ACCOUNT_ACCOUNTSUFFIX),'Unknown') AS SAVING_ACCOUNT_NUMBER_AND_SUFFIX,  
        sc.APPROVED_DATE AS SAVING_ACCOUNT_APPROVED_DATE,
        sc.ACTIVATION_DATE AS SAVING_ACCOUNT_ACTIVATION_DATE,
        sc.CLOSED_DATE AS SAVING_ACCOUNT_CLOSE_DATE,
        sc.LAST_INTEREST_CALCULATION_DATE AS SAVING_ACCOUNT_LAST_INTEREST_CALCULATION_DATE,
        sc.LAST_INTEREST_STORED_DATETIME AS SAVING_ACCOUNT_LAST_INTEREST_STORED_DATETIME,
        sc.LAST_ACCOUNT_APPRAISAL_DATE AS SAVING_ACCOUNT_LAST_ACCOUNT_APPRAISAL_DATE,
        sc.LAST_INTEREST_REVIEW_DATE AS SAVING_ACCOUNT_LAST_INTEREST_REVIEW_DATE,
        sc.MDP_LOAD_ID,
        sc.MDP_LOAD_DATETIME,
        sc.ROW_START_DATETIME,
        sc.ROW_END_DATETIME,
        sc.ROW_IS_CURRENT
    FROM 
        {env_var}_catalog.silver_int.mambu_saving_account sc
    LEFT JOIN 
        cte_sortcode scv_sortcode
        ON sc.ENCODED_KEY = scv_sortcode.ACCOUNT_ENCODED_KEY
    LEFT JOIN
        cte_accountnumber scv_accountnumber
        ON sc.ENCODED_KEY = scv_accountnumber.ACCOUNT_ENCODED_KEY  
    LEFT JOIN
        cte_accountsuffix scv_accountsuffix
        ON sc.ENCODED_KEY = scv_accountsuffix.ACCOUNT_ENCODED_KEY
    WHERE sc.ACCOUNT_TYPE <> 'CURRENT_ACCOUNT'
    """

    df = spark.sql(query)
    df = df.withColumn("PK_SAVING_ACCOUNT", monotonically_increasing_id() + 1)

    df = df.select(
        "PK_SAVING_ACCOUNT",
        "BK_SAVING_ACCOUNT",
        "SAVING_ACCOUNT_ID",
        "SAVING_ACCOUNT_HOLDER_TYPE",
        "SAVING_ACCOUNT_STATE",
        "SAVING_ACCOUNT_TYPE",
        "SAVING_ACCOUNT_NAME",
        "SAVING_ACCOUNT_CURRENCY_CODE",
        "SAVING_ACCOUNT_SORTCODE",
        "SAVING_ACCOUNT_NUMBER",
        "SAVING_ACCOUNT_NUMBER_SUFFIX",
        "SAVING_ACCOUNT_NUMBER_AND_SUFFIX",
        "SAVING_ACCOUNT_APPROVED_DATE",
        "SAVING_ACCOUNT_ACTIVATION_DATE",
        "SAVING_ACCOUNT_CLOSE_DATE",
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
    name="dim_gl_account",
    comment="Contains descriptive attribuites for general ledger accounts from mambu"
)
def gold_dim_gl_account():
    query = f"""
    SELECT
        COALESCE(ENCODED_KEY, 'Unknown') AS BK_GL_ACCOUNT
        ,COALESCE(NAME, 'Unknown') AS ACCOUNT_NAME
        ,COALESCE(TYPE, 'Unknown') AS ACCOUNT_TYPE
        ,COALESCE(GL_CODE, 'Unknown') AS GL_CODE
        ,COALESCE(USAGE, 'Unknown') AS USAGE
        ,COALESCE(DESCRIPTION, 'Unknown') AS ACCOUNT_DESCRIPTION
        ,CASE
            WHEN ACTIVATED = 1 THEN 'Yes'
            WHEN ACTIVATED = 0 THEN 'No'
            ELSE 'Unknown'
        END AS IS_ACTIVE
        ,MDP_LOAD_ID
        ,MDP_LOAD_DATETIME
        ,ROW_START_DATETIME
        ,ROW_END_DATETIME
        ,ROW_IS_CURRENT
    FROM  {env_var}_catalog.silver_int.mambu_gl_account mga
    """
    df = spark.sql(query)

    # Add the surrogate key and gold layer audit column
    df = df.withColumns({
        "PK_GL_ACCOUNT": monotonically_increasing_id() + 1,
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(lit(RUN_DATE))
        })
    
    df = df.select(
        "PK_GL_ACCOUNT",
        "BK_GL_ACCOUNT",
        "ACCOUNT_NAME",
        "ACCOUNT_TYPE",
        "GL_CODE",
        "USAGE",
        "ACCOUNT_DESCRIPTION",
        "IS_ACTIVE",
        "MDP_LOAD_ID",
        "MDP_LOAD_DATETIME",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )


    #insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    return df
