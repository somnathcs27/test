# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id, md5, xxhash64, lit, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from mdp_databricks_common.utils.time_utils import get_local_time

#get mdp gold audit log time
LOCAL_TIME = get_local_time()

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

# MAGIC %md
# MAGIC To aid readability the pooling of silver fact data for the Mambu fact tables and the comparison of that pooled data against the gold model has been seperated into different cells per fact entity

# COMMAND ----------

#pool together the incoming silver saving account fact data...
cte_silver_saving_account_fact = f"""
    WITH cte_stage_saving_account AS (
        SELECT
            SA.ID AS  PK_FACT_SAVING_ACCOUNT,
            SA.ENCODED_KEY AS SAVING_ACCOUNT_ENCODED_KEY, 
           (CASE WHEN SA.ACCOUNT_HOLDER_TYPE = 'CLIENT' THEN SA.ACCOUNT_HOLDER_KEY
                 ELSE NULL END) AS SAVING_ACCOUNT_HOLDER_KEY_CLIENT, 
            SA.PRODUCT_TYPE_KEY AS SAVING_ACCOUNT_PRODUCT_TYPE_KEY,
            SA.ASSIGNED_BRANCH_KEY AS SAVING_ACCOUNT_ASSIGNED_BRANCH_KEY,
            SA.ASSIGNED_USER_KEY AS SAVING_ACCOUNT_ASSIGNED_USER_KEY,
            SA.WITHHOLDING_TAX_SOURCE_KEY AS SAVING_ACCOUNT_WITHHOLDING_TAX_SOURCE_KEY,
            SA.ID AS SAVING_ACCOUNT_ID,
            SA.APPROVED_DATE AS SAVING_ACCOUNT_APPROVED_DATE,
            SA.ACTIVATION_DATE AS SAVING_ACCOUNT_ACTIVATION_DATE,
            SA.CLOSED_DATE AS SAVING_ACCOUNT_CLOSE_DATE,
            COALESCE(SA.BALANCE,0) AS CAPITAL_LEDGER_BALANCE,
            COALESCE(SA.BALANCE,0) - (COALESCE(SA.BLOCKED_BALANCE,0) + COALESCE(SA.LOCKED_BALANCE,0)) AS CAPITAL_AVAILABLE_BALANCE,
            COALESCE(SA.BALANCE,0) - COALESCE(ENTRIES.FUTURE_AMOUNT, 0) AS REP_CAPITAL_LEDGER_BALANCE,
            SA.ACCRUED_INTEREST AS ACCRUED_INTEREST_GROSS,
            COALESCE(TAX_INFO.TAX_RATE, 0) AS TAX_RATE,
            COALESCE(((SA.ACCRUED_INTEREST) * TAX_INFO.TAX_RATE / 100), 0) AS ACCRUED_INTEREST_TAX,
            COALESCE(((SA.ACCRUED_INTEREST) - COALESCE((SA.ACCRUED_INTEREST),0) * TAX_INFO.TAX_RATE / 100), 0) AS ACCRUED_INTEREST_NET,
            COALESCE(SA.BLOCKED_BALANCE,0) AS SAVING_ACCOUNT_BLOCKED_BALANCE,
            COALESCE(SA.LOCKED_BALANCE,0) AS SAVING_ACCOUNT_LOCKED_BALANCE,
            1 AS SAVING_ACCOUNT_COUNT

        FROM {env_var}_catalog.silver_int.mambu_saving_account AS SA

        LEFT JOIN (
            SELECT
                ST.SAVING_PARENT_ACCOUNT_KEY AS ACCOUNT_KEY,
                COALESCE(IR.VALUE,0) AS TAX_RATE
             FROM {env_var}_catalog.silver_int.mambu_saving_transaction AS ST
             JOIN {env_var}_catalog.silver_con.mambu_index_rate AS IR
               ON ST.TAX_RATE_ENCODEDKEY_OID = IR.ENCODED_KEY 
              AND IR.ROW_IS_CURRENT = 1
             JOIN {env_var}_catalog.silver_con.mambu_index_rate_source AS IRS
               ON IR.SOURCE_ENCODED_KEY_OID = IRS.ENCODED_KEY 
              AND IRS.ROW_IS_CURRENT = 1
            WHERE IRS.TYPE IN ('TAX_RATE', 'WITHHOLDING_TAX_RATE')
                 ) AS TAX_INFO
          ON SA.ENCODED_KEY = TAX_INFO.ACCOUNT_KEY

        LEFT JOIN ( 
            SELECT 
                SAVING_PARENT_ACCOUNT_KEY AS ACCOUNT_KEY, 
                SUM(CASE WHEN CREATION_DATE > CURRENT_DATE THEN AMOUNT ELSE 0 END) AS FUTURE_AMOUNT 
              FROM {env_var}_catalog.silver_int.mambu_saving_transaction SST
             WHERE TYPE IN ('DEPOSIT', 'WITHDRAWAL') 
               AND SST.ROW_IS_CURRENT = 1
             GROUP BY SAVING_PARENT_ACCOUNT_KEY 
                  ) AS ENTRIES 
          ON SA.ENCODED_KEY = ENTRIES.ACCOUNT_KEY

    WHERE SA.ROW_IS_CURRENT = 1
    AND SA.ACCOUNT_TYPE <> 'CURRENT_ACCOUNT'
    )"""


# COMMAND ----------

@dlt.table(
    name = "fact_saving_account",
    comment = "fact saving accout with the necessary transformations" 
)
def gold_fact_saving_account():
    # Execute SQL query to select and transform data
    query = f"""

    {cte_silver_saving_account_fact}

    SELECT
      CSA.PK_FACT_SAVING_ACCOUNT,
     (CASE
        WHEN DSA.PK_SAVING_ACCOUNT IS NOT NULL THEN DSA.PK_SAVING_ACCOUNT
        WHEN DSA.PK_SAVING_ACCOUNT IS NULL AND CSA.SAVING_ACCOUNT_ENCODED_KEY IS NOT NULL THEN -1
        ELSE -2 
      END) AS FK_SAVING_ACCOUNT,
     (CASE
        WHEN DSP.PK_SAVING_PRODUCT IS NOT NULL THEN DSP.PK_SAVING_PRODUCT
        WHEN DSP.PK_SAVING_PRODUCT IS NULL AND CSA.SAVING_ACCOUNT_PRODUCT_TYPE_KEY IS NOT NULL THEN -1 
        ELSE -2
      END) AS FK_SAVING_PRODUCT,
     (CASE
        WHEN DSB.PK_BRANCH IS NOT NULL THEN DSB.PK_BRANCH
        WHEN DSB.PK_BRANCH IS NULL AND CSA.SAVING_ACCOUNT_ASSIGNED_BRANCH_KEY IS NOT NULL THEN -1 
        ELSE -2
      END) AS FK_BRANCH,
     (CASE
        WHEN DSC.PK_CLIENT IS NOT NULL THEN DSC.PK_CLIENT
        WHEN DSC.PK_CLIENT IS NULL AND CSA.SAVING_ACCOUNT_HOLDER_KEY_CLIENT IS NOT NULL THEN -1 
        ELSE -2
      END) AS FK_CLIENT,
     (CASE
        WHEN DSU.PK_USER IS NOT NULL THEN DSU.PK_USER
        WHEN DSU.PK_USER IS NULL AND CSA.SAVING_ACCOUNT_ASSIGNED_USER_KEY IS NOT NULL THEN -1
        ELSE -2
      END) AS FK_USER,
     (CASE
        WHEN DSIRS.PK_SAVING_INDEX_RATE_SOURCE IS NOT NULL THEN DSIRS.PK_SAVING_INDEX_RATE_SOURCE
        WHEN DSIRS.PK_SAVING_INDEX_RATE_SOURCE IS NULL AND CSA.SAVING_ACCOUNT_WITHHOLDING_TAX_SOURCE_KEY IS NOT NULL THEN -1 
        ELSE -2
      END) AS FK_SAVING_INDEX_RATE_SOURCE,
      CSA.SAVING_ACCOUNT_APPROVED_DATE AS FK_APPROVED_DATE,
      CSA.SAVING_ACCOUNT_ACTIVATION_DATE AS FK_ACTIVATION_DATE,
      CSA.SAVING_ACCOUNT_CLOSE_DATE AS FK_CLOSED_DATE,
      CSA.SAVING_ACCOUNT_ID,
      CSA.CAPITAL_LEDGER_BALANCE,
      CSA.CAPITAL_AVAILABLE_BALANCE,
      CSA.REP_CAPITAL_LEDGER_BALANCE,
      CSA.ACCRUED_INTEREST_GROSS,
      CSA.TAX_RATE,
      CSA.ACCRUED_INTEREST_TAX,
      CSA.ACCRUED_INTEREST_NET,
      CSA.SAVING_ACCOUNT_BLOCKED_BALANCE,
      CSA.SAVING_ACCOUNT_LOCKED_BALANCE,
      CSA.SAVING_ACCOUNT_COUNT
    FROM cte_stage_saving_account CSA

    LEFT JOIN (SELECT PK_SAVING_ACCOUNT, BK_SAVING_ACCOUNT FROM {env_var}_catalog.gold_con.dim_saving_account WHERE ROW_IS_CURRENT = 1) DSA
      ON CSA.SAVING_ACCOUNT_ENCODED_KEY = DSA.BK_SAVING_ACCOUNT

    LEFT JOIN (SELECT PK_SAVING_PRODUCT, BK_SAVING_PRODUCT FROM {env_var}_catalog.gold_int.dim_saving_product WHERE ROW_IS_CURRENT = 1) DSP
      ON CSA.SAVING_ACCOUNT_PRODUCT_TYPE_KEY = DSP.BK_SAVING_PRODUCT

    LEFT JOIN (SELECT PK_BRANCH, BK_BRANCH FROM {env_var}_catalog.gold_con.dim_branch WHERE ROW_IS_CURRENT = 1) DSB
      ON CSA.SAVING_ACCOUNT_ASSIGNED_BRANCH_KEY = DSB.BK_BRANCH

    LEFT JOIN (SELECT PK_CLIENT, BK_CLIENT FROM {env_var}_catalog.gold_con.dim_client WHERE ROW_IS_CURRENT = 1) DSC
      ON CSA.SAVING_ACCOUNT_HOLDER_KEY_CLIENT = DSC.BK_CLIENT

    LEFT JOIN (SELECT PK_SAVING_INDEX_RATE_SOURCE, BK_SAVING_INDEX_RATE_SOURCE FROM {env_var}_catalog.gold_con.dim_saving_index_rate_source WHERE ROW_IS_CURRENT = 1) DSIRS
      ON CSA.SAVING_ACCOUNT_WITHHOLDING_TAX_SOURCE_KEY = DSIRS.BK_SAVING_INDEX_RATE_SOURCE

    LEFT JOIN (SELECT PK_USER, BK_USER FROM {env_var}_catalog.gold_con.dim_user WHERE ROW_IS_CURRENT = 1) DSU
      ON CSA.SAVING_ACCOUNT_ASSIGNED_USER_KEY = DSU.BK_USER
    """

    df = spark.sql(query)

    return df


# COMMAND ----------

#pool together the incoming silver saving transaction fact data...
cte_silver_saving_transaction_fact = f"""
    WITH cte_stage_saving_transaction AS (
    SELECT 
          ST.TRANSACTION_ID AS PK_FACT_SAVING_TRANSACTION,
          ST.ENCODED_KEY AS SAVING_TRANSACTION_ENCODED_KEY,
          ST.PRODUCT_TYPE_KEY,
          ST.BRANCH_KEY,
          ST.SAVING_PARENT_ACCOUNT_KEY,
          ST.USER_KEY,
          ST.REVERSAL_TRANSACTION_KEY AS REVERSAL_SAVING_TRANSACTION_KEY,
          ST.LINKED_SAVING_TRANSACTION_KEY,
          ST.TRANSACTION_ID,
          ST.TYPE AS SAVING_TRANSACTION_TYPE,
          ST.CREATION_DATE,
          COALESCE(ST.BALANCE,0) AS SAVING_TRANSACTION_BALANCE,
          COALESCE(ST.AMOUNT,0) AS SAVING_TRANSACTION_AMOUNT,
         (CASE WHEN COALESCE(ST.AMOUNT,0) > 0.0 
                AND ST.TYPE IN ('DEPOSIT','INTEREST_APPLIED') THEN COALESCE(ST.AMOUNT,0) 
               WHEN REV.IS_REVERSAL_TRANSACTION = 1 THEN -1 * ABS(COALESCE(ST.AMOUNT, 0)) ELSE 0.0 END) AS SAVING_TRANSACTION_RECEIPT_AMOUNT,
         (CASE WHEN COALESCE(ST.AMOUNT, 0) < 0.0 
                AND ST.TYPE = 'WITHDRAWAL' THEN ABS(COALESCE(ST.AMOUNT, 0)) ELSE 0.0 END) AS SAVING_TRANSACTION_WITHDRAWAL_AMOUNT,
          COALESCE(ST.INTEREST_AMOUNT,0) AS SAVING_TRANSACTION_INTEREST_AMOUNT,
          COALESCE(ST.INTEREST_RATE,0) AS SAVING_TRANSACTION_INTEREST_RATE,
          COALESCE(ST.FUNDS_AMOUNT,0) AS SAVING_TRANSACTION_FUNDS_AMOUNT,
          COALESCE(ST.FEES_AMOUNT,0) AS SAVING_TRANSACTION_FEES_AMOUNT,
         (CASE WHEN REV.IS_REVERSAL_TRANSACTION = 1 THEN -1 ELSE 1 END) AS SAVING_TRANSACTION_COUNT

    FROM {env_var}_catalog.silver_int.mambu_saving_transaction ST

    LEFT JOIN (
      SELECT 
          OT.SAVING_PARENT_ACCOUNT_KEY,
          OT.ENCODED_KEY AS SAVING_TRANSACTION_ENCODED_KEY,
          RT.ENCODED_KEY AS REVERSAL_SAVING_TRANSACTION_KEY,
          1 AS IS_REVERSAL_TRANSACTION
      FROM 
          {env_var}_catalog.silver_int.mambu_saving_transaction OT 
      JOIN 
        {env_var}_catalog.silver_int.mambu_saving_transaction RT 
        ON OT.ENCODED_KEY = RT.REVERSAL_TRANSACTION_KEY

      WHERE OT.ENCODED_KEY = RT.REVERSAL_TRANSACTION_KEY 
        AND OT.ROW_IS_CURRENT = 1 
        AND RT.ROW_IS_CURRENT = 1
                ) AS REV

       ON ST.ENCODED_KEY = REV.SAVING_TRANSACTION_ENCODED_KEY 
      AND ST.SAVING_PARENT_ACCOUNT_KEY = REV.SAVING_PARENT_ACCOUNT_KEY 

    WHERE ST.ROW_IS_CURRENT = 1
    )"""

# COMMAND ----------

@dlt.table(
    name="fact_saving_transaction",
    comment="fact saving transaction with the necessary transformations"
)
def gold_fact_saving_transaction():
    # Execute SQL query to select and transform data
    query = f"""

    {cte_silver_saving_transaction_fact}

SELECT 
      GTA.PK_FACT_SAVING_TRANSACTION,
     (CASE
        WHEN GSA.PK_SAVING_ACCOUNT IS NOT NULL THEN GSA.PK_SAVING_ACCOUNT
        WHEN GSA.PK_SAVING_ACCOUNT IS NULL AND GTA.SAVING_PARENT_ACCOUNT_KEY IS NOT NULL THEN -1
        ELSE -2
      END) AS FK_SAVING_ACCOUNT,
     (CASE
        WHEN GSP.PK_SAVING_PRODUCT IS NOT NULL THEN GSP.PK_SAVING_PRODUCT
        WHEN GSP.PK_SAVING_PRODUCT IS NULL AND GTA.PRODUCT_TYPE_KEY IS NOT NULL THEN -1 
        ELSE -2
      END) AS FK_SAVING_PRODUCT,
     (CASE
        WHEN GB.PK_BRANCH IS NOT NULL THEN GB.PK_BRANCH
        WHEN GB.PK_BRANCH IS NULL AND GTA.BRANCH_KEY IS NOT NULL THEN -1
        ELSE -2
      END) AS FK_BRANCH,
     (CASE
        WHEN GSU.PK_USER IS NOT NULL THEN GSU.PK_USER
        WHEN GSU.PK_USER IS NULL AND GTA.USER_KEY IS NOT NULL THEN -1 
        ELSE -2
      END) AS FK_USER,
    (CASE
        WHEN GST.PK_SAVING_TRANSACTION_TYPE IS NOT NULL THEN GST.PK_SAVING_TRANSACTION_TYPE
        WHEN GST.PK_SAVING_TRANSACTION_TYPE IS NULL AND GTA.SAVING_TRANSACTION_TYPE IS NOT NULL THEN -1 
        ELSE -2
      END) AS FK_SAVING_TRANSACTION_TYPE,
    (CASE
        WHEN GPT.PK_SAVING_TRANSACTION_PAYMENT IS NOT NULL THEN GPT.PK_SAVING_TRANSACTION_PAYMENT
        -- We are treating saving transaction payment differently as it may be legitimate for a transaction record not to have a  
        -- matching payment detail record, if there's no match we simply want to set the value as a null unknown member I.E. -2
        ELSE -2
      END) AS FK_SAVING_TRANSACTION_PAYMENT,      
      GTA.CREATION_DATE AS FK_CREATION_DATE,
      GTA.TRANSACTION_ID,
      GTA.SAVING_TRANSACTION_AMOUNT,
      GTA.SAVING_TRANSACTION_BALANCE,
      GTA.SAVING_TRANSACTION_RECEIPT_AMOUNT,
      GTA.SAVING_TRANSACTION_WITHDRAWAL_AMOUNT,
      GTA.SAVING_TRANSACTION_INTEREST_AMOUNT,
      GTA.SAVING_TRANSACTION_INTEREST_RATE,
      GTA.SAVING_TRANSACTION_FUNDS_AMOUNT,
      GTA.SAVING_TRANSACTION_FEES_AMOUNT,
      GTA.SAVING_TRANSACTION_COUNT

FROM cte_stage_saving_transaction GTA
LEFT JOIN (SELECT PK_SAVING_ACCOUNT, BK_SAVING_ACCOUNT FROM {env_var}_catalog.gold_con.dim_saving_account WHERE ROW_IS_CURRENT = 1) GSA 
  ON GTA.SAVING_PARENT_ACCOUNT_KEY = GSA.BK_SAVING_ACCOUNT

LEFT JOIN (SELECT PK_SAVING_PRODUCT, BK_SAVING_PRODUCT FROM {env_var}_catalog.gold_int.dim_saving_product WHERE ROW_IS_CURRENT = 1) GSP 
  ON GTA.PRODUCT_TYPE_KEY = GSP.BK_SAVING_PRODUCT

  LEFT JOIN (SELECT PK_BRANCH, BK_BRANCH FROM {env_var}_catalog.gold_con.dim_branch WHERE ROW_IS_CURRENT = 1) GB 
    ON GTA.BRANCH_KEY = GB.BK_BRANCH

  LEFT JOIN (SELECT PK_USER, BK_USER FROM {env_var}_catalog.gold_con.dim_user WHERE ROW_IS_CURRENT = 1) GSU 
    ON GTA.USER_KEY = GSU.BK_USER

  LEFT JOIN (SELECT PK_SAVING_TRANSACTION_TYPE, BK_SAVING_TRANSACTION_TYPE FROM {env_var}_catalog.gold_int.dim_saving_transaction_type WHERE ROW_IS_CURRENT = 1) GST 
    ON GTA.SAVING_TRANSACTION_TYPE = GST.BK_SAVING_TRANSACTION_TYPE

  LEFT JOIN (SELECT PK_SAVING_TRANSACTION_PAYMENT, BK_SAVING_TRANSACTION_PAYMENT FROM {env_var}_catalog.gold_con.dim_saving_transaction_payment WHERE ROW_IS_CURRENT = 1) GPT 
    ON GTA.SAVING_TRANSACTION_ENCODED_KEY = GPT.BK_SAVING_TRANSACTION_PAYMENT 
  """

    df = spark.sql(query)

    return df

# COMMAND ----------

@dlt.table(
    name="fact_gl_journal_entry",
    comment=""
)
def gold_fact_gl_journal_entry():
    # Execute SQL query to select and transform data
    query = f"""
        SELECT
            glje.ENTRY_ID AS PK_FACT_GL_JOURNAL_ENTRY
            ,CASE
                WHEN dglam.PK_GL_ACCOUNT IS NOT NULL THEN dglam.PK_GL_ACCOUNT
                WHEN dglam.PK_GL_ACCOUNT IS NULL AND glje.GL_ACCOUNT_ENCODED_KEY_OID IS NOT NULL THEN -1
                ELSE -2
            END AS FK_GL_ACCOUNT
            ,CASE
                WHEN du.PK_USER IS NOT NULL THEN du.PK_USER
                WHEN du.PK_USER IS NULL AND glje.USER_KEY IS NOT NULL THEN -1
                ELSE -2
            END AS FK_USER
            ,CAST(COALESCE(glje.CREATION_DATE, '1900-01-01') AS DATE) AS FK_CREATION_DATE
            ,CAST(COALESCE(glje.ENTRY_DATE, '1900-01-01') AS DATE) AS FK_BOOKING_DATE
            ,glje.ENTRY_DATETIME AS BOOKING_DATETIME
            ,glje.CREATION_DATETIME AS CREATION_DATETIME
            ,glje.ENTRY_ID AS ENTRY_ID
            ,glje.TRANSACTION_ID AS TRANSACTION_ID
            ,reversal_glje.ENTRY_ID AS REVERSAL_ENTRY_ID
            ,CAST(CASE
                WHEN glje.TYPE = 'CREDIT' THEN glje.AMOUNT
                ELSE 0
            END AS DECIMAL(38,2)) AS CREDIT_AMOUNT
            ,CAST(CASE
                WHEN glje.TYPE = 'DEBIT' THEN glje.AMOUNT
                ELSE 0
            END AS DECIMAL(38,2)) AS DEBIT_AMOUNT
        FROM {env_var}_catalog.silver_con.mambu_gl_journal_entry glje
        LEFT JOIN {env_var}_catalog.gold_con.dim_gl_account dglam
        ON dglam.BK_GL_ACCOUNT = glje.GL_ACCOUNT_ENCODED_KEY_OID
        AND dglam.ROW_IS_CURRENT = 1
        LEFT JOIN {env_var}_catalog.gold_con.dim_user du
        ON du.BK_USER = glje.USER_KEY
        AND du.ROW_IS_CURRENT = 1 
        LEFT JOIN {env_var}_catalog.silver_con.mambu_gl_journal_entry reversal_glje
        ON reversal_glje.ENCODED_KEY = glje.REVERSAL_ENTRY_KEY
        AND reversal_glje.ROW_IS_CURRENT = 1 
     """

    df = spark.sql(query)

        # Add the gold layer audit column
    df = df.withColumns({
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(lit(LOCAL_TIME))
        })

    return df
