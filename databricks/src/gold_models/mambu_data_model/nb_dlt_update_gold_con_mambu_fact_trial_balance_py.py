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

@dlt.table(
    name="fact_gl_trial_balance",
    comment="Daily trial balance with opening and closing balances"
)
def gold_fact_gl_trial_balance():
    # Execute SQL query to select and transform data
    query = f"""
        WITH daily_aggregation AS (
        SELECT
            fglje.FK_GL_ACCOUNT
            ,fglje.FK_BOOKING_DATE as FK_BOOKING_DATE
            ,SUM(fglje.DEBIT_AMOUNT) as TOTAL_DEBIT_AMOUNT
            ,SUM(fglje.CREDIT_AMOUNT) as TOTAL_CREDIIT_AMOUNT
            ,SUM(fglje.DEBIT_AMOUNT - fglje.CREDIT_AMOUNT) as NET_CHANGE
        FROM {env_var}_catalog.gold_con.fact_gl_journal_entry fglje
        GROUP BY fglje.FK_GL_ACCOUNT, fglje.FK_BOOKING_DATE
        )
        SELECT
            xxhash64(CONCAT(FK_GL_ACCOUNT, CAST(FK_BOOKING_DATE AS STRING))) AS PK_FACT_GL_TRIAL_BALANCE
            ,FK_GL_ACCOUNT
            ,FK_BOOKING_DATE
            ,SUM(NET_CHANGE) OVER (
                PARTITION BY FK_GL_ACCOUNT 
                ORDER BY FK_BOOKING_DATE
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS OPENING_BALANCE
            ,TOTAL_DEBIT_AMOUNT AS DEBIT_AMOUNT
            ,TOTAL_CREDIIT_AMOUNT AS CREDIT_AMOUNT
            ,NET_CHANGE
            ,COALESCE(
                SUM(NET_CHANGE) OVER (
                PARTITION BY FK_GL_ACCOUNT 
                ORDER BY FK_BOOKING_DATE
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) ,0
                ) AS CLOSING_BALANCE
        FROM daily_aggregation
     """

    df = spark.sql(query)

        # Add the gold layer audit column
    df = df.withColumns({
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(lit(LOCAL_TIME))
        })

    return df
