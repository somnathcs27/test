# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "sap_transform_staff_details_scd",
    comment = "the static sunrise data relating to the department"
)
@dlt.expect("SK_KEY is not null", "SK_KEY IS NOT NULL")
def silver_sap_transform_staff_details_scd():

    df = spark.sql(f"""
    SELECT
    CAST(SK_KEY AS BIGINT) AS SK_KEY,
    CAST(COLLEAGUE_ID AS BIGINT) AS COLLEAGUE_ID,
    COLLEAGUE_NAME,
    JOB_TITLE,
    DEPARTMENT,
    COLLEAGUE_START_DATE,
    COLLEAGUE_LEAVE_DATE,
    SCD_START_DATE,
    SCD_END_DATE,
    SCD_CURRENT_FLAG,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_sap_transform_backfill_con.staff_details_scd_backup
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("SK_KEY", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df
