# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "bottomline_payments_matched_accounts",
    comment = "data from account matching process"
)
@dlt.expect("PAYMENTS_MATCHED_ACCOUNTS_ID is not null", "PAYMENTS_MATCHED_ACCOUNTS_ID IS NOT NULL")
def silver_bottomline_matched_accounts():

    df = spark.sql(f"""
    SELECT
    id AS PAYMENTS_MATCHED_ACCOUNTS_ID,
    customer_id AS CUSTOMER_ID,
    customer_name AS CUSTOMER_NAME,
    identification AS IDENTIFICATION,
    date_created_utc AS CREATED_DATETIME,
    date_last_verified_utc AS LAST_VERIFIED_DATETIME,
    account_type AS ACCOUNT_TYPE,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_bottomline_con.matchedaccounts
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("PAYMENTS_MATCHED_ACCOUNTS_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "bottomline_payments_name_verification_audit_records",
    comment = "data from the verification audit records"
)
@dlt.expect("PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS_ID is not null", "PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS_ID IS NOT NULL")
def silver_bottomline_matched_accounts():

    df = spark.sql(f"""
    SELECT
    id AS PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS_ID,
    interaction_id AS INTERACTION_ID,
    scheme_name AS SCHEMA_NAME,
    account_type AS ACCOUNT_TYPE,
    identification AS IDENTIFICATION,
    name AS CUSTOMER_NAME,
    verification_matched AS VERIFICATION_MATCHED,
    verification_reason_code AS VERIFICATION_REASON_CODE,
    verification_name AS VERIFICATION_NAME,
    links_self AS LINKS_SELF,
    code AS CODE,
    message AS MESSAGE,
    errors_csv AS ERRORS_CSV,
    customer_id AS CUSTOMER_ID,
    branch_id AS BRANCH_ID,
    colleague_id AS COLLEAGUE_ID,
    platform_application_journey AS PLATFORM_APPLICATION_JOURNEY,
    session_id AS SESSION_ID,
    date_created_utc AS CREATED_DATETIME,
    verification_source AS VERIFICATION_SOURCE,
    secondary_identification AS SECONDARY_IDENTIFICATION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_bottomline_con.nameverificationauditrecords
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df
