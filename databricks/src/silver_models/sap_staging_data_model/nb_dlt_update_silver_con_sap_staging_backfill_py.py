# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "sap_staging_sunrise_contact",
    comment = "the static sunrise data relating to the colleague"
)
@dlt.expect("CONTACT_ID is not null", "CONTACT_ID IS NOT NULL")
def silver_sap_staging_contact():

    df = spark.sql(f"""
    SELECT
    CAST(SERVICEREQNO AS BIGINT) AS CONTACT_ID,
    CAST(STAFFID AS BIGINT) AS STAFF_ID,
    FORENAME,
    NAME AS SURNAME,
    STATUS,
    TELEPHONE,
    MOBILE,
    JOBTITLE AS JOB_TITLE,
    STARTDT AS START_DATETIME,
    ENDDT AS END_DATETIME,
    CAST(ADDEDDATE AS TIMESTAMP) AS ADDED_DATETIME,
    CAST(UPDATEDDATE AS TIMESTAMP) AS UPDATED_DATETIME,
    CAST(`A#112001102` AS BIGINT) AS DEPARTMENT_ID,
    EMAIL,
    LINEMANAGER AS LINE_MANAGER,
    LINEMANEMAIL AS LINE_MANAGER_EMAIL,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_sap_staging_backfill_con.contacts c
    WHERE STAFFID RLIKE '^[0-9]+$'
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("CONTACT_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "sap_staging_sunrise_department",
    comment = "the static sunrise data relating to the department"
)
@dlt.expect("DEPARTMENT_ID is not null", "DEPARTMENT_ID IS NOT NULL")
def silver_sap_staging_department():

    df = spark.sql(f"""
    SELECT
    CAST(SERVICEREQNO AS BIGINT) AS DEPARTMENT_ID,
    NAME AS DEPARTMENT_NAME,
    CAST(ADDEDDATE AS TIMESTAMP) AS ADDED_DATETIME,
    CAST(UPDATEDDATE AS TIMESTAMP) AS UPDATED_DATETIME,
    STATUS,
    COSTCENTRE,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_sap_staging_backfill_con.departments
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("DEPARTMENT_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df
