# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf
 
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "reference_data_auto_income_verification",
    comment = "Verification codes and descriptions"
)
@dlt.expect("VERIFICATION_CODE is not null", "VERIFICATION_CODE IS NOT NULL")
def silver_reference_data_auto_income_verification():

    df = spark.sql(f"""
        select 
        VERIFICATION_CODE,
        VERIFICATION_DESCRIPTION,
        cast(MDP_LOAD_ID AS DECIMAL),
        MDP_LOAD_DATETIME, 
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME
        FROM {env_var}_catalog.bronze_reference_data_int.auto_income_verification
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("VERIFICATION_CODE", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

        

# COMMAND ----------

@dlt.table(
    name = "reference_data_base_rate_loading",
    comment = "Details of the load timings of base rates"
)
@dlt.expect("RATE is not null", "RATE IS NOT NULL")
def silver_reference_data_base_rate_loading():

    df = spark.sql(f"""
     select
        CAST(RATE AS DECIMAL(4,2)) AS RATE,
        START_DATE,
        END_DATE,
        DATA_LOAD_RUN_DATE,
        DATA_LOAD_LAST_UPDATED,
        cast(MDP_LOAD_ID AS DECIMAL),
        MDP_LOAD_DATETIME, 
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME   
    FROM {env_var}_catalog.bronze_reference_data_int.base_rate_loading
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("RATE", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df


# COMMAND ----------

@dlt.table(
    name = "reference_data_mso_application_category",
    comment = "Table to hold MSO case stages, status' and aplication categories."
)
@dlt.expect("MSO_CASE_STAGE is not null", "MSO_CASE_STAGE IS NOT NULL")
def silver_reference_data_mso_application_category():

    df = spark.sql(f"""
        select
        MSO_CASE_STAGE,
        MSO_CASE_STATUS,
        APPLICATION_CATEGORY,
        PIPELINE_STATUS,
        cast(MDP_LOAD_ID AS DECIMAL),
        MDP_LOAD_DATETIME, 
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME   
    FROM {env_var}_catalog.bronze_reference_data_int.mso_application_category
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("MSO_CASE_STAGE", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df


# COMMAND ----------

@dlt.table(
    name = "reference_data_mso_marketing_source",
    comment = "Marketing source codes and types"
)
@dlt.expect("MARKETING_SOURCE_CODE is not null", "MARKETING_SOURCE_CODE IS NOT NULL")
def silver_reference_data_mso_marketing_source():

    df = spark.sql(f"""
        select
        MARKETING_SOURCE_CODE,
        MARKETING_SOURCE_TYPE,
        cast(MDP_LOAD_ID AS DECIMAL),
        MDP_LOAD_DATETIME, 
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME   
    FROM {env_var}_catalog.bronze_reference_data_int.mso_marketing_source
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("MARKETING_SOURCE_CODE", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df


# COMMAND ----------

@dlt.table(
    name = "reference_data_mso_message_type_order",
    comment = "Order of message types."
)
@dlt.expect("MESSAGE_TYPE is not null", "MESSAGE_TYPE IS NOT NULL")
def silver_reference_data_mso_message_type_order():

    df = spark.sql(f"""
        select
        MESSAGE_TYPE,
        MESSAGE_ORDER,
        cast(MDP_LOAD_ID AS DECIMAL),
        MDP_LOAD_DATETIME, 
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME   
    FROM {env_var}_catalog.bronze_reference_data_int.mso_message_type_order
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("MESSAGE_TYPE", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df


# COMMAND ----------

@dlt.table(
    name = "reference_data_mso_repayment_strategy",
    comment = "Table to hold repayment strategies."
)
@dlt.expect("NUMBER is not null", "NUMBER IS NOT NULL")
def silver_reference_data_mso_repayment_strategy():

    df = spark.sql(f"""
        select
        NUMBER,
        REGEXP_REPLACE((REPAYMENT_STRATEGY), "[A-Z]", " $0") AS REPAYMENT_STRATEGY,
        cast(MDP_LOAD_ID AS DECIMAL),
        MDP_LOAD_DATETIME, 
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME   
    FROM {env_var}_catalog.bronze_reference_data_int.mso_repayment_strategy
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df


# COMMAND ----------

@dlt.table(
    name = "reference_data_mso_valuation_status_order",
    comment = "Table to hold ranking of valuation statuses."
)
@dlt.expect("VALUATION_STATUS is not null", "VALUATION_STATUS IS NOT NULL")
def silver_reference_data_mso_valuation_status_order():

    df = spark.sql(f"""
        select
        VALUATION_STATUS,
        RANK,
        cast(MDP_LOAD_ID AS DECIMAL),
        MDP_LOAD_DATETIME, 
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME   
    FROM {env_var}_catalog.bronze_reference_data_int.mso_valuation_status_order
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("VALUATION_STATUS", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df


# COMMAND ----------

#SCD TABLE WITHIN SAP, DOES THIS NEED CHANGING?

@dlt.table(
    name = "reference_data_rdm_regions",
    comment = "Table to hold postcodes associated with regional development managers."
)
@dlt.expect("POSTCODE is not null", "POSTCODE IS NOT NULL")
def silver_reference_data_rdm_regions():

    df = spark.sql(f"""
        SELECT
        POSTCODE,
        RDM_NAME,
        REPORTING_STREAM,
        CAST(MDP_LOAD_ID AS DECIMAL),
        MDP_LOAD_DATETIME,
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_reference_data_int.rdm_regions
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("POSTCODE", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df


# COMMAND ----------

@dlt.table(
    name = "reference_data_lending_segments",
    comment = "Table to hold lneding segments."
)
@dlt.expect("LENDING_SEGMENT is not null", "LENDING_SEGMENT IS NOT NULL")
def silver_reference_data_lending_segments():

    df = spark.sql(f"""
        SELECT
        RISK_CATEGORY,
        LENDING_SEGMENT,
        RISK_CATEGORY_NARR,
        ANCHOR_SEGMENT_FLAG,
        REPORTING_CATEGORY,
        CAST(MDP_LOAD_ID AS DECIMAL),
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) MDP_LOAD_DATETIME,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_reference_data_int.lending_segments
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("LENDING_SEGMENT", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df


# COMMAND ----------

@dlt.table(
    name = "reference_data_channel",
    comment = "Table to hold channel data including codes branches and regions."
)
@dlt.expect("CHANNEL_KEY is not null", "CHANNEL_KEY IS NOT NULL")
def silver_reference_data_channel():
    df = spark.sql(f"""
        SELECT
        CHANNEL_KEY,
        CHANNEL_CODE,
        CHANNEL_NARRATIVE_LEV1,
        CHANNEL_NARRATIVE_LEV2,
        CHANNEL_NARRATIVE_LEV3,
        GEOG_REGION,
        OVERIDE_CODE AS OVERRIDE_CODE,
        POSTCODE,
        START_DATE,
        END_DATE,
        CAST(MDP_LOAD_ID AS DECIMAL),
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) MDP_LOAD_DATETIME,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_reference_data_int.channel
    """)
    # Define the window specification
    windowSpec = dlf.get_window_spec("CHANNEL_KEY", "MDP_LOAD_DATETIME")
    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)
    return df



# COMMAND ----------

@dlt.table(
    name="reference_data_azure_subscriptions_metadata",
    comment="This table stores curated metadata about Azure subscriptions to support cost management, reporting, chargeback, and governance processes."
)
@dlt.expect("ROW_ID is not null", "ROW_ID IS NOT NULL")
def silver_reference_data_azure_subscriptions_metadata():

    df = spark.sql(f"""
        SELECT
        ROW_ID,        
        SUBSCRIPTION,
        APPLICATION,
        CRITICALITY,
        ENVIRONMENT,
        OWNER,
        DATA_CLASSIFICATION,
        SUPPORT_CONTACT,
        CHARGE_CODE,
        DELIVERY_STREAM,
        MONITORING_REQUIRED,
        SERVICE_ID,
        CAST(MDP_LOAD_ID AS DECIMAL) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_reference_data_int.azure_subscriptions_metadata
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("ROW_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)
    return df
    
