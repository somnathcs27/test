# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name="general_ledger_fnd_flex_values_tl", 
    comment="Description of the fnd flex values tl"
)
@dlt.expect("FLEX_VALUE_ID is not null", "FLEX_VALUE_ID IS NOT NULL")
def general_ledger_fnd_flex_values_tl():

    df = spark.sql(
        f"""
    SELECT   
        CAST(FLEX_VALUE_ID AS BIGINT) AS FLEX_VALUE_ID,  
        DESCRIPTION,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_con.fnd_flex_values_tl
    """
    )

    windowSpec = dlf.get_window_spec("FLEX_VALUE_ID", "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_seg_val_norm_hierarchy",
    comment="Description of the seg val norm hierarchy",
)
@dlt.expect("FLEX_VALUE_SET_ID is not null", "FLEX_VALUE_SET_ID IS NOT NULL")
def general_ledger_seg_val_norm_hierarchy():

    df = spark.sql(
        f"""
    SELECT 
        CAST(FLEX_VALUE_SET_ID AS BIGINT) AS FLEX_VALUE_SET_ID,
        PARENT_FLEX_VALUE,
        CHILD_FLEX_VALUE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_con.gl_seg_val_norm_hierarchy 
    """
    )

    windowSpec = dlf.get_window_spec(["FLEX_VALUE_SET_ID", "PARENT_FLEX_VALUE", "CHILD_FLEX_VALUE"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec) .drop("row_num")

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_wf_action_history", 
    comment="Description of the wf action history"
)
@dlt.expect("OBJECT_ID is not null", "OBJECT_ID IS NOT NULL")
def general_ledger_wf_action_history():

    df = spark.sql(
        f"""
    SELECT   
        CAST(OBJECT_ID AS BIGINT) AS OBJECT_ID,
        WF_APPROVER_NAME AS APPROVER_NAME,
        ROW_NUMBER() OVER(PARTITION BY OBJECT_ID,ACTION_CODE ORDER BY OBJECT_ID,ACTION_CODE,SEQUENCE_NUM DESC) AS ACTION_RANK,
        ACTION_CODE AS ACTION_CODE,
        CAST(ACTION_DATE AS DATE) AS ACTION_DATE,
        ACTION_DATE AS ACTION_DATETIME,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_con.gl_wf_action_history 
    """
    )

    windowSpec = dlf.get_window_spec(["OBJECT_ID"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec) .drop("row_num")

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_journal_entry_headers", 
    comment="Description of the journal entry headers"
)
@dlt.expect("JE_HEADER_ID is not null", "JE_HEADER_ID IS NOT NULL")
def general_ledger_journal_entry_headers():

    df = spark.sql(
        f"""
    SELECT
        CAST(JE_HEADER_ID AS BIGINT) AS JE_HEADER_ID,
        CAST(JE_BATCH_ID AS BIGINT) AS JE_BATCH_ID,
        STATUS,
        CAST(DOC_SEQUENCE_VALUE AS BIGINT) AS DOC_SEQUENCE_VALUE,
        DESCRIPTION,
        JE_SOURCE,
        ACTUAL_FLAG,
        CURRENCY_CODE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_con.gl_je_headers
    """
    )

    windowSpec = dlf.get_window_spec(["JE_HEADER_ID"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec) .drop("row_num")

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_xla_ae_lines", 
    comment="The XLA_AE_LINES table stores the subledger journal entry lines. There is a one-to-many relationship between subledger journal entry headers and subledger journal entry lines."
)
@dlt.expect("GL_SL_LINK_ID is not null", "GL_SL_LINK_ID IS NOT NULL")
def general_ledger_user():

    df = spark.sql(
        f"""
    SELECT
        CAST(GL_SL_LINK_ID AS BIGINT) AS GL_SL_LINK_ID,
        GL_SL_LINK_TABLE,
        CAST(PARTY_ID AS BIGINT) AS PARTY_ID,
        CAST(AE_HEADER_ID AS BIGINT) AS AE_HEADER_ID,
        CAST(AE_LINE_NUM AS BIGINT) AS AE_LINE_NUM,
        CAST(ENTERED_DR AS DECIMAL(12,2)) AS ENTERED_DR,
        CAST(ENTERED_CR AS DECIMAL(12,2)) AS ENTERED_CR,
        CAST(ACCOUNTED_DR AS DECIMAL(12,2)) AS ACCOUNTED_DR,
        CAST(ACCOUNTED_CR AS DECIMAL(12,2)) AS ACCOUNTED_CR,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_con.xla_ae_lines
    """
    )

    windowSpec = dlf.get_window_spec(["AE_HEADER_ID","AE_LINE_NUM","GL_SL_LINK_ID","PARTY_ID","GL_SL_LINK_TABLE"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec) .drop("row_num")

    return df
