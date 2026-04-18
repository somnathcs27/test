# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name="general_ledger_user", 
    comment="Description of the user"
)
@dlt.expect("USER_ID is not null", "USER_ID IS NOT NULL")
def general_ledger_user():

    df = spark.sql(
        f"""
    SELECT   
        CAST(USER_ID AS BIGINT) AS USER_ID,
        USER_NAME AS USER_NAME,
        START_DATE AS START_DATE,
        END_DATE AS END_DATE,
        DESCRIPTION AS DESCRIPTION,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_int.fnd_user 
    """
    )

    windowSpec = dlf.get_window_spec(["USER_ID"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_account_type_description", 
    comment="Description of the account type"
)
@dlt.expect("ACCOUNT_CODE is not null", "ACCOUNT_CODE IS NOT NULL")
def general_ledger_account_type_description():

    df = spark.sql(
        f"""
    SELECT   
        CODE AS ACCOUNT_CODE,
        DESCR AS ACCOUNT_DESCRIPTION,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS ROW_START_DATETIME, --use the load datetime as its a full load into bronze so the table has no start and end date
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_reference_data_int.gl_account_type_descr
    """
    )

    windowSpec = dlf.get_window_spec(["ACCOUNT_CODE"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_journal_entry_lines", 
    comment="Description of the journal entry lines"
)
@dlt.expect("JE_HEADER_ID is not null", "JE_HEADER_ID IS NOT NULL")
def general_ledger_journal_entry_lines():

    df = spark.sql(
        f"""
    SELECT   
        CAST(JE_HEADER_ID AS BIGINT) AS JE_HEADER_ID,
	    CAST(JE_LINE_NUM AS BIGINT) AS JE_LINE_NUM,
	    CAST(CODE_COMBINATION_ID AS BIGINT) AS CODE_COMBINATION_ID,
	    EFFECTIVE_DATE,
        CAST(CREATED_BY AS BIGINT) AS CREATED_BY,
        CAST(LAST_UPDATED_BY AS BIGINT) AS LAST_UPDATED_BY,
        LAST_UPDATE_DATE,
        ATTRIBUTE2,
        CAST(CREATION_DATE AS DATE) AS CREATION_DATE,
	    CREATION_DATE AS CREATION_DATE_TIME,
        DESCRIPTION AS LINE_DESCRIPTION,
        CAST(ACCOUNTED_DR AS DECIMAL(12,2)) AS ACCOUNTED_DR,
        CAST(ACCOUNTED_CR AS DECIMAL(12,2)) AS ACCOUNTED_CR,
        CAST(ENTERED_DR AS DECIMAL(12,2)) AS ENTERED_DR,
        CAST(ENTERED_CR AS DECIMAL(12,2)) AS ENTERED_CR,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_int.gl_je_lines
    """
    )

    windowSpec = dlf.get_window_spec(["JE_HEADER_ID","JE_LINE_NUM"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_code_combinations", 
    comment="Description of the code combinations"
)
@dlt.expect("CODE_COMBINATION_ID is not null", "CODE_COMBINATION_ID IS NOT NULL")
def general_ledger_code_combinations():

    df = spark.sql(
        f"""
    SELECT
        CAST(CODE_COMBINATION_ID AS BIGINT) AS CODE_COMBINATION_ID,   
        SEGMENT1,
        SEGMENT2,
        SEGMENT3,
        SEGMENT4,
        SEGMENT5,
        ACCOUNT_TYPE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_int.gl_code_combinations
    """
    )

    windowSpec = dlf.get_window_spec(["CODE_COMBINATION_ID"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_import_references", 
    comment="Description of the import references"
)
@dlt.expect("JE_HEADER_ID is not null", "JE_HEADER_ID IS NOT NULL")
def general_ledger_import_references():

    df = spark.sql(
        f"""
    SELECT
        CAST(JE_HEADER_ID AS BIGINT) AS JE_HEADER_ID,
        CAST(JE_LINE_NUM AS BIGINT) AS JE_LINE_NUM,
        CAST(JE_BATCH_ID AS BIGINT) AS JE_BATCH_ID,
        CAST(GL_SL_LINK_ID AS BIGINT) AS GL_SL_LINK_ID,
        GL_SL_LINK_TABLE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_int.gl_import_references
    """
    )

    windowSpec = dlf.get_window_spec(["JE_HEADER_ID","JE_BATCH_ID","JE_HEADER_ID","JE_LINE_NUM","GL_SL_LINK_ID","GL_SL_LINK_TABLE"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_suppliers", 
    comment="Description of the suppliers"
)
@dlt.expect("SUPPLIER_ID is not null", "SUPPLIER_ID IS NOT NULL")
def general_ledger_user():

    df = spark.sql(
        f"""
    SELECT
        CAST(VENDOR_ID AS BIGINT) AS SUPPLIER_ID,
        VENDOR_NAME AS SUPPLIER_NAME,
        SEGMENT1 AS SUPPLIER_NUMBER,
        END_DATE_ACTIVE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_int.ap_suppliers
    """
    )

    windowSpec = dlf.get_window_spec(["SUPPLIER_ID"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_fnd_flex_values", 
    comment="Description of the fnd flex values"
)
@dlt.expect("FLEX_VALUE_SET_ID is not null", "FLEX_VALUE_SET_ID IS NOT NULL")
def general_ledger_fnd_flex_values():

    df = spark.sql(
        f"""
    SELECT   
        CAST(FLEX_VALUE_SET_ID AS BIGINT) AS FLEX_VALUE_SET_ID,
        CAST(FLEX_VALUE_ID AS BIGINT) AS FLEX_VALUE_ID,
        FLEX_VALUE,
        HIERARCHY_LEVEL,
        ENABLED_FLAG,
        SUMMARY_FLAG,
        START_DATE_ACTIVE AS ACTIVE_START_DATE,
        END_DATE_ACTIVE AS ACTIVE_END_DATE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_ebs_int.fnd_flex_values ffv
  
    """
    )

    windowSpec = dlf.get_window_spec(["FLEX_VALUE_SET_ID","FLEX_VALUE_ID"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="general_ledger_quantum_control_codes",
    comment="Description of the quantum control codes",
)
@dlt.expect("GL_ACCOUNT is not null", "GL_ACCOUNT IS NOT NULL")
def general_ledger_quantum_control_codes():

    df = spark.sql(
        f"""
    SELECT 
        CAST(GL_ACCOUNT AS BIGINT) AS GL_ACCOUNT,
        CONTROL_CODE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        MDP_LOAD_DATETIME AS ROW_START_DATETIME, --use the load datetime as its a full load into bronze so the table has no start and end date
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_reference_data_int.quantum_control_codes

    """
    )

    windowSpec = dlf.get_window_spec(["GL_ACCOUNT"], "MDP_LOAD_DATETIME")

    df = dlf.get_row_number(df, windowSpec)

    return df
