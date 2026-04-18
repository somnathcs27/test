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
    name = "dim_general_ledger_segment",
    comment = "the segement associated to the general ledger"
)
def gold_dim_general_ledger_segment():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
        ffv.FLEX_VALUE AS BK_GENERAL_LEDGER_SEGMENT,
        CASE 
            WHEN ffv.FLEX_VALUE_SET_ID = 1004327 THEN 'Segment 1'
            WHEN ffv.FLEX_VALUE_SET_ID = 1004326 THEN 'Segment 2'
            WHEN ffv.FLEX_VALUE_SET_ID = 1004331 THEN 'Segment 3'
            WHEN ffv.FLEX_VALUE_SET_ID = 1004332 THEN 'Segment 4'
            WHEN ffv.FLEX_VALUE_SET_ID = 1004333 THEN 'Segment 5'
            ELSE 'Unknown'
        END AS SEGMENT_TYPE,
        COALESCE(tl.DESCRIPTION,'Unknown') AS SEGMENT_DESCRIPTION,
        CASE 
            WHEN ffv.ENABLED_FLAG = 'Y' THEN 'Yes'
            WHEN ffv.ENABLED_FLAG = 'N' THEN 'No' 
            ELSE 'Unknown' 
        END AS IS_SEGMENT_ENABLED, 
        CASE
            WHEN ffv.SUMMARY_FLAG = 'Y' THEN 'Yes' 
            WHEN ffv.SUMMARY_FLAG = 'N' THEN 'No' 
            ELSE 'Unknown'
        END AS IS_SEGMENT_SUMMARY,
        ffv.ACTIVE_START_DATE AS SEGMENT_START_DATE,
        ffv.ACTIVE_END_DATE AS SEGMENT_END_DATE,
        ffv.MDP_LOAD_ID,
        ffv.MDP_LOAD_DATETIME,
        ffv.ROW_START_DATETIME,
        ffv.ROW_END_DATETIME,
        ffv.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values AS ffv
    INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl AS tl
    ON ffv.FLEX_VALUE_ID = tl.FLEX_VALUE_ID
    AND ffv.FLEX_VALUE_SET_ID IN (1004327,1004326,1004331,1004332,1004333)
    """
    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_SEGMENT", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_SEGMENT",
        "BK_GENERAL_LEDGER_SEGMENT",
        "SEGMENT_TYPE",
        "SEGMENT_DESCRIPTION",
        "IS_SEGMENT_ENABLED",
        "IS_SEGMENT_SUMMARY",
        "SEGMENT_START_DATE",
        "SEGMENT_END_DATE",
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
    name = "dim_general_ledger_account",
    comment = "the account associated with the general ledger"
)
def gold_dim_general_ledger_account():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
        CASE 
            WHEN e.DESCRIPTION LIKE '%(Acc)%' THEN CONCAT(d.CHILD_FLEX_VALUE,'|','ACC')
            WHEN e.DESCRIPTION LIKE '%(BofE)%' THEN CONCAT(d.CHILD_FLEX_VALUE,'|','BOE')
            ELSE 'Unknown'
        END AS BK_GENERAL_LEDGER_ACCOUNT,
        a.FLEX_VALUE_SET_ID AS ACCOUNT_SET_ID,
       'YBS ACCOUNT' AS ACCOUNT_SET_NAME,     
        CASE 
            WHEN e.DESCRIPTION LIKE '%(Acc)%' THEN 'ACC'
            WHEN e.DESCRIPTION LIKE '%(BofE)%' THEN 'BOE'
            ELSE 'Unknown'
        END AS ACCOUNT_DESCRIPTION,  
        COALESCE(a.FLEX_VALUE,'Unknown') AS LEVEL1_CODE,
        COALESCE(e.DESCRIPTION,'Unknown') AS LEVEL1_DESCRIPTION,
        COALESCE(b.CHILD_FLEX_VALUE,'Unknown') AS LEVEL2_CODE,
        COALESCE(f.DESCRIPTION,'Unknown') AS LEVEL2_DESCRIPTION,
        COALESCE(c.CHILD_FLEX_VALUE,'Unknown') AS LEVEL3_CODE,
        COALESCE(g.DESCRIPTION,'Unknown') AS LEVEL3_DESCRIPTION,
        COALESCE(d.CHILD_FLEX_VALUE,'Unknown') AS LEVEL4_CODE,
        COALESCE(h.DESCRIPTION,'Unknown') AS LEVEL4_DESCRIPTION,
        COALESCE(qcc.CONTROL_CODE,'Unknown') AS CONTROL_CODE,
        a.ACTIVE_START_DATE AS ACCOUNT_START_DATE,
        a.ACTIVE_END_DATE AS ACCOUNT_END_DATE,
        a.MDP_LOAD_ID,
        a.MDP_LOAD_DATETIME,
        a.ROW_START_DATETIME,
        a.ROW_END_DATETIME,
        a.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values AS a
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_seg_val_norm_hierarchy AS b
    ON a.FLEX_VALUE = b.PARENT_FLEX_VALUE 
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_seg_val_norm_hierarchy c 
    ON b.CHILD_FLEX_VALUE = c.PARENT_FLEX_VALUE
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_seg_val_norm_hierarchy d 
    ON c.CHILD_FLEX_VALUE = d.PARENT_FLEX_VALUE
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl e 
    ON a.FLEX_VALUE_ID = e.FLEX_VALUE_ID
    LEFT JOIN 
                (
                    SELECT      
                        a.FLEX_VALUE,
                        b.DESCRIPTION
                    FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values a
                    INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl b 
                    ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                    AND a.ENABLED_FLAG = 'Y'
                ) f ON b.CHILD_FLEX_VALUE = f.FLEX_VALUE
    LEFT JOIN  
                (
                    SELECT      
                        a.FLEX_VALUE,
                        b.DESCRIPTION
                    FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values a
                    INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl b 
                    ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                    AND a.ENABLED_FLAG = 'Y'
                ) g ON c.CHILD_FLEX_VALUE = g.FLEX_VALUE
    LEFT JOIN  
                (
                    SELECT      
                        a.FLEX_VALUE,
                        b.DESCRIPTION,
                        a.FLEX_VALUE_SET_ID
                    FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values a
                    INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl b 
                    ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                    AND a.ENABLED_FLAG = 'Y'
                ) h ON d.CHILD_FLEX_VALUE = h.FLEX_VALUE AND d.FLEX_VALUE_SET_ID = h.FLEX_VALUE_SET_ID
    LEFT JOIN {env_var}_catalog.silver_int.general_ledger_quantum_control_codes qcc
    ON d.CHILD_FLEX_VALUE = qcc.gl_account
    WHERE a.FLEX_VALUE_SET_ID IN (1004326) --YBSACCOUNT structure
    AND a.HIERARCHY_LEVEL = 'LVL1' --start at level 1 then join the child value from b onto a, then the parent from c onto the child from b....etc..
    AND d.CHILD_FLEX_VALUE IS NOT NULL --only interested in codes that are mapped from top to bottom (lvl4 = segment2)
    AND (e.DESCRIPTION LIKE '%(Acc)%' OR e.DESCRIPTION LIKE '%(BofE)%')--only interested in the accounts structure within YBSACCOUNT
    AND a.ENABLED_FLAG = 'Y'
 
    """

    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_ACCOUNT", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_ACCOUNT",
        "BK_GENERAL_LEDGER_ACCOUNT",
        "ACCOUNT_SET_ID",
        "ACCOUNT_SET_NAME",
        "ACCOUNT_DESCRIPTION",  
        "LEVEL1_CODE",
        "LEVEL1_DESCRIPTION",
        "LEVEL2_CODE",
        "LEVEL2_DESCRIPTION",
        "LEVEL3_CODE",
        "LEVEL3_DESCRIPTION",
        "LEVEL4_CODE",
        "LEVEL4_DESCRIPTION",
        "CONTROL_CODE",
        "ACCOUNT_START_DATE",
        "ACCOUNT_END_DATE",
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
    name = "dim_general_ledger_cost_centre",
    comment = "the cost centre associated with the general ledger"
)
def gold_dim_general_ledger_cost_centre():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
        d.CHILD_FLEX_VALUE AS BK_GENERAL_LEDGER_COST_CENTRE,
        a.FLEX_VALUE_SET_ID AS COST_CENTRE_SET_ID,
        'YBS COST CENTRE' AS COST_CENTRE_SET_NAME,
        COALESCE(a.FLEX_VALUE,'Unknown') AS LEVEL1_CODE,
        COALESCE(e.DESCRIPTION,'Unknown') AS LEVEL1_DESCRIPTION,
        COALESCE(b.CHILD_FLEX_VALUE,'Unknown') AS LEVEL2_CODE,
        COALESCE(f.DESCRIPTION,'Unknown') AS LEVEL2_DESCRIPTION,
        COALESCE(c.CHILD_FLEX_VALUE,'Unknown') AS LEVEL3_CODE,
        COALESCE(g.DESCRIPTION,'Unknown') AS LEVEL3_DESCRIPTION,
        COALESCE(d.CHILD_FLEX_VALUE,'Unknown') AS LEVEL4_CODE,
        COALESCE(h.DESCRIPTION,'Unknown') AS LEVEL4_DESCRIPTION,
        a.MDP_LOAD_ID,
        a.MDP_LOAD_DATETIME,
        a.ROW_START_DATETIME,
        a.ROW_END_DATETIME,
        a.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values AS a
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_seg_val_norm_hierarchy AS b
    ON a.FLEX_VALUE = b.PARENT_FLEX_VALUE 
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_seg_val_norm_hierarchy c 
    ON b.CHILD_FLEX_VALUE = c.PARENT_FLEX_VALUE
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_seg_val_norm_hierarchy d 
    ON c.CHILD_FLEX_VALUE = d.PARENT_FLEX_VALUE
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl e 
    ON a.FLEX_VALUE_ID = e.FLEX_VALUE_ID
    LEFT JOIN 
            (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION
                FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values a
                INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                AND a.ENABLED_FLAG = 'Y'
            ) f ON b.CHILD_FLEX_VALUE = f.FLEX_VALUE
    LEFT JOIN   
            (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION
                FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values a
                INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                AND a.ENABLED_FLAG = 'Y'
            ) g ON c.CHILD_FLEX_VALUE = g.FLEX_VALUE
    LEFT JOIN   
            (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION,
                    a.FLEX_VALUE_SET_ID
                FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values a
                INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                AND a.ENABLED_FLAG = 'Y'
            ) h ON d.CHILD_FLEX_VALUE = h.FLEX_VALUE AND d.FLEX_VALUE_SET_ID = h.FLEX_VALUE_SET_ID
    WHERE a.FLEX_VALUE_SET_ID IN (1004331) --YBSACCOUNT structure
    AND a.HIERARCHY_LEVEL = 'LVL1' --start at level 1 then join the child value from b onto a, then the parent from c onto the child from b....etc..
    AND d.CHILD_FLEX_VALUE IS NOT NULL --only interested in codes that are mapped from top to bottom (lvl4 = segment2)
    AND a.ENABLED_FLAG = 'Y'
 
    """

    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_COST_CENTRE", monotonically_increasing_id() +1)
 
    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_COST_CENTRE",
        "BK_GENERAL_LEDGER_COST_CENTRE", 
        "COST_CENTRE_SET_ID",
        "COST_CENTRE_SET_NAME",
        "LEVEL1_CODE",
        "LEVEL1_DESCRIPTION",
        "LEVEL2_CODE",
        "LEVEL2_DESCRIPTION",
        "LEVEL3_CODE",
        "LEVEL3_DESCRIPTION",
        "LEVEL4_CODE",
        "LEVEL4_DESCRIPTION",
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
    name = "dim_general_ledger_company",
    comment = "the company associated with the general ledger"
)
def gold_dim_general_ledger_company():
    # Execute SQL query to select and transform data
    query = f"""
       SELECT   
        ffv.FLEX_VALUE AS BK_GENERAL_LEDGER_COMPANY,
        COALESCE(tl.DESCRIPTION,'Unknown') AS COMPANY_DESCRIPTION,
        CASE 
            WHEN ffv.ENABLED_FLAG = 'Y' THEN 'Yes' 
            WHEN ffv.ENABLED_FLAG = 'N' THEN 'No'
            ELSE 'Unknown' 
        END AS IS_COMPANY_ENABLED,
        CASE 
            WHEN ffv.SUMMARY_FLAG = 'Y' THEN 'Yes' 
            WHEN ffv.SUMMARY_FLAG = 'N' THEN 'No' 
            ELSE 'Unknown' 
        END AS IS_COMPANY_SUMMARY,
        ffv.ACTIVE_START_DATE AS COMPANY_START_DATE,
        ffv.ACTIVE_END_DATE AS COMPANY_END_DATE,
        ffv.MDP_LOAD_ID,
        ffv.MDP_LOAD_DATETIME,
        ffv.ROW_START_DATETIME,
        ffv.ROW_END_DATETIME,
        ffv.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values ffv
    INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl tl
    ON ffv.FLEX_VALUE_ID = tl.FLEX_VALUE_ID
    AND ffv.FLEX_VALUE_SET_ID = 1004327
 
    """

    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_COMPANY", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_COMPANY",
        "BK_GENERAL_LEDGER_COMPANY",  
        "COMPANY_DESCRIPTION",
        "IS_COMPANY_ENABLED",
        "IS_COMPANY_SUMMARY",
        "COMPANY_START_DATE",
        "COMPANY_END_DATE",
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
    name = "dim_general_ledger_organisation",
    comment = "the organisation associated with the general ledger"
)
def gold_dim_general_ledger_organisation():
    # Execute SQL query to select and transform data
    query = f"""
       SELECT   
        ffv.FLEX_VALUE AS BK_GENERAL_LEDGER_ORGANISATION,
        COALESCE(tl.DESCRIPTION,'Unknown') AS ORGANISATION_DESCRIPTION,
        CASE 
            WHEN ffv.ENABLED_FLAG = 'Y' THEN 'Yes' 
            WHEN ffv.ENABLED_FLAG = 'N' THEN 'No'
            ELSE 'Unknown' 
        END AS IS_ORGANISATION_ENABLED,
        CASE 
            WHEN ffv.SUMMARY_FLAG = 'Y' THEN 'Yes' 
            WHEN ffv.SUMMARY_FLAG = 'N' THEN 'No'
            ELSE 'Unknown' 
        END AS IS_ORGANISATION_SUMMARY,
        ffv.ACTIVE_START_DATE AS OGRANISATION_START_DATE,
        ffv.ACTIVE_END_DATE AS ORGANISATION_END_DATE,
        ffv.MDP_LOAD_ID,
        ffv.MDP_LOAD_DATETIME,
        ffv.ROW_START_DATETIME,
        ffv.ROW_END_DATETIME,
        ffv.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.general_ledger_fnd_flex_values ffv
    INNER JOIN {env_var}_catalog.silver_con.general_ledger_fnd_flex_values_tl tl
    ON ffv.FLEX_VALUE_ID = tl.FLEX_VALUE_ID
    AND ffv.FLEX_VALUE_SET_ID = 1004333
 
    """

    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_ORGANISATION", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_ORGANISATION",
        "BK_GENERAL_LEDGER_ORGANISATION",  
        "ORGANISATION_DESCRIPTION",
        "IS_ORGANISATION_ENABLED",
        "IS_ORGANISATION_SUMMARY",
        "OGRANISATION_START_DATE",
        "ORGANISATION_END_DATE",
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
    name = "dim_general_ledger_journal_entry_header",
    comment = "the journal entry header associated with the general ledger"
)
def gold_dim_general_ledger_journal_entry_header():
    # Execute SQL query to select and transform data
    query = f"""
   
    SELECT
        jeh.JE_HEADER_ID AS BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER,
        jeh.JE_BATCH_ID AS JOURNAL_ENTRY_BATCH_ID,
        COALESCE(jeh.STATUS,'Unknown') AS JOURNAL_STATUS,
        COALESCE(jeh.DOC_SEQUENCE_VALUE,0) AS JOURNAL_SEQUENCE_VALUE,
        COALESCE(jeh.DESCRIPTION,'Unknown') AS JOURNAL_DESCRIPTION,
        COALESCE(jeh.JE_SOURCE,'Unknown') AS JOURNAL_SOURCE,
        COALESCE(jeh.ACTUAL_FLAG,'Unknown') AS ACTUAL_FLAG,
        COALESCE(jeh.CURRENCY_CODE,'Unknown') AS CURRENCY_CODE,
        jeh.MDP_LOAD_ID,
        jeh.MDP_LOAD_DATETIME,
        jeh.ROW_START_DATETIME,
        jeh.ROW_END_DATETIME,
        jeh.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.general_ledger_journal_entry_headers jeh

    """
    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER", monotonically_increasing_id() +1)

    # # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER",
        "BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER",
        "JOURNAL_ENTRY_BATCH_ID",
        "JOURNAL_STATUS",
        "JOURNAL_SEQUENCE_VALUE",
        "JOURNAL_DESCRIPTION",
        "JOURNAL_SOURCE",
        "ACTUAL_FLAG",
        "CURRENCY_CODE",
        "MDP_LOAD_ID",
        "MDP_LOAD_DATETIME",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT"
    )

    #insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    return df
