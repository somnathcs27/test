# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id, md5, xxhash64
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "fact_general_ledger",
    comment = "the general ledger fact table"
)
def gold_fact_general_ledger():
    # Execute SQL query to select and transform data
    query = f"""
    WITH cte_stage_general_ledger AS (
    SELECT
        jel.JE_HEADER_ID AS JOURNAL_ENTRY_HEADER_ID,
        jel.JE_LINE_NUM AS JOURNAL_ENTRY_LINE_NUMBER,
        COALESCE(xla.AE_HEADER_ID,-2) AS AE_HEADER_ID,
        COALESCE(xla.AE_LINE_NUM,-2) AS AE_LINE_NUM,
        cc.SEGMENT1,
        cc.SEGMENT2,
        CONCAT(cc.SEGMENT2,'|','ACC') AS SEGMENT2_ACC,
        CONCAT(cc.SEGMENT2,'|','BOE') AS SEGMENT2_BOE,
        cc.SEGMENT3,
        cc.SEGMENT4,
        cc.SEGMENT5,
        jel.CREATED_BY,
        jel.LAST_UPDATED_BY,
        cc.ACCOUNT_TYPE,
        s.SUPPLIER_ID,
        CAST(COALESCE(jel.CREATION_DATE,'1900-01-01') AS DATE) AS FK_DATE_CREATION_DATE,
        CAST(COALESCE(jel.LAST_UPDATE_DATE,'1900-01-01') AS DATE) AS FK_DATE_LAST_UPDATE_DATE,
        CAST(COALESCE(jel.EFFECTIVE_DATE,'1900-01-01') AS DATE) AS FK_DATE_EFFECTIVE_DATE,
        jel.CREATION_DATE AS CREATION_DATETIME,
        jel.LAST_UPDATE_DATE AS LAST_UPDATE_DATETIME,
        jel.EFFECTIVE_DATE AS EFFECTIVE_DATETIME,
        CAST(COALESCE(wf_ah.ACTION_DATE,'1900-01-01') AS DATE) AS FK_DATE_APPROVAL_DATE,
        CAST(wf_ah.ACTION_DATE AS TIMESTAMP) AS APPROVAL_DATETIME,
        wf_ah.APPROVER_NAME,
        (jel.ACCOUNTED_DR - jel.ACCOUNTED_CR) AS ACCOUNTED_NET,
        (jel.ENTERED_DR - jel.ENTERED_CR) AS ENTERED_NET,
        xla.ENTERED_CR AS ENTERED_CR_SUPPLIER,
        xla.ENTERED_DR AS ENTERED_DR_SUPPLIER,
        xla.ACCOUNTED_DR AS ACCOUNTED_DR_SUPPLIER,
        xla.ACCOUNTED_CR AS ACCOUNTED_CR_SUPPLIER,
        CASE WHEN jeh.JE_SOURCE = 'Payables' THEN xla.ENTERED_CR ELSE jel.ENTERED_CR END AS REVISED_ENTERED_CR,
        CASE WHEN jeh.JE_SOURCE = 'Payables' THEN xla.ENTERED_DR ELSE jel.ENTERED_DR END AS REVISED_ENTERED_DR,
        CASE WHEN jeh.JE_SOURCE = 'Payables' THEN xla.ACCOUNTED_CR ELSE jel.ACCOUNTED_CR END AS REVISED_ACCOUNTED_CR,
        CASE WHEN jeh.JE_SOURCE = 'Payables' THEN xla.ACCOUNTED_DR ELSE jel.ACCOUNTED_DR END AS REVISED_ACCOUNTED_DR    
    FROM {env_var}_catalog.silver_int.general_ledger_journal_entry_lines AS jel
    LEFT JOIN {env_var}_catalog.silver_int.general_ledger_code_combinations AS cc
    ON jel.CODE_COMBINATION_ID = cc.CODE_COMBINATION_ID
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_journal_entry_headers jeh
    ON jel.JE_HEADER_ID = jeh.JE_HEADER_ID
    LEFT JOIN {env_var}_catalog.silver_int.general_ledger_import_references ref
    on jel.JE_HEADER_ID = ref.JE_HEADER_ID
    and jel.JE_LINE_NUM = ref.JE_LINE_NUM
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_xla_ae_lines xla
    on xla.GL_SL_LINK_ID = ref.GL_SL_LINK_ID
    and xla.GL_SL_LINK_TABLE = ref.GL_SL_LINK_TABLE
    LEFT JOIN {env_var}_catalog.silver_int.general_ledger_suppliers s
    on xla.PARTY_ID = s.SUPPLIER_ID
    LEFT JOIN {env_var}_catalog.silver_con.general_ledger_wf_action_history wf_ah
    ON jeh.JE_BATCH_ID = wf_ah.OBJECT_ID
    AND wf_ah.ACTION_RANK = 1 AND wf_ah.ACTION_CODE = 'APPROVE'
    )

    SELECT
        CONCAT(stage.JOURNAL_ENTRY_HEADER_ID,'|',stage.JOURNAL_ENTRY_LINE_NUMBER,'|',stage.AE_HEADER_ID,'|',stage.AE_LINE_NUM) AS PK_FACT_GENERAL_LEDGER, 
        CASE 
            WHEN ISNULL(seg1.PK_GENERAL_LEDGER_SEGMENT) = 0 THEN seg1.PK_GENERAL_LEDGER_SEGMENT
            WHEN ISNULL(seg1.PK_GENERAL_LEDGER_SEGMENT) = 1 AND ISNULL(stage.SEGMENT1) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_SEGMENT_1,
        CASE 
            WHEN ISNULL(seg2.PK_GENERAL_LEDGER_SEGMENT) = 0 THEN seg2.PK_GENERAL_LEDGER_SEGMENT
            WHEN ISNULL(seg2.PK_GENERAL_LEDGER_SEGMENT) = 1 AND ISNULL(stage.SEGMENT2) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_SEGMENT_2,
        CASE 
            WHEN ISNULL(seg3.PK_GENERAL_LEDGER_SEGMENT) = 0 THEN seg3.PK_GENERAL_LEDGER_SEGMENT
            WHEN ISNULL(seg3.PK_GENERAL_LEDGER_SEGMENT) = 1 AND ISNULL(stage.SEGMENT3) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_SEGMENT_3,
        CASE 
            WHEN ISNULL(seg4.PK_GENERAL_LEDGER_SEGMENT) = 0 THEN seg4.PK_GENERAL_LEDGER_SEGMENT
            WHEN ISNULL(seg4.PK_GENERAL_LEDGER_SEGMENT) = 1 AND ISNULL(stage.SEGMENT4) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_SEGMENT_4,
        CASE 
            WHEN ISNULL(seg5.PK_GENERAL_LEDGER_SEGMENT) = 0 THEN seg5.PK_GENERAL_LEDGER_SEGMENT
            WHEN ISNULL(seg5.PK_GENERAL_LEDGER_SEGMENT) = 1 AND ISNULL(stage.SEGMENT5) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_SEGMENT_5,
        CASE
            WHEN ISNULL(user_cb.PK_GENERAL_LEDGER_USER) = 0 THEN user_cb.PK_GENERAL_LEDGER_USER
            WHEN ISNULL(user_cb.PK_GENERAL_LEDGER_USER) = 1 AND ISNULL(stage.CREATED_BY) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_USER_CREATED_BY,
        CASE
            WHEN ISNULL(user_lub.PK_GENERAL_LEDGER_USER) = 0 THEN user_lub.PK_GENERAL_LEDGER_USER
            WHEN ISNULL(user_lub.PK_GENERAL_LEDGER_USER) = 1 AND ISNULL(stage.LAST_UPDATED_BY) = 0 THEN -1      
            ELSE -2
        END AS FK_GENERAL_LEDGER_USER_LAST_UPDATED_BY,
        CASE
            WHEN ISNULL(user_approve.PK_GENERAL_LEDGER_USER) = 0 THEN user_approve.PK_GENERAL_LEDGER_USER
            WHEN ISNULL(user_approve.PK_GENERAL_LEDGER_USER) = 1 AND ISNULL(stage.APPROVER_NAME) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_USER_APPROVER,
        CASE 
            WHEN ISNULL(at.PK_GENERAL_LEDGER_ACCOUNT_TYPE) = 0 THEN at.PK_GENERAL_LEDGER_ACCOUNT_TYPE
            WHEN ISNULL(at.PK_GENERAL_LEDGER_ACCOUNT_TYPE) = 1 AND ISNULL(stage.ACCOUNT_TYPE) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_ACCOUNT_TYPE,        
        CASE
            WHEN ISNULL(s.PK_GENERAL_LEDGER_SUPPLIER) = 0 THEN s.PK_GENERAL_LEDGER_SUPPLIER
            WHEN ISNULL(s.PK_GENERAL_LEDGER_SUPPLIER) = 1 AND ISNULL(stage.SUPPLIER_ID) = 0 THEN -1 
            ELSE -2
        END AS FK_GENERAL_LEDGER_SUPPLIER,
        CASE 
            WHEN ISNULL(acc.PK_GENERAL_LEDGER_ACCOUNT) = 0 THEN acc.PK_GENERAL_LEDGER_ACCOUNT
            WHEN ISNULL(acc.PK_GENERAL_LEDGER_ACCOUNT) = 1 AND ISNULL(stage.SEGMENT2_ACC) = 0 THEN -1 
            ELSE -2
        END AS FK_GENERAL_LEDGER_ACCOUNT_ACC,
        CASE
            WHEN ISNULL(boe.PK_GENERAL_LEDGER_ACCOUNT) = 0 THEN boe.PK_GENERAL_LEDGER_ACCOUNT
            WHEN ISNULL(boe.PK_GENERAL_LEDGER_ACCOUNT) = 1 AND ISNULL(stage.SEGMENT2_BOE) = 0 THEN -1  
            ELSE -2
        END AS FK_GENERAL_LEDGER_ACCOUNT_BOE,
        CASE
            WHEN ISNULL(cc.PK_GENERAL_LEDGER_COST_CENTRE) = 0 THEN cc.PK_GENERAL_LEDGER_COST_CENTRE
            WHEN ISNULL(cc.PK_GENERAL_LEDGER_COST_CENTRE) = 1 AND ISNULL(stage.SEGMENT3) = 0 THEN -1           
            ELSE -2
        END AS FK_GENERAL_LEDGER_COST_CENTRE,
        CASE
            WHEN ISNULL(com.PK_GENERAL_LEDGER_COMPANY) = 0 THEN com.PK_GENERAL_LEDGER_COMPANY
            WHEN ISNULL(com.PK_GENERAL_LEDGER_COMPANY) = 1 AND ISNULL(stage.SEGMENT1) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_COMPANY,
        CASE 
            WHEN ISNULL(org.PK_GENERAL_LEDGER_ORGANISATION) = 0 THEN org.PK_GENERAL_LEDGER_ORGANISATION
            WHEN ISNULL(org.PK_GENERAL_LEDGER_ORGANISATION) = 1 AND ISNULL(stage.SEGMENT5) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_ORGANISATION,
        CASE
            WHEN ISNULL(jel.PK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE) = 0 THEN jel.PK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE
            WHEN ISNULL(jel.PK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE) = 1 AND ISNULL(stage.JOURNAL_ENTRY_HEADER_ID) = 0 THEN -1 
            WHEN ISNULL(jel.PK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE) = 1 AND ISNULL(stage.JOURNAL_ENTRY_LINE_NUMBER) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE,        
        CASE
            WHEN ISNULL(jeh.PK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER) = 0 THEN jeh.PK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER
            WHEN ISNULL(jeh.PK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER) = 1 AND ISNULL(stage.JOURNAL_ENTRY_HEADER_ID) = 0 THEN -1
            ELSE -2
        END AS FK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER,
        stage.FK_DATE_CREATION_DATE,
        stage.FK_DATE_LAST_UPDATE_DATE,
        stage.FK_DATE_EFFECTIVE_DATE,
        stage.FK_DATE_APPROVAL_DATE,
        cast(stage.CREATION_DATETIME AS TIMESTAMP) AS CREATION_DATETIME,
        stage.LAST_UPDATE_DATETIME,
        stage.EFFECTIVE_DATETIME,
        stage.APPROVAL_DATETIME,
        CAST(stage.REVISED_ENTERED_CR AS DECIMAL(12,2)) AS ENTERED_CR,
        CAST(stage.REVISED_ENTERED_DR AS DECIMAL(12,2)) AS ENTERED_DR,
        CAST(stage.REVISED_ACCOUNTED_CR AS DECIMAL(12,2)) AS ACCOUNTED_CR,
        CAST(stage.REVISED_ACCOUNTED_DR AS DECIMAL(12,2)) AS ACCOUNTED_DR,
        CAST(COALESCE(stage.REVISED_ENTERED_DR,0) - COALESCE(stage.REVISED_ENTERED_CR,0) AS DECIMAL(12,2)) AS ENTERED_NET,
        CAST(COALESCE(stage.REVISED_ACCOUNTED_DR,0) - COALESCE(stage.REVISED_ACCOUNTED_CR,0) AS DECIMAL(12,2)) AS ACCOUNTED_NET
    FROM CTE_STAGE_GENERAL_LEDGER stage
    LEFT JOIN {env_var}_catalog.gold_int.dim_general_ledger_journal_entry_line jel
    ON stage.JOURNAL_ENTRY_HEADER_ID = jel.BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER
    AND stage.JOURNAL_ENTRY_LINE_NUMBER = jel.BK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE_NUMBER
    AND jel.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_journal_entry_header jeh
    ON stage.JOURNAL_ENTRY_HEADER_ID = jeh.BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER
    AND jeh.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_segment seg1
    ON stage.SEGMENT1 = seg1.BK_GENERAL_LEDGER_SEGMENT
    AND seg1.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_segment seg2
    ON stage.SEGMENT2 = seg2.BK_GENERAL_LEDGER_SEGMENT
    AND seg2.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_segment seg3
    ON stage.SEGMENT3 = seg3.BK_GENERAL_LEDGER_SEGMENT
    AND seg3.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_segment seg4
    ON stage.SEGMENT4 = seg4.BK_GENERAL_LEDGER_SEGMENT
    AND seg4.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_segment seg5
    ON stage.SEGMENT5 = seg5.BK_GENERAL_LEDGER_SEGMENT
    AND seg5.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_int.dim_general_ledger_user user_cb
    ON stage.CREATED_BY = user_cb.BK_GENERAL_LEDGER_USER
    AND user_cb.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_int.dim_general_ledger_user user_lub
    ON stage.LAST_UPDATED_BY = user_lub.BK_GENERAL_LEDGER_USER
    AND user_lub.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_int.dim_general_ledger_user user_approve
    ON stage.APPROVER_NAME = user_approve.USER_NAME
    AND user_approve.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_int.dim_general_ledger_account_type at
    ON stage.ACCOUNT_TYPE = at.BK_GENERAL_LEDGER_ACCOUNT_TYPE
    AND at.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_int.dim_general_ledger_supplier s
    ON stage.SUPPLIER_ID = s.BK_GENERAL_LEDGER_SUPPLIER
    AND s.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_account acc
    ON stage.SEGMENT2_ACC =acc.BK_GENERAL_LEDGER_ACCOUNT
    AND acc.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_account boe 
    ON stage.SEGMENT2_BOE = boe.BK_GENERAL_LEDGER_ACCOUNT
    AND boe.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_cost_centre cc
    ON stage.SEGMENT3 = cc.BK_GENERAL_LEDGER_COST_CENTRE
    AND cc.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_company com
    ON stage.SEGMENT1 = com.BK_GENERAL_LEDGER_COMPANY
    AND com.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.gold_con.dim_general_ledger_organisation org
    ON stage.SEGMENT5 = org.BK_GENERAL_LEDGER_ORGANISATION
    AND org.ROW_IS_CURRENT = 1
    """
    df = spark.sql(query)
    
    return df
