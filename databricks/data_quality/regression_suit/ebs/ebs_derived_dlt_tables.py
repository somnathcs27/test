# Databricks notebook source
import dlt

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "fact_general_ledger_derived_data_test",
    comment = "the general ledger fact table"
)
def gold_fact_general_ledger_derived_data_test():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
        jel.JE_HEADER_ID AS JOURNAL_ENTRY_HEADER_ID,
        jel.JE_LINE_NUM AS JOURNAL_ENTRY_LINE_NUMBER,
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
    FROM {env_var}_catalog.data_quality.general_ledger_journal_entry_lines_test_data AS jel
    LEFT JOIN {env_var}_catalog.data_quality.general_ledger_code_combinations_test_data cc
    ON jel.CODE_COMBINATION_ID = cc.CODE_COMBINATION_ID
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_journal_entry_headers_test_data jeh
    ON jel.JE_HEADER_ID = jeh.JE_HEADER_ID
    LEFT JOIN {env_var}_catalog.data_quality.general_ledger_import_references_test_data ref
    on jel.JE_HEADER_ID = ref.JE_HEADER_ID
    and jel.JE_LINE_NUM = ref.JE_LINE_NUM
    LEFT JOIN {env_var}_catalog.data_quality.general_ledger_xla_ae_lines_test_data xla
    on xla.GL_SL_LINK_ID = ref.GL_SL_LINK_ID
    and xla.GL_SL_LINK_TABLE = ref.GL_SL_LINK_TABLE
    LEFT JOIN {env_var}_catalog.data_quality.general_ledger_suppliers_test_data s
    on xla.PARTY_ID = s.SUPPLIER_ID
    LEFT JOIN {env_var}_catalog.data_quality.general_ledger_wf_action_history_test_data wf_ah
    ON jeh.JE_BATCH_ID = wf_ah.OBJECT_ID
    AND wf_ah.ACTION_RANK = 1 AND wf_ah.ACTION_CODE = 'APPROVE';
    """
    df = spark.sql(query)

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_general_ledger_journal_entry_line_derived_data_test",
    comment = "the dim general ledger entry line table"
)
def gold_dim_general_ledger_journal_entry_line_derived_data_test():
    # Execute SQL query to select and transform data
    query = f"""
     SELECT
        JE_HEADER_ID AS BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER,
        JE_LINE_NUM AS BK_GENERAL_LEDGER_JOURNAL_ENTRY_LINE_NUMBER,
        COALESCE(LINE_DESCRIPTION,'Unknown') AS LINE_DESCRIPTION,
        CASE 
            WHEN LENGTH(REPLACE(ATTRIBUTE2,' ','')) = 4 THEN ATTRIBUTE2 
            ELSE COALESCE(ATTRIBUTE2,'Unknown')
        END AS CASHIER,
        LOAD_ID,
        LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {env_var}_catalog.data_quality.general_ledger_journal_entry_lines_test_data;
    """
    df = spark.sql(query)

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_general_ledger_segment_derived_data_test",
    comment = "the general ledger segment table"
)
def gold_dim_general_ledger_segment_derived_data_test():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
        ffv.FLEX_VALUE AS BK_GENERAL_LEDGER_SEGMENT,
        CASE 
            WHEN ffv.FLEX_VALUE_SET_ID = 1 THEN 'Segment 1'
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
        ffv.LOAD_ID,
        ffv.LOAD_DATETIME,
        ffv.ROW_START_DATETIME,
        --ffv.ROW_END_DATETIME,
        ffv.ROW_IS_CURRENT
    FROM {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data AS ffv
    INNER JOIN {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data AS tl
    ON ffv.FLEX_VALUE_ID = tl.FLEX_VALUE_ID;
    """
    df = spark.sql(query)

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_general_ledger_account_derived_data_test",
    comment = "the general ledger account table"
)
def gold_dim_general_ledger_account_derived_data_test():
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
        a.LOAD_ID,
        a.LOAD_DATETIME,
        a.ROW_START_DATETIME,
        a.ROW_END_DATETIME,
        a.ROW_IS_CURRENT
    FROM  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data AS a
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_seg_val_norm_hierarchy_test_data AS b
    ON a.FLEX_VALUE = b.PARENT_FLEX_VALUE 
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_seg_val_norm_hierarchy_test_data c 
    ON b.CHILD_FLEX_VALUE = c.PARENT_FLEX_VALUE
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_seg_val_norm_hierarchy_test_data d 
    ON c.CHILD_FLEX_VALUE = d.PARENT_FLEX_VALUE
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data e 
    ON a.FLEX_VALUE_ID = e.FLEX_VALUE_ID
     LEFT JOIN   (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION
                FROM  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data a
                INNER JOIN  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                ) f ON b.CHILD_FLEX_VALUE = f.FLEX_VALUE
     LEFT JOIN   (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION
                FROM  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data a
                INNER JOIN  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                ) g ON c.CHILD_FLEX_VALUE = g.FLEX_VALUE
     LEFT JOIN   (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION
                FROM  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data a
                INNER JOIN  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                ) h ON d.CHILD_FLEX_VALUE = h.FLEX_VALUE
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_quantum_control_codes_test_data qcc
    ON d.CHILD_FLEX_VALUE = qcc.gl_account;

    """
    df = spark.sql(query)

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_general_ledger_cost_centre_derived_data_test",
    comment = "the general ledger cost centre table"
)
def gold_dim_general_ledger_cost_centre_derived_data_test():
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
        a.LOAD_ID,
        a.LOAD_DATETIME,
        a.ROW_START_DATETIME,
        a.ROW_END_DATETIME,
        a.ROW_IS_CURRENT
    FROM  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data AS a
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_seg_val_norm_hierarchy_test_data AS b
    ON a.FLEX_VALUE = b.PARENT_FLEX_VALUE 
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_seg_val_norm_hierarchy_test_data c 
    ON b.CHILD_FLEX_VALUE = c.PARENT_FLEX_VALUE
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_seg_val_norm_hierarchy_test_data d 
    ON c.CHILD_FLEX_VALUE = d.PARENT_FLEX_VALUE
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data e 
    ON a.FLEX_VALUE_ID = e.FLEX_VALUE_ID
     LEFT JOIN   (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION
                FROM  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data a
                INNER JOIN  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                ) f ON b.CHILD_FLEX_VALUE = f.FLEX_VALUE
     LEFT JOIN   (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION
                FROM  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data a
                INNER JOIN  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                ) g ON c.CHILD_FLEX_VALUE = g.FLEX_VALUE
     LEFT JOIN   (
                SELECT      
                    a.FLEX_VALUE,
                    b.DESCRIPTION
                FROM  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data a
                INNER JOIN  {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data b 
                ON a.FLEX_VALUE_ID = b.FLEX_VALUE_ID
                ) h ON d.CHILD_FLEX_VALUE = h.FLEX_VALUE
    LEFT JOIN  {env_var}_catalog.data_quality.general_ledger_quantum_control_codes_test_data qcc
    ON d.CHILD_FLEX_VALUE = qcc.gl_account;
 
    """
    df = spark.sql(query)

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_general_ledger_company_derived_data_test",
    comment = "the general ledger company table"
)
def gold_dim_general_ledger_company_derived_data_test():
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
        ffv.LOAD_ID,
        ffv.LOAD_DATETIME,
        ffv.ROW_START_DATETIME,
        ffv.ROW_END_DATETIME,
        ffv.ROW_IS_CURRENT
    FROM {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data ffv
    INNER JOIN {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data tl
    ON ffv.FLEX_VALUE_ID = tl.FLEX_VALUE_ID;
    """
    df = spark.sql(query)

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_general_ledger_organisation_derived_data_test",
    comment = "the general ledger organisation table"
)
def gold_dim_general_ledger_organisation_derived_data_test():
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
        ffv.LOAD_ID,
        ffv.LOAD_DATETIME,
        ffv.ROW_START_DATETIME,
        ffv.ROW_END_DATETIME,
        ffv.ROW_IS_CURRENT
    FROM {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data ffv
    INNER JOIN {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data tl
    ON ffv.FLEX_VALUE_ID = tl.FLEX_VALUE_ID;
    """
    df = spark.sql(query)

    return df
