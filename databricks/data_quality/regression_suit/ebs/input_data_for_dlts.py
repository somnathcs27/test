# Databricks notebook source
# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

# Create the SQL query string
sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_journal_entry_lines_test_data AS

SELECT
    1 AS JE_HEADER_ID,
    1 AS JE_LINE_NUM,
    1 AS CODE_COMBINATION_ID,
    '2023-10-01T00:00:00Z' AS EFFECTIVE_DATE,
    101 AS CREATED_BY,
    102 AS LAST_UPDATED_BY,
    '2023-10-01T12:00:00Z' AS LAST_UPDATE_DATE,
    '1234 ' AS ATTRIBUTE2,
    '2023-10-01' AS CREATION_DATE,
    '2023-10-01T12:00:00Z' AS CREATION_DATE_TIME,
    'Sample line description' AS LINE_DESCRIPTION,
    500.00 AS ACCOUNTED_DR,
    0.00 AS ACCOUNTED_CR,
    500.00 AS ENTERED_DR,
    0.00 AS ENTERED_CR,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT

UNION ALL

SELECT
    2 AS JE_HEADER_ID,
    2 AS JE_LINE_NUM,
    1 AS CODE_COMBINATION_ID,
    '2023-10-01T00:00:00Z' AS EFFECTIVE_DATE,
    101 AS CREATED_BY,
    102 AS LAST_UPDATED_BY,
    '2023-10-01T12:00:00Z' AS LAST_UPDATE_DATE,
    'ESBUSER' AS ATTRIBUTE2,
    '2023-10-01' AS CREATION_DATE,
    '2023-10-01T12:00:00Z' AS CREATION_DATE_TIME,
    'Sample line description' AS LINE_DESCRIPTION,
    500.00 AS ACCOUNTED_DR,
    0.00 AS ACCOUNTED_CR,
    500.00 AS ENTERED_DR,
    0.00 AS ENTERED_CR,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT    

"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_code_combinations_test_data AS
SELECT
    1 AS CODE_COMBINATION_ID,
    'Segment1Value' AS SEGMENT1,
    'Segment2Value' AS SEGMENT2,
    'Segment3Value' AS SEGMENT3,
    'Segment4Value' AS SEGMENT4,
    'Segment5Value' AS SEGMENT5,
    'Asset' AS ACCOUNT_TYPE,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_journal_entry_headers_test_data AS
SELECT
    1 AS JE_HEADER_ID,
    1 AS JE_BATCH_ID,
    'Posted' AS STATUS,
    1001 AS DOC_SEQUENCE_VALUE,
    'Initial journal entry' AS DESCRIPTION,
    'Payables' AS JE_SOURCE,
    'Y' AS ACTUAL_FLAG,
    'USD' AS CURRENCY_CODE,
    1 AS LOAD_ID,
    '2023-10-01T00:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T00:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT;
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_import_references_test_data AS
SELECT
    1 AS JE_HEADER_ID,
    1 AS JE_LINE_NUM,
    1 AS JE_BATCH_ID,
    1 AS GL_SL_LINK_ID,
    'GL_SL_LINK_TABLE_NAME' AS GL_SL_LINK_TABLE,
    1 AS LOAD_ID,
    '2023-10-01T00:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T00:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_xla_ae_lines_test_data AS
SELECT
 1 AS GL_SL_LINK_ID,
    'GL_SL_LINK_TABLE_NAME' AS GL_SL_LINK_TABLE,
    1 AS PARTY_ID,
    1 AS AE_HEADER_ID,
    1 AS AE_LINE_NUM,
    100.00 AS ENTERED_DR,
    0.00 AS ENTERED_CR,
    100.00 AS ACCOUNTED_DR,
    0.00 AS ACCOUNTED_CR,
    1 AS LOAD_ID,
    '2023-10-01T00:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T00:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_suppliers_test_data AS
SELECT
    1 AS SUPPLIER_ID,
    'Supplier A' AS SUPPLIER_NAME,
    'SUP123' AS SUPPLIER_NUMBER,
    NULL AS END_DATE_ACTIVE,
    1 AS LOAD_ID,
    '2023-10-01T00:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T00:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_wf_action_history_test_data AS
SELECT
    1 AS OBJECT_ID,
    'John Doe' AS APPROVER_NAME,
    1 AS ACTION_RANK,
    'APPROVE' AS ACTION_CODE,
    '2023-10-01' AS ACTION_DATE,
    '2023-10-01T12:00:00Z' AS ACTION_DATETIME,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_test_data AS
SELECT
    1 AS FLEX_VALUE_SET_ID,
    1 AS FLEX_VALUE_ID,
    'FLEX001' AS FLEX_VALUE,
    'Level1' AS HIERARCHY_LEVEL,
    'Y' AS ENABLED_FLAG,
    'N' AS SUMMARY_FLAG,
    '2023-10-01T00:00:00Z' AS ACTIVE_START_DATE,
    '2023-10-01T00:00:00Z' AS ACTIVE_END_DATE,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T00:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_fnd_flex_values_tl_test_data AS
SELECT
    1 AS FLEX_VALUE_ID,
    'BOE' AS DESCRIPTION,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T00:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_seg_val_norm_hierarchy_test_data AS
SELECT
    1 AS FLEX_VALUE_SET_ID,
    'ParentValue' AS PARENT_FLEX_VALUE,
    'ChildValue' AS CHILD_FLEX_VALUE,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_quantum_control_codes_test_data AS
SELECT
    'ChildValue' AS GL_ACCOUNT,
    'CTRL001' AS CONTROL_CODE,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_user_test_data AS
SELECT
    1 AS USER_ID,
    'JohnDoe' AS USER_NAME,
    '2023-10-01T00:00:00Z' AS START_DATE,
    '2023-10-01T12:00:00Z' AS END_DATE,
    'Sample user description' AS DESCRIPTION,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE OR REPLACE TABLE {env_var}_catalog.data_quality.general_ledger_account_type_description_test_data AS
SELECT
    'ACC001' AS ACCOUNT_CODE,
    'Sample account description' AS ACCOUNT_DESCRIPTION,
    1 AS LOAD_ID,
    '2023-10-01T12:00:00Z' AS LOAD_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_START_DATETIME,
    '2023-10-01T12:00:00Z' AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
"""

# Execute the SQL query
spark.sql(sql_query)
