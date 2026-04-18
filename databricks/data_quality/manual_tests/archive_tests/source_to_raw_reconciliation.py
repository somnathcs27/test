# Databricks notebook source
from datetime import datetime
from pyspark.sql import SparkSession

# Secret scopes for the environments
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

def raw_table_list(int_table_schema, con_table_schema):
    if int_table_schema:
        int_tables = spark.sql(f"SHOW TABLES IN {int_table_schema}").collect()
    else:
        int_tables = []

    if con_table_schema:
        con_tables = spark.sql(f"SHOW TABLES IN {con_table_schema}").collect()
    else:
        con_tables = []
    
    tables = int_tables + con_tables
    catalog = f"{env_var}_catalog"
    reconciliation_table = f"{catalog}.data_quality.source_to_raw_reconciliation"
    max_load_id_result = spark.sql(f"SELECT MAX(LOAD_ID) AS max_pk FROM {reconciliation_table}").collect()
    max_load_id = max_load_id_result[0]['max_pk'] if max_load_id_result else None
    load_id = max_load_id + 1 if max_load_id is not None else 1

    for table in tables:
        table_name = table.tableName
        schema_name = table.database
        full_table_name = f"{catalog}.{schema_name}.{table_name}"
        test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        
        source_recon_result = spark.sql(f"""
            SELECT RowsInsertedCount 
            FROM {catalog}.etl.ext_etl_task_reconciliation 
            WHERE TaskName = UPPER('{table_name}') 
            ORDER BY LoadId DESC 
            LIMIT 1
        """).collect()
        
        if not source_recon_result:
            continue
        
        source_recon_result = source_recon_result[0]['RowsInsertedCount']
        
        SourceLoadId_result = spark.sql(f"""
            SELECT LoadId 
            FROM {catalog}.etl.ext_etl_task_reconciliation 
            WHERE TaskName = UPPER('{table_name}') 
            ORDER BY LoadId DESC 
            LIMIT 1
        """).collect()
        
        if not SourceLoadId_result:
            continue
        
        SourceLoadId = SourceLoadId_result[0]['LoadId']
        
        raw_count_result = spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM {catalog}.{schema_name}.{table_name} 
            WHERE MDP_LOAD_ID = '{SourceLoadId}'
        """).collect()
        
        if not raw_count_result:
            continue
        
        raw_count = raw_count_result[0]['count']
        
        RecordCountDifference = raw_count - source_recon_result
        ResultOutcome = "Pass" if RecordCountDifference == 0 else "Fail"
        test_outcome_message = f"Record counts match for table {table_name}." if source_recon_result == raw_count else f"Record counts do not match for table {table_name}."
        
        max_pk_result = spark.sql(f"""
            SELECT MAX(PK_SOURCE_TO_RAW_RECONCILIATION) AS max_pk 
            FROM {reconciliation_table}
        """).collect()
        
        max_pk = max_pk_result[0]['max_pk'] if max_pk_result else None
        pk_source_to_raw_reconciliation = max_pk + 1 if max_pk is not None else 1
        
        load_type_result = spark.sql(f"""
            SELECT LoadTypeName 
            FROM {catalog}.etl.ext_etl_task_details 
            WHERE TaskName = UPPER('{table_name}')
        """).collect()
        
        if not load_type_result:
            continue
        
        load_type = load_type_result[0]['LoadTypeName']
        
        if load_type == "Full Load":
            Full_load_check_result = spark.sql(f"""
                SELECT DISTINCT 
                    CASE 
                        WHEN COUNT(MDP_LOAD_ID) = 1 THEN 'Passed: Only one Load Present' 
                        WHEN COUNT(MDP_LOAD_ID) > 1 THEN 'Failed: Multiple Loads Processed' 
                        ELSE 'Passed' 
                    END AS full_load_result 
                FROM (
                    SELECT MDP_LOAD_ID, COUNT(MDP_LOAD_ID) AS load_count
                    FROM {catalog}.{schema_name}.{table_name}
                    GROUP BY MDP_LOAD_ID
                ) subquery
            """).collect()
             
        elif load_type == "Incremental":
            Full_load_check_result = spark.sql(f"""
                SELECT DISTINCT 
                    CASE 
                        WHEN COUNT(MDP_LOAD_ID) = 1 THEN 'Failed: Only One Load processed' 
                        WHEN COUNT(MDP_LOAD_ID) > 1 THEN 'Passed: Multiple Loads Processed'
                        ELSE 'Passed' 
                    END AS full_load_result 
                FROM (
                    SELECT MDP_LOAD_ID
                    FROM {catalog}.{schema_name}.{table_name}
                    GROUP BY MDP_LOAD_ID
                ) subquery
            """).collect()
        
        if not Full_load_check_result:
            continue
        
        Full_load_check = Full_load_check_result[0]['full_load_result']

        spark.sql(f"""
            INSERT INTO {env_var}_catalog.data_quality.source_to_raw_reconciliation
            (PK_SOURCE_TO_RAW_RECONCILIATION, TEST_TYPE, CATALOG_NAME, SOURCE_LATEST_LOAD_COUNT, RAW_SCHEMA_NAME, RAW_TABLE_NAME, RAW_TABLE_LATEST_LOAD_COUNT, RECORD_COUNT_DIFFERENCE, COUNT_TEST_OUTCOME, COUNT_OUTCOME_MESSAGE, LOAD_TYPE, LOAD_CHECK, LOAD_ID, TEST_CREATED_DATETIME)
            VALUES ('{pk_source_to_raw_reconciliation}', 'Source to Raw Reconciliation', '{catalog}', '{source_recon_result}', '{schema_name}', '{table_name}', '{raw_count}', '{RecordCountDifference}', '{ResultOutcome}', '{test_outcome_message}', '{load_type}', '{Full_load_check}', '{SourceLoadId}', '{test_created_datetime}')
        """)

# COMMAND ----------

int_table_schema = ""
con_table_schema = ""

raw_table_list(int_table_schema, con_table_schema)
