# Databricks notebook source
from datetime import datetime
from pyspark.sql import SparkSession

# Secret scopes for the environments
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------


def bronze_table_list(job_name):

    catalog = f"{env_var}_catalog"
    foreign_catalog = f"{env_var}_mdp_workflow_control_catalog"
    schemas = spark.sql("SHOW SCHEMAS").collect()
    schema_names = [row['databaseName'] for row in schemas]

    tables, schema_suffixes = [], ["int", "con", "sec", "pub"]
    for suffix in schema_suffixes:
        target_schema = f"bronze_{job_name.lower()}_{suffix}"
        if target_schema in schema_names:
            tables = tables + spark.sql(f"SHOW TABLES IN {target_schema}").collect()
        else:
            print(
                f"Warning: we can't find the schema `{target_schema}`." \
                " This may be normal; or you may have missed something?"
            )

    reconciliation_table = f"{catalog}.data_quality.source_to_bronze_reconciliation"
    max_load_id_result = spark.sql(f"""
        SELECT
            MAX(LOAD_ID) AS max_pk
        FROM
            {reconciliation_table}
    """).collect()

    max_load_id = max_load_id_result[0]['max_pk'] if max_load_id_result else None
    recon_load_id = max_load_id + 1 if max_load_id is not None else 1

    for table in tables:
        table_name = table.tableName.lower()
        schema_name = table.database
        full_table_name = f"{catalog}.{schema_name}.{table_name}"
        test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        load_type_result = spark.sql(f"""
            SELECT
                *
            FROM
                {foreign_catalog}.control.vwTaskRuntimeAudit
            WHERE
                TaskName = '{table_name}'
            ORDER BY
                LoadId DESC
            LIMIT 1
        """).collect()

        load_type = load_type_result[0]['LoadTypeName']
        load_id = load_type_result[0]['LoadId']

        load_check_result = spark.sql(f"""
            SELECT DISTINCT
                CASE
                    WHEN COUNT(MDP_LOAD_ID) = 1 THEN 'Passed: Only one Load processed'
                    WHEN COUNT(MDP_LOAD_ID) > 1 THEN 'Failed: Multiple Loads Processed'
                    ELSE 'Passed'
                END AS load_result
            FROM (
                SELECT
                    MDP_LOAD_ID,
                    COUNT(MDP_LOAD_ID) AS load_count
                FROM
                    {catalog}.{schema_name}.{table_name}
                GROUP BY
                    MDP_LOAD_ID
            ) subquery
        """).collect()[0]['load_result']

        source_recon_result = spark.sql(f"""
            SELECT
                SUM(RowsInsertedCount) AS RowsInsertedCount
            FROM
                {foreign_catalog}.control.vwTaskRuntimeAudit
            WHERE
                JobName = '{job_name}'
                AND RowsInsertedCount > 0
                AND LoadId = '{load_id}'
                AND TaskName = '{table_name}'
            GROUP BY
                LoadId
            --ORDER BY
            --    PK_TaskRuntimeAudit DESC LIMIT 1
        """).collect()

        # No rows in source_recon_result === 0 rows loaded (because no files loaded).
        source_rows_loaded = 0 if len(source_recon_result) == 0 else source_recon_result[0]['RowsInsertedCount']

        bronze_count = spark.sql(f"""
            SELECT
                COUNT(*) as count
            FROM
                {catalog}.{schema_name}.{table_name}
            WHERE
                MDP_LOAD_ID = '{load_id}'
        """).collect()[0]['count']

        RecordCountDifference = bronze_count - source_rows_loaded
        ResultOutcome = "Pass" if RecordCountDifference == 0 else "Fail"
        test_outcome_message = f"Record counts {'' if RecordCountDifference == 0 else 'do not '}match for table {table_name}."

        max_pk_result = spark.sql(f"""
            SELECT
                MAX(PK_SOURCE_TO_BRONZE_RECONCILIATION) AS max_pk
            FROM
                {reconciliation_table}
        """).collect()

        max_pk = max_pk_result[0]['max_pk'] if max_pk_result else None
        pk_source_to_bronze_reconciliation = max_pk + 1 if max_pk is not None else 1

        spark.sql(f"""
            INSERT INTO {env_var}_catalog.data_quality.source_to_bronze_reconciliation (
                PK_SOURCE_TO_BRONZE_RECONCILIATION,
                TEST_TYPE,
                CATALOG_NAME,
                SOURCE_LATEST_LOAD_COUNT,
                BRONZE_SCHEMA_NAME,
                BRONZE_TABLE_NAME,
                BRONZE_TABLE_LATEST_LOAD_COUNT,
                RECORD_COUNT_DIFFERENCE,
                COUNT_TEST_OUTCOME,
                COUNT_OUTCOME_MESSAGE,
                LOAD_TYPE,
                LOAD_CHECK,
                LOAD_ID,
                TEST_CREATED_DATETIME
            ) VALUES (
                '{pk_source_to_bronze_reconciliation}',
                'Source to bronze Reconciliation',
                '{catalog}',
                '{source_rows_loaded}',
                '{schema_name}',
                '{table_name}',
                '{bronze_count}',
                '{RecordCountDifference}',
                '{ResultOutcome}',
                '{test_outcome_message}',
                '{load_type}',
                '{load_check_result}',
                '{recon_load_id}',
                '{test_created_datetime}'
            )
        """)

    output_query = spark.sql(f"""
        SELECT
            *
        FROM
            {catalog}.data_quality.source_to_bronze_reconciliation
        WHERE
            LOAD_ID = (
                SELECT
                    MAX(LOAD_ID)
                FROM
                    {catalog}.data_quality.source_to_bronze_reconciliation
            )
        ORDER BY
            TEST_CREATED_DATETIME DESC
    """)

    display(output_query)

# COMMAND ----------

job_name = ""

bronze_table_list(job_name)
