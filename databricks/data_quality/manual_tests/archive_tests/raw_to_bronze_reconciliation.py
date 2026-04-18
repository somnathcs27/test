# Databricks notebook source
from datetime import datetime
from pyspark.sql import SparkSession

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

# Function to get all tables in a schema
def get_tables(schema):
    query = f"SHOW TABLES IN {schema}"
    return [table['tableName'] for table in spark.sql(query).collect()]


# COMMAND ----------

def compare_table_counts(spark, schema_pairs):

    catalog = env_var + '_catalog'

    for raw_schema, bronze_schema in schema_pairs:

        # Get all tables in the source and bronze schemas
        source_tables = get_tables(f"{catalog}.{raw_schema}")
        bronze_tables = get_tables(f"{catalog}.{bronze_schema}")
        reconciliation_table = f"{catalog}.data_quality.raw_to_bronze_reconciliation"
        max_load_id_result = spark.sql(f"SELECT MAX(LOAD_ID) AS max_pk FROM {reconciliation_table}").collect()[0]['max_pk']
        load_id = max_load_id_result + 1 if max_load_id_result is not None else 1

        # Loop through all tables and perform the count test
        for table in source_tables:
            #try:
            full_source_table = f"{catalog}.{raw_schema}.{table}"
            raw_schema = full_source_table.split('.')[1]
            full_bronze_table = f"{catalog}.{bronze_schema}.{table}"
            bronze_schema = full_bronze_table.split('.')[1]
            max_pk_result = spark.sql(f"SELECT MAX(PK_RAW_TO_BRONZE_RECONCILIATION) AS max_pk FROM {reconciliation_table}").collect()[0]['max_pk']
            pk_raw_to_bronze_reconciliation = max_pk_result + 1 if max_pk_result is not None else 1

            # Query the source table to get the count of records
            source_count = spark.sql(f"SELECT COUNT(*) AS count FROM {full_source_table}").collect()[0]['count']

            # Query the bronze table to get the count of records
            bronze_count = spark.sql(f"SELECT COUNT(*) AS count FROM {full_bronze_table}").collect()[0]['count']

            # Compare the counts
            test_outcome_message = f"Record counts match for table {table}." if source_count == bronze_count else f"Record counts do not match for table {table}."

            # Insert the reconciliation record into dev_catalog.data_quality.raw_to_bronze_reconciliation table
            raw_table_name = table
            bronze_table_name = table
            test_type = 'Raw to Bronze Reconciliation'
            count_difference = source_count - bronze_count
            test_outcome = 'FAIL' if count_difference != 0 else 'PASS'
            test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            # Query the source table to get the distinct records of load_id and load_datetime
            source_distinct_query = f"""
                SELECT DISTINCT CAST(MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID, MDP_LOAD_DATETIME
                FROM {full_source_table}
            """

            # Query the bronze table to get the distinct records of load_id and load_datetime
            bronze_distinct_query = f"""
                SELECT DISTINCT CAST(MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID, MDP_LOAD_DATETIME
                FROM {full_bronze_table}
            """

            # Perform the EXCEPT operation to find mismatched records
            mismatch_query = f"""
                (SELECT MDP_LOAD_ID, MDP_LOAD_DATETIME FROM ({source_distinct_query}))
                EXCEPT
                (SELECT MDP_LOAD_ID, MDP_LOAD_DATETIME FROM ({bronze_distinct_query}))
            """

            mismatches = spark.sql(mismatch_query).collect()

            job_test = 'PASS' if not mismatches else 'FAIL'
            job_test_message = 'Load ID and Load Datetime mismatch found for table {table}.' if mismatches else 'Load ID and Load Datetime match found for table {table}.'

            insert_query = f"""
                INSERT INTO {reconciliation_table}
                (PK_RAW_TO_BRONZE_RECONCILIATION, TEST_TYPE, CATALOG_NAME, RAW_SCHEMA_NAME, RAW_TABLE_NAME, RAW_TABLE_COUNT, BRONZE_SCHEMA_NAME, BRONZE_TABLE_NAME, BRONZE_TABLE_COUNT, RECORD_COUNT_DIFFERENCE, COUNT_TEST_OUTCOME, COUNT_OUTCOME_MESSAGE, JOB_METADATA_TEST_OUTCOME, JOB_METADATA_OUTCOME_MESSAGE, LOAD_ID, TEST_CREATED_DATETIME)
                VALUES
                ('{pk_raw_to_bronze_reconciliation}','{test_type}','{catalog}','{raw_schema}','{raw_table_name}','{source_count}','{bronze_schema}','{bronze_table_name}','{bronze_count}','{count_difference}','{test_outcome}','{test_outcome_message}','{job_test}','{job_test_message}','{load_id}','{test_created_datetime}')
            """

            spark.sql(insert_query)
                
            '''except Exception as e:
                error_message = str(e).replace("`", "").replace("'", "''")[:200]
                exception_table = env_var + '_catalog.data_quality.raw_to_bronze_exception'
                exception_max_pk_query = f"SELECT MAX(PK_RAW_TO_BRONZE_EXCEPTION) AS max_pk FROM {exception_table}"
                exception_max_pk_result = spark.sql(exception_max_pk_query).collect()[0]['max_pk']
                pk_exception = exception_max_pk_result + 1 if exception_max_pk_result is not None else 1
                test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                max_load_id_result = spark.sql(f"SELECT MAX(LOAD_ID) AS max_pk FROM {exception_table}").collect()[0]['max_pk']
                load_id = max_load_id_result + 1 if max_load_id_result is not None else 1
                insert_exception_query = f"""
                    INSERT INTO {exception_table}
                    (PK_RAW_TO_BRONZE_EXCEPTION, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, ERROR_MESSAGE, LOAD_ID, EXCEPTION_CREATED_DATETIME)
                    VALUES
                    ('{pk_exception}', '{catalog}', '{raw_schema}', '{table}', '{error_message}', '{load_id}', '{test_created_datetime}')
                """
                spark.sql(insert_exception_query)'''

# COMMAND ----------

schema_pairs = [('', '')]
compare_table_counts(spark, schema_pairs)
