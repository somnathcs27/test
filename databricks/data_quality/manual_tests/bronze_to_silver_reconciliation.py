# Databricks notebook source
from datetime import datetime
from pyspark.sql import SparkSession

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

def silver_record_count(tables_to_test): 

    catalog = env_var + '_catalog'
    reconciliation_table = f"{catalog}.data_quality.bronze_to_silver_reconciliation"
    max_load_id_result = spark.sql(f"SELECT MAX(LOAD_ID) AS max_pk FROM {reconciliation_table}").collect()[0]['max_pk']
    load_id = max_load_id_result + 1 if max_load_id_result is not None else 1
    
    for bronze_table_name, silver_table_name in tables_to_test:
        try:
            full_bronze_table_name = f'{catalog}.{bronze_table_name}'
            full_silver_table_name = f'{catalog}.{silver_table_name}'
            bronze_schema = full_bronze_table_name.split('.')[1]
            silver_schema = full_silver_table_name.split('.')[1]
            bronze_table_name = bronze_table_name.split('.')[1]           
            silver_table_name = silver_table_name.split('.')[1]
            raw_schema = str.replace(bronze_schema, 'bronze', 'raw')

            bronze_count_result = spark.sql(f"SELECT COUNT(*) AS count FROM {full_bronze_table_name}").collect()[0]['count']
            silver_count_result =  spark.sql(f"SELECT COUNT(*) AS count FROM {full_silver_table_name}").collect()[0]['count']
            count_test_result = 'PASS' if bronze_count_result == silver_count_result else 'FAIL'
            count_difference = abs(bronze_count_result - silver_count_result)
            test_outcome_message = f"Record counts match for table {silver_table_name}." if bronze_count_result == silver_count_result else f"Record counts do not match for table {silver_table_name}."
            load_type = spark.sql(f"SELECT LoadTypeName FROM {env_var}_mdp_workflow_control_catalog.control.vwtaskdetail where SparkSchema = '{raw_schema}' and TaskName = '{bronze_table_name}'")

            max_pk_result = spark.sql(f"SELECT MAX(PK_BRONZE_TO_SILVER_RECONCILIATION) AS max_pk FROM {reconciliation_table}").collect()[0]['max_pk']
            pk_bronze_to_silver_reconciliation = max_pk_result + 1 if max_pk_result is not None else 1

            test_type = 'Bronze to Silver Reconciliation'
            test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            house_keeping_result = spark.sql(f"""
            SELECT
                CASE
                    WHEN '{load_type}' = 'Incremental' THEN
                        CASE
                            WHEN COUNT(*) = 5 THEN 'PASS'
                            WHEN COUNT(*) = 4 THEN 'One Housekeeping Column Missing'
                            WHEN COUNT(*) = 3 THEN 'Two Housekeeping Column Missing'
                            WHEN COUNT(*) = 2 THEN 'Three Housekeeping Column Missing'
                            WHEN COUNT(*) = 1 THEN 'Four Housekeeping Column Missing'
                            ELSE 'All HouseKeeping Columns Missing'
                        END
                    ELSE
                        CASE
                            WHEN COUNT(*) = 3 THEN 'PASS'
                            WHEN COUNT(*) = 2 THEN 'One Housekeeping Column Missing'
                            WHEN COUNT(*) = 1 THEN 'Two Housekeeping Column Missing'
                            ELSE 'All HouseKeeping Columns Missing'
                        END
                END AS test_result
            FROM {catalog}.information_schema.columns 
            WHERE table_name = '{silver_table_name}'
            AND table_schema = '{silver_schema}'
            AND column_name IN ('MDP_LOAD_ID', 'MDP_LOAD_DATETIME', 'ROW_START_DATETIME', 'ROW_END_DATETIME', 'ROW_IS_CURRENT')
            """).collect()[0]['test_result']

            housekeeping_pass_fail = 'PASS' if house_keeping_result == 'PASS' else 'FAIL'

            housekeeping_messge = 'All housekeeping columns are present' if house_keeping_result == 'PASS' else f"{house_keeping_result}"

            column_naming_result = spark.sql(f"""
                SELECT 
                    CASE 
                        WHEN COUNT(*) = MAX(upper_count) THEN 'PASS' 
                        ELSE 'FAIL' 
                    END AS column_naming_test
                FROM {catalog}.information_schema.columns a
                LEFT JOIN (
                    SELECT 
                        COUNT(*) AS upper_count, 
                        ANY_VALUE(table_schema) AS table_schema, 
                        ANY_VALUE(table_name) AS table_name, 
                        ANY_VALUE(column_name) AS column_name 
                    FROM {catalog}.information_schema.columns 
                    WHERE table_name = '{silver_table_name}'
                    AND column_name = UPPER(column_name)
                ) b
                ON a.table_schema = b.table_schema
                AND a.table_name = b.table_name
                AND a.column_name = b.column_name
                WHERE a.table_name = '{silver_table_name}'
            """).collect()[0]['column_naming_test']

            column_naming_message = 'All columns follow the expected naming convention of upper case characters' if column_naming_result == 'PASS' else 'Lower case characters are present in the column names'

            # Data Type Check

            double_type_check = spark.sql(f"SELECT COUNT(*) as double_count FROM {catalog}.information_schema.columns WHERE table_name = '{silver_table_name}' AND data_type IN('double')").collect()[0]['double_count']
            double_check_result = 'PASS' if double_type_check == 0 else f"FAIL: {double_type_check} columns have a double data type in {silver_table_name}"

            spark.sql(f"""
            INSERT INTO {reconciliation_table}
            (PK_BRONZE_TO_SILVER_RECONCILIATION, TEST_TYPE, CATALOG_NAME, BRONZE_SCHEMA_NAME, BRONZE_TABLE_NAME, BRONZE_TABLE_COUNT, SILVER_SCHEMA_NAME, SILVER_TABLE_NAME, SILVER_TABLE_COUNT, RECORD_COUNT_DIFFERENCE, COUNT_TEST_OUTCOME, COUNT_OUTCOME_MESSAGE, COLUMN_NAMING_TEST_OUTCOME, COLUMN_NAMING_OUTCOME_MESSAGE, HOUSEKEEPING_TEST_OUTCOME, HOUSEKEEPING_TEST_OUTCOME_MESSAGE, DATA_TYPE_TEST_OUTCOME, LOAD_ID, TEST_CREATED_DATETIME)
            VALUES
            ('{pk_bronze_to_silver_reconciliation}','{test_type}','{catalog}','{bronze_schema}','{bronze_table_name}','{bronze_count_result}','{silver_schema}','{silver_table_name}','{silver_count_result}','{count_difference}','{count_test_result}','{test_outcome_message}','{column_naming_result}','{column_naming_message}','{housekeeping_pass_fail}','{housekeeping_messge}','{double_check_result}','{load_id}','{test_created_datetime}')
            """)

        except Exception as e:
            error_message = str(e).replace("`", "").replace("'", "''")[:200]
            exception_table = env_var + '_catalog.data_quality.bronze_to_silver_exception'
            exception_max_pk_result = spark.sql(f"SELECT MAX(PK_BRONZE_TO_SILVER_EXCEPTION) AS max_pk FROM {exception_table}").collect()[0]['max_pk']
            pk_exception = exception_max_pk_result + 1 if exception_max_pk_result is not None else 1
            test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            max_load_id_result = spark.sql(f"SELECT MAX(LOAD_ID) AS max_pk FROM {exception_table}").collect()[0]['max_pk']
            load_id = max_load_id_result + 1 if max_load_id_result is not None else 1
            insert_exception_query = f"""
                INSERT INTO {exception_table}
                (PK_BRONZE_TO_SILVER_EXCEPTION, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, ERROR_MESSAGE, LOAD_ID, EXCEPTION_CREATED_DATETIME)
                VALUES
                ('{pk_exception}', '{catalog}', '{silver_schema}', '{table}', '{error_message}', '{load_id}', '{test_created_datetime}')
            """
            spark.sql(insert_exception_query)
        
    output_query = spark.sql(
    f"""SELECT * FROM {catalog}.data_quality. bronze_to_silver_reconciliation WHERE LOAD_ID = (SELECT MAX(LOAD_ID) from {catalog}.data_quality.bronze_to_silver_reconciliation) ORDER BY TEST_CREATED_DATETIME DESC""")

    display(output_query)

# COMMAND ----------

tables_to_test = [ 
('', '')
]
silver_record_count(tables_to_test)

