# Databricks notebook source
from datetime import datetime
from pyspark.sql import SparkSession

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

def gold_fact_tests(fact_to_test):

    # Set the catalog name
    catalog = env_var + '_catalog'
    fact_table = fact_to_test

    # Run queries to get the schema name and a list of foreign key columns from the information schema
    schema_name = spark.sql(
        f"SELECT table_schema FROM {catalog}.information_schema.tables WHERE table_name = '{fact_table}'"
    ).select('table_schema').collect()[0]['table_schema']

    reconciliation_table = env_var + '_catalog.data_quality.gold_fact_quality_engineering'
    test_type = 'Gold Fact tests'
    fact_count = spark.sql(f"SELECT COUNT(*) as fact_count FROM {catalog}.{schema_name}.{fact_table}").collect()[0]['fact_count']
    max_load_id_result = spark.sql(f"SELECT MAX(LOAD_ID) AS max_pk FROM {reconciliation_table}").collect()[0]['max_pk']
    load_id = max_load_id_result + 1 if max_load_id_result is not None else 1
    test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    fk_columns_df = spark.sql(
        f"SELECT column_name FROM {catalog}.information_schema.columns WHERE table_name = '{fact_table}'"
    ).select('column_name')

    fk_columns_list = [row['column_name'] for row in fk_columns_df.collect()]

    for column in fk_columns_list:
        try:
            max_pk_result = spark.sql(f"SELECT MAX(PK_GOLD_FACT_QUALITY_ENGINEERING) AS max_pk FROM {reconciliation_table}").collect()[0]['max_pk']
            pk_gold_reconciliation = max_pk_result + 1 if max_pk_result is not None else 1

            if column.startswith('FK_'):
                # Foreign Key Check
                fk_check_query = spark.sql(
                    f"SELECT COUNT(*) as fk_count FROM {catalog}.{schema_name}.{fact_table} WHERE {column} NOT IN ('-2', '-1', '0')").collect()[0]['fk_count']
                fk_check_result = 'FAIL' if fk_check_query == 0 else 'PASS'
                fk_check_message = f"Foreign Key Check Failed. {column} count: {fk_check_query}" if fk_check_result == 'FAIL' else f"Foreign Key Check Passed. {column} count: {fk_check_query}"

                # Foreign Key Null Check
                fk_null_check = spark.sql(
                    f"SELECT COUNT(*) as fk_null_count FROM {catalog}.{schema_name}.{fact_table} WHERE {column} IS NULL").collect()[0]['fk_null_count']
                fk_null_check_result = 'PASS' if fk_null_check == 0 else 'FAIL'
                fk_null_message = f"Foreign Key Null Check Failed. {column} NULL count: {fk_null_check}" if fk_null_check_result == 'FAIL' else f"Foreign Key Null Check Passed. {column} NULL count: {fk_null_check}"


                # Foreign Key Datatype Check
                fk_string_type_pass = spark.sql(
                    f"SELECT case when column_name not like '%_DATE%' AND full_data_type != 'bigint' THEN 'FAIL' ELSE 'PASS' END AS result FROM {catalog}.information_schema.columns WHERE table_name = '{fact_table}' AND column_name = '{column}'").collect()[0]['result']
                fk_string_data_type = spark.sql(
                    f"SELECT full_data_type FROM {catalog}.information_schema.columns WHERE table_name = '{fact_table}' AND column_name = '{column}'").collect()[0]['full_data_type']
                fk_string_type_message = f"Foreign Key Datatype Check Failed. Needs to be set to BIGINT. {column} Datatype: {fk_string_data_type}" if fk_string_type_pass == 'FAIL' else f"Foreign Key Datatype Check Passed. {column} Datatype: {fk_string_data_type}"

                insert_query = spark.sql(f"""
                    INSERT INTO {reconciliation_table}
                    (PK_GOLD_FACT_QUALITY_ENGINEERING, TEST_TYPE, CATALOG_NAME, SCHEMA_NAME, FACT_NAME, FACT_COUNT, COLUMN_NAME, FOREIGN_KEY_RELATIONSHIP_CHECK, FOREIGN_KEY_RELATIONSHIP_CHECK_MESSAGE, FOREIGN_KEY_NULL_CHECK, FOREIGN_KEY_NULL_CHECK_MESSAGE, DATA_TYPE_CHECK, DATA_TYPE_CHECK_MESSAGE, LOAD_ID, TEST_CREATED_DATETIME)
                    VALUES
                    ('{pk_gold_reconciliation}','{test_type}','{catalog}','{schema_name}','{fact_table}','{fact_count}','{column}','{fk_check_result}','{fk_check_message}','{fk_null_check_result}','{fk_null_message}','{fk_string_type_pass}','{fk_string_type_message}','{load_id}','{test_created_datetime}')
                """)

            else:
                fk_string_type = spark.sql(
                    f"SELECT data_type FROM {catalog}.information_schema.columns WHERE table_name = '{fact_table}' AND column_name = '{column}'").collect()[0]['data_type']
                fk_string_type_result = 'PASS' if fk_string_type not in ('string', 'double') else 'FAIL'
                fk_string_type_message = f"Datatype Check Failed. {column} Datatype: {fk_string_type}" if fk_string_type_result == 'FAIL' else f"Datatype Check Passed. {column} Datatype: {fk_string_type}"

                insert_query = spark.sql(f"""
                INSERT INTO {reconciliation_table}
                (PK_GOLD_FACT_QUALITY_ENGINEERING, TEST_TYPE, CATALOG_NAME, SCHEMA_NAME, FACT_NAME, FACT_COUNT, COLUMN_NAME, FOREIGN_KEY_RELATIONSHIP_CHECK, FOREIGN_KEY_RELATIONSHIP_CHECK_MESSAGE, FOREIGN_KEY_NULL_CHECK, FOREIGN_KEY_NULL_CHECK_MESSAGE, DATA_TYPE_CHECK, DATA_TYPE_CHECK_MESSAGE, LOAD_ID, TEST_CREATED_DATETIME)
                VALUES
                ('{pk_gold_reconciliation}','{test_type}','{catalog}','{schema_name}','{fact_table}','{fact_count}','{column}','N/A','N/A','N/A','N/A','{fk_string_type_result}','{fk_string_type_message}','{load_id}','{test_created_datetime}')
                """)

        except Exception as e:
            error_message = str(e).replace("`", "").replace("'", "''")[:200]
            exception_table = env_var + '_catalog.data_quality.gold_exception'
            exception_max_pk_result = spark.sql(f"SELECT MAX(PK_GOLD_EXCEPTION) AS max_pk FROM {exception_table}").collect()[0]['max_pk']
            pk_exception = exception_max_pk_result + 1 if exception_max_pk_result is not None else 1
            test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            max_load_id_result = spark.sql(f"SELECT MAX(LOAD_ID) AS max_pk FROM {exception_table}").collect()[0]['max_pk']
            load_id = max_load_id_result + 1 if max_load_id_result is not None else 1
            insert_exception_query = f"""
                INSERT INTO {exception_table}
                (PK_GOLD_EXCEPTION, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, ERROR_MESSAGE, LOAD_ID, EXCEPTION_CREATED_DATETIME)
                VALUES
                ('{pk_exception}', '{catalog}', '{schema_name}', '{fact_table}', '{error_message}', '{load_id}', '{test_created_datetime}')
            """
            spark.sql(insert_exception_query)

    output_query = spark.sql(
        f"""SELECT * FROM {catalog}.data_quality.gold_fact_quality_engineering WHERE (FOREIGN_KEY_RELATIONSHIP_CHECK = 'FAIL' OR FOREIGN_KEY_NULL_CHECK = 'FAIL' OR DATA_TYPE_CHECK = 'FAIL') AND LOAD_ID = (SELECT MAX(LOAD_ID) from {catalog}.data_quality.gold_fact_quality_engineering) ORDER BY TEST_CREATED_DATETIME DESC""")
    display(output_query)

# COMMAND ----------

fact_to_test = ''
gold_fact_tests(fact_to_test)
