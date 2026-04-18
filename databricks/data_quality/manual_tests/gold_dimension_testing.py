# Databricks notebook source
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

from pyspark.sql.types import DateType, TimestampType

def count_nulls(catalog, schema_name, table_name):
    # Get the dataframe
    df = spark.table(f"{catalog}.{schema_name}.{table_name}")
    
    # Get the schema of the table
    schema = df.schema
    
    relevant_columns = [field.name for field in schema.fields if field.name != 'ROW_END_DATETIME']
    
    # Count nulls only for relevant columns
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in relevant_columns])
    
    # Collect columns with null values
    columns_with_nulls = [col for col, count in null_counts.collect()[0].asDict().items() if count > 0]
    
    # Convert the list of columns with null values to a comma-separated string
    columns_with_nulls_str = ', '.join(columns_with_nulls)
    
    return columns_with_nulls_str

# COMMAND ----------

def check_row_is_current(catalog, schema_name, dim_table):
    # Check if ROW_IS_CURRENT column exists
    columns = spark.sql(f"SELECT column_name FROM {catalog}.information_schema.columns WHERE table_name = '{dim_table}'").collect()
    column_names = [col['column_name'] for col in columns]
    
    if 'ROW_IS_CURRENT' in column_names:
        row_is_current_check = spark.sql(
            f"SELECT CASE WHEN COUNT(*) = 0 THEN 'Only current Rows are in the dimension {dim_table}' else 'historic records are in the dimension {dim_table}, is this expected?' END AS row_is_current_check FROM {catalog}.{schema_name}.{dim_table} WHERE ROW_IS_CURRENT NOT IN(-1, -2, 1)"
        ).collect()[0]['row_is_current_check']
    else:
        row_is_current_check = f"Failed: ROW_IS_CURRENT column not found in {dim_table}"
    
    return row_is_current_check

# COMMAND ----------

def flag_column_check(catalog, schema_name, dim_table, primary_key_column):
    columns = spark.sql(f"DESCRIBE {catalog}.{schema_name}.{dim_table}").collect()
    column_names = [col['col_name'] for col in columns]

    failures = []

    for column in column_names:
        distinct_values = spark.sql(
            f"SELECT DISTINCT {column} FROM {catalog}.{schema_name}.{dim_table} WHERE {primary_key_column} NOT IN (-1, -2)"
        ).collect()
        
        distinct_values = [row[column] for row in distinct_values]

        if column.startswith('IS_') or column.startswith('HAS_') or column.startswith('WAS_'):
            distinct_count = len(distinct_values)
            if distinct_count < 2:
                failures.append(f"Column {column} check failed. Only one value in column.")
            elif distinct_count == 2:
                if set(distinct_values) == {"Yes", "No"}:
                    continue
                else:
                    failures.append(f"Column {column} check failed. Distinct values: {distinct_values}")
            elif distinct_count == 3:
                if set(distinct_values) == {"Yes", "No", "Unknown"}:
                    continue
                else:
                    failures.append(f"Column {column} check failed. Distinct values: {distinct_values}")
            else:
                failures.append(f"Column {column} check failed. Too many values in column: {distinct_values}")
        else:
            if "Yes" in distinct_values and "No" in distinct_values and len(distinct_values) <= 4:
                failures.append(f"Column {column} is a flag, but naming convention not being adhered to.")
            else:
                continue

    if failures:
        return "Failed: " + "; ".join(failures)
    else:
        return "Passed"

# COMMAND ----------

def bk_duplicate_check(catalog, schema_name, dim_table):
    # Get all business key columns
    bk_columns = spark.sql(
        f"SELECT column_name FROM {catalog}.information_schema.columns WHERE table_name = '{dim_table}' and column_name LIKE 'BK_%'"
    ).collect()
    
    # Extract column names
    bk_column_names = [col['column_name'] for col in bk_columns]
    
    # Join column names to form the partition by clause
    partition_by_clause = ", ".join(bk_column_names)
    
    # Construct the query
    duplicate_check_query = spark.sql(
        f"""
        SELECT * FROM (
            SELECT row_number() OVER (
                PARTITION BY {partition_by_clause} 
                ORDER BY {partition_by_clause} DESC
            ) as rownum 
            FROM {catalog}.{schema_name}.{dim_table} 
            WHERE ROW_IS_CURRENT = 1
        ) 
        WHERE rownum > 1
        """
    ).collect()
    
    # Determine the result
    duplicate_check_result = "Passed: No Duplicate Business Keys Found" if len(duplicate_check_query) == 0 else "Failed: Duplicate Records Found"
    
    return duplicate_check_result

# COMMAND ----------

def gold_dimension_tests(dimension_tables):
    catalog = env_var + '_catalog'

    for dim_table in dimension_tables:
        try:
            # Run queries to get the schema name and a list of foreign key columns from the information schema
            schema_name = spark.sql(
                f"SELECT table_schema FROM {catalog}.information_schema.tables WHERE table_name = '{dim_table}'"
            ).select('table_schema').collect()[0]['table_schema']

            reconciliation_table = f"{catalog}.data_quality.gold_dimension_quality_engineering"
            max_pk_result = spark.sql(f"SELECT MAX(PK_GOLD_DIMENSION_QUALITY_ENGINEERING) AS max_pk FROM {reconciliation_table}").collect()[0]['max_pk']
            pk_gold_reconciliation = max_pk_result + 1 if max_pk_result is not None else 1
            test_type = 'Gold dimension tests'
            dimension_count = spark.sql(f"SELECT COUNT(*) as dim_count FROM {catalog}.{schema_name}.{dim_table}").collect()[0]['dim_count']
            primary_key_column = spark.sql(f"SELECT column_name FROM {catalog}.information_schema.columns WHERE table_name = '{dim_table}' AND ordinal_position = 0").collect()[0]['column_name']
            max_load_id_result = spark.sql(f"SELECT MAX(LOAD_ID) AS max_pk FROM {reconciliation_table}").collect()[0]['max_pk']
            load_id = max_load_id_result + 1 if max_load_id_result is not None else 1
            test_created_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            #Business Key Duplicate Check
            bk_duplicate_check_result = bk_duplicate_check(catalog, schema_name, dim_table)

            # Primary Key Check
            pk_check = spark.sql(
                f"SELECT CASE WHEN column_name = concat('PK_', substring(upper(table_name), 5)) and full_data_type = 'bigint' then 'Passed: Primary Key set correctly for {dim_table}' when column_name != concat('PK_', substring(upper(table_name), 5)) then 'Failed: Primary Key column name is incorrect for {dim_table}' when full_data_type != 'bigint' then 'Failed: Primary Key is not a bigint for {dim_table}' else 'Failed: Primary Key not set correctly for {dim_table}' END AS pk_check FROM {catalog}.information_schema.columns WHERE table_name = '{dim_table}' AND ordinal_position = 0"
            ).collect()[0]['pk_check']

            pk_pass_fail = 'PASS' if pk_check == 'Passed: Primary Key set correctly for ' + dim_table else 'FAIL'

            bk_check = spark.sql(
                f"SELECT CASE WHEN column_name = concat('BK_', substring(upper(table_name), 5)) then 'Passed: Business Key set correctly for {dim_table}' else 'Failed: Business Key not set correctly for {dim_table}' END AS bk_check FROM {catalog}.information_schema.columns WHERE table_name = '{dim_table}' AND ordinal_position = 1"
            ).collect()[0]['bk_check']

            bk_pass_fail = 'PASS' if bk_check == 'Passed: Business Key set correctly for ' + dim_table else 'FAIL'

            # Check if ROW_IS_CURRENT column exists
            row_is_current_check = check_row_is_current(catalog, schema_name, dim_table)

            # Column Null Check
            columns_with_nulls = count_nulls(catalog, schema_name, dim_table)
            column_with_nulls_result = "PASS" if len(columns_with_nulls) == 0 else "FAIL"

            MDP_Load_Columns = spark.sql(f"""
                                         SELECT 
                                         CASE 
                                            WHEN COUNT(*) > 0 THEN 'FAIL: MDP_LOAD Columns Need Removing From the Dimension'
                                            ELSE 'PASS'
                                         END AS test_result
                                         FROM {catalog}.information_schema.columns 
                                         WHERE table_name = '{dim_table}' AND column_name IN ('MDP_LOAD_DATETIME', 'MDP_LOAD_ID')""").collect()[0]['test_result']

            # Housekeeping Columns Check
            house_keeping_check = spark.sql(f"""
                SELECT 
                    CASE
                        WHEN COUNT(*) = 3 THEN 'PASS'
                        WHEN COUNT(*) = 2 THEN 'One Housekeeping Column Missing'
                        WHEN COUNT(*) = 1 THEN 'Two Housekeeping Column Missing'
                        ELSE 'All HouseKeeping Columns Missing' 
                    END AS test_result
                FROM {catalog}.information_schema.columns 
                WHERE table_name = '{dim_table}'
                    AND column_name IN ('ROW_START_DATETIME', 'ROW_END_DATETIME', 'ROW_IS_CURRENT', 'ROW_START_DATE', 'ROW_END_DATE')
            """).collect()[0]['test_result']

            housekeeping_result = f"Housekeeping Check Passed for {dim_table}: All columns present." if house_keeping_check == 'PASS' and MDP_Load_Columns == 'PASS' else f"Housekeeping Check Failed for {dim_table}: {house_keeping_check}" if house_keeping_check != 'PASS' and MDP_Load_Columns == 'PASS' else f"Housekeeping Check Failed for {dim_table}: {MDP_Load_Columns}"
            # Dummy Records Check
            dummy_records_check = spark.sql(f"SELECT COUNT(*) as dummy_count FROM {catalog}.{schema_name}.{dim_table} WHERE {primary_key_column} in(-1, -2)").collect()[0]['dummy_count']
            dummy_records_result = 'PASS' if dummy_records_check == 2 else f"FAIL: {dummy_records_check} dummy records found in {dim_table}"

            # Data Type Check
            double_type_check = spark.sql(f"SELECT COUNT(*) as double_count FROM {catalog}.information_schema.columns WHERE table_name = '{dim_table}' AND data_type IN('double')").collect()[0]['double_count']
            double_check_result = 'PASS' if double_type_check == 0 else f"FAIL: {double_type_check} columns have a double data type in {dim_table}"

            flag_column_check_result = flag_column_check(catalog, schema_name, dim_table, primary_key_column)

            insert_query = spark.sql(f"""
                INSERT INTO {reconciliation_table}
                (PK_GOLD_DIMENSION_QUALITY_ENGINEERING, TEST_TYPE, CATALOG_NAME, SCHEMA_NAME, DIMENSION_NAME, DIMENSION_COUNT, PRIMARY_KEY_CHECK, PRIMARY_KEY_CHECK_MESSAGE, BUSINESS_KEY_CHECK, BUSINESS_KEY_CHECK_MESSAGE, HOUSEKEEPING_TEST_OUTCOME, DATA_TYPE_CHECK, DUMMY_RECORDS_CHECK, COLUMN_NULL_CHECK, COLUMNS_WITH_NULL_VALUES, ROW_IS_CURRENT_CHECK, COLUMN_FLAG_CHECK, BUSINESS_KEY_DUPLICATE_CHECK, LOAD_ID, TEST_CREATED_DATETIME)
                VALUES
                ('{pk_gold_reconciliation}','{test_type}','{catalog}','{schema_name}','{dim_table}','{dimension_count}','{pk_pass_fail}','{pk_check.replace("'", "''")}','{bk_pass_fail}','{bk_check.replace("'", "''")}','{housekeeping_result.replace("'", "''")}','{double_check_result.replace("'", "''")}','{dummy_records_result.replace("'", "''")}','{column_with_nulls_result.replace("'", "''")}','{columns_with_nulls.replace("'", "''")}', '{row_is_current_check.replace("'", "''")}', '{flag_column_check_result.replace("'", "''")}',  '{bk_duplicate_check_result.replace("'", "''")}', '{load_id}','{test_created_datetime}')
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
                ('{pk_exception}', '{catalog}', '{schema_name}', '{dim_table}', '{error_message}', '{load_id}', '{test_created_datetime}')
            """
            spark.sql(insert_exception_query)
    
    output_query = spark.sql(
    f"""SELECT * FROM {catalog}.data_quality.gold_dimension_quality_engineering WHERE LOAD_ID = (SELECT MAX(LOAD_ID) from {catalog}.data_quality.gold_dimension_quality_engineering) ORDER BY TEST_CREATED_DATETIME DESC""")

    display(output_query) 

# COMMAND ----------

dimension_tables = ['']
gold_dimension_tests(dimension_tables)
