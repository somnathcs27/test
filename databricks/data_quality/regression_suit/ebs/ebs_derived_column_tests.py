# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Load the derived data
derived_data = spark.table(f"{env_var}_catalog.data_quality.fact_general_ledger_derived_data_test")

# Perform assertions
filtered_data = derived_data.filter(col("JOURNAL_ENTRY_HEADER_ID") == "1")
assert filtered_data.filter(col("SEGMENT2_ACC") == "Segment2Value|ACC").count() == 1, "Test failed for SEGMENT2_ACC"
assert filtered_data.filter(col("SEGMENT2_BOE") == "Segment2Value|BOE").count() == 1, "Test failed for SEGMENT2_BOE"
assert filtered_data.filter(col("ACCOUNTED_NET") == "500").count() == 1, "Test failed for ACCOUNTED_NET"
assert filtered_data.filter(col("ENTERED_NET") == "500").count() == 1, "Test failed for ENTERED_NET"
assert filtered_data.filter(col("REVISED_ENTERED_CR") == "0").count() == 1, "Test failed for REVISED_ENTERED_CR"
assert filtered_data.filter(col("REVISED_ENTERED_DR") == "100").count() == 1, "Test failed for REVISED_ENTERED_DR"
assert filtered_data.filter(col("REVISED_ACCOUNTED_CR") == "0").count() == 1, "Test failed for REVISED_ACCOUNTED_CR"
assert filtered_data.filter(col("REVISED_ACCOUNTED_DR") == "100").count() == 1, "Test failed for REVISED_ACCOUNTED_DR"

print("All tests passed!")

display(filtered_data)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Load the derived data
derived_data = spark.table(f"{env_var}_catalog.data_quality.dim_general_ledger_journal_entry_line_derived_data_test")

# Perform assertions
filtered_data = derived_data.filter(col("BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER") == "1")
assert filtered_data.filter(col("CASHIER") == 1234).count() == 1, "Test failed for CASHIER"

filtered_data = derived_data.filter(col("BK_GENERAL_LEDGER_JOURNAL_ENTRY_HEADER") == "2")
assert filtered_data.filter(col("CASHIER") == "ESBUSER").count() == 1, "Test failed for CASHIER"

print("All tests passed!")

display(filtered_data)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Load the derived data
derived_data = spark.table(f"{env_var}_catalog.data_quality.dim_general_ledger_segment_derived_data_test")

# Perform assertions
filtered_data = derived_data.filter(col("BK_GENERAL_LEDGER_SEGMENT") == "FLEX001")
assert filtered_data.filter(col("SEGMENT_TYPE") == "Segment 1").count() == 1, "Test failed for SEGMENT_TYPE"
assert filtered_data.filter(col("IS_SEGMENT_ENABLED") == "Yes").count() == 1, "Test failed for IS_SEGMENT_ENABLED"
assert filtered_data.filter(col("IS_SEGMENT_SUMMARY") == "No").count() == 1, "Test failed for IS_SEGMENT_SUMMARY"

print("All tests passed!")

display(filtered_data)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Load the derived data
derived_data = spark.table(f"{env_var}_catalog.data_quality.dim_general_ledger_account_derived_data_test")

# Perform assertions
filtered_data = derived_data.filter(col("BK_GENERAL_LEDGER_ACCOUNT") == "Unknown")
assert filtered_data.filter(col("BK_GENERAL_LEDGER_ACCOUNT") == "Unknown").count() == 1, "Test failed for BK_GENERAL_LEDGER_ACCOUNT"
assert filtered_data.filter(col("ACCOUNT_DESCRIPTION") == "Unknown").count() == 1, "Test failed for ACCOUNT_DESCRIPTION"

print("All tests passed!")

display(derived_data)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Load the derived data
derived_data = spark.table(f"{env_var}_catalog.data_quality.dim_general_ledger_company_derived_data_test")

# Perform assertions
filtered_data = derived_data.filter(col("BK_GENERAL_LEDGER_COMPANY") == "FLEX001")
assert filtered_data.filter(col("IS_COMPANY_ENABLED") == "Yes").count() == 1, "Test failed for IS_COMPANY_ENABLED"
assert filtered_data.filter(col("IS_COMPANY_SUMMARY") == "No").count() == 1, "Test failed for IS_COMPANY_SUMMARY"

print("All tests passed!")

display(filtered_data)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Load the derived data
derived_data = spark.table(f"{env_var}_catalog.data_quality.dim_general_ledger_organisation_derived_data_test")

# Perform assertions
filtered_data = derived_data.filter(col("BK_GENERAL_LEDGER_ORGANISATION") == "FLEX001")
assert filtered_data.filter(col("IS_ORGANISATION_ENABLED") == "Yes").count() == 1, "Test failed for IS_ORGANISATION_ENABLED"
assert filtered_data.filter(col("IS_ORGANISATION_SUMMARY") == "No").count() == 1, "Test failed for IS_ORGANISATION_SUMMARY"

print("All tests passed!")

display(filtered_data)
