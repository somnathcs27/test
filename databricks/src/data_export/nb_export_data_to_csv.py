# Databricks notebook source
import csv
import json

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("child_pipeline", "")
dbutils.widgets.text("source_query", "")

# COMMAND ----------

DEFAULT_CATALOG = dbutils.widgets.get("catalog_name")
CHILD_PIPELINE = dbutils.widgets.get("child_pipeline")
SOURCE_QUERY = dbutils.widgets.get("source_query")

# COMMAND ----------

# Get all rows from the ETL metadata which must be processed
sourcedata_df = spark.sql(f"SELECT * FROM {DEFAULT_CATALOG}.etl.ext_etl_task_details WHERE ext_etl_task_details.JobName = '{CHILD_PIPELINE}' AND ext_etl_task_details.TaskName = 'MI_REPORT'")
source_data = sourcedata_df.collect()

entity_details = {}

for row in source_data:
  # Consolidate values as a dictionary to pass to function
    entity_details[row.TaskName] = {
      "entity_id": row.PK_TaskDetail,
      "source_system" : row.JobName,
      "source_schema" : row.SourceSchema,
      "source_entity" : row.TaskName,
      "spark_schema" : row.SparkSchema,
      "spark_table" : row.SparkTable,
      "sensitivity" : row.SensitivityName,
      "catalog" : DEFAULT_CATALOG,
      "is_change_data" : row.LoadTypeName,
      "file_type" : row.FileType,
      "first_row_headers": str(bool(json.loads(row.TaskDetailJSON).get("FlatFileConnectionParameters", {}).get("FirstRowContainsHeaders", None))),
      "delimiter" : str(json.loads(row.TaskDetailJSON).get("FlatFileConnectionParameters", {}).get("ColumnDelimiter")),
      "quote_character" : str(json.loads(row.TaskDetailJSON).get("FlatFileConnectionParameters", {}).get("QuoteCharacter"))
  }
     
print(entity_details)

# COMMAND ----------

df = spark.sql(SOURCE_QUERY)

csv_string = df.toPandas().to_csv(
  path_or_buf=None,
  index=False,
  sep=entity_details.get("MI_REPORT", {}).get("delimiter")
)

# COMMAND ----------

dbutils.notebook.exit(csv_string)
