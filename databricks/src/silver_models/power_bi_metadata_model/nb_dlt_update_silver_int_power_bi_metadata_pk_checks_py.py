# Databricks notebook source
# MAGIC %md
# MAGIC # [outdated - be careful - pull in from repo if needed]

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
import pyspark.sql.types as T
from typing import Optional

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')
target_schema = f"{env_var}_catalog.silver_int"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC |||
# MAGIC |---|---|
# MAGIC | ![asd](https://upload.wikimedia.org/wikipedia/en/d/d8/Windows_11_Clippy_paperclip_emoji.png)    | This is a simple pipeline whose purpose is to run the DQ checks on PK uniqueness for the items loaded in the prior Silver data pipeline. |
# MAGIC |||

# COMMAND ----------

def generate_pk_uniqueness_test(
    source_table_name: str,
    source_table_schema: str,
    group_by_cols: list[str],
    target_count: str = "num_entries"
    ) -> None:
    """
    generate_pk_uniqueness_test
    ============================

    Generate a test for uniqueness of a given PK (or set thereof) on a given table.
    """
    @dlt.table(
        name=f"{source_table_name}_silver_pk_tests",
        comment=f"Validates primary key uniqueness for {source_table_schema}.{source_table_name}."
    )
    @dlt.expect_or_fail(f"unique_ID_on_{source_table_name}", f"{target_count} = 1")
    def validate_pk_uniqueness():
        return (
            spark
            .read
            .table(f"{source_table_schema}.{source_table_name}")
            .groupBy(*group_by_cols)
            .count()
            .withColumnRenamed("count", target_count)
        )

# COMMAND ----------

# MAGIC %md
# MAGIC # Activity Events

# COMMAND ----------

generate_pk_uniqueness_test(
    source_table_name = "power_bi_metadata_activity_events",
    source_table_schema = target_schema,
    group_by_cols = ["ACTIVITY_EVENT_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apps

# COMMAND ----------

generate_pk_uniqueness_test(
    source_table_name = "power_bi_metadata_apps",
    source_table_schema = target_schema,
    group_by_cols = ["APP_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Dashboards

# COMMAND ----------

generate_pk_uniqueness_test(
    source_table_name = "power_bi_metadata_dashboards",
    source_table_schema = target_schema,
    group_by_cols = ["DASHBOARD_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Datasets

# COMMAND ----------

generate_pk_uniqueness_test(
    source_table_name = "power_bi_metadata_datasets",
    source_table_schema = target_schema,
    group_by_cols = ["DATASET_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Groups

# COMMAND ----------

generate_pk_uniqueness_test(
    source_table_name = "power_bi_metadata_groups",
    source_table_schema = target_schema,
    group_by_cols = ["GROUP_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reports

# COMMAND ----------

generate_pk_uniqueness_test(
    source_table_name = "power_bi_metadata_reports",
    source_table_schema = target_schema,
    group_by_cols = ["REPORT_ID"],
)
