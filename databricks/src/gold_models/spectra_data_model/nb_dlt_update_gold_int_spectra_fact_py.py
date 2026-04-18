# Databricks notebook source
import dlt
from mdp_databricks_common.sql.sql_loader import SQLLoader
from mdp_databricks_common.utils.data_load_utils import get_asset_bundle_root_path
from pyspark.sql.functions import (
    xxhash64,
    lit,
    to_timestamp,
)
from datetime import datetime

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')
job_name = "SPECTRA"
mdp_gold_layer_processed_datetime = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC # fact_workflow_management

# COMMAND ----------

@dlt.table(
    name="fact_workflow_management",
    comment="Fact table for workflow management. One row per work item."
)
def fact_workflow_management():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/fact_workflow_management.sql"
        ),
        replacements=None
    )
 
    # Load the initial DataFrame
    df = spark.sql(
        sl.load()
    )
 
    # Generate PK's and housekeeping
    df = df.withColumns({
        "PK_FACT_WORKFLOW_MANAGEMENT": xxhash64("BK_WORKFLOW_WORK_ITEM"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })
 
    df = df.select(
        "PK_FACT_WORKFLOW_MANAGEMENT",
        "FK_WORKFLOW_CHANNEL",
        "FK_WORKFLOW_CREATED_DATE",
        "FK_WORKFLOW_RECEIVED_DATE",
        "FK_WORKFLOW_CLOSED_DATE",
        "WORKFLOW_CREATED_TIME",
        "WORKFLOW_RECEIVED_TIME",
        "WORKFLOW_CLOSED_TIME",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )
 
    # Check duplicate PK
    duplicate_check = df.groupBy("PK_FACT_WORKFLOW_MANAGEMENT").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in fact_workflow_management")
 
    return df
