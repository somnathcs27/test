# Databricks notebook source
import dlt
from mdp_databricks_common.sql.sql_loader import SQLLoader
from mdp_databricks_common.utils.data_load_utils import get_asset_bundle_root_path
from mdp_databricks_common.gold_layer_functions import insert_dimension_dummy_rows
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
# MAGIC #dim_workflow_user
# MAGIC

# COMMAND ----------

expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_USER IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_USER IS NOT NULL",
}
 
@dlt.table(
    name="dim_workflow_user",
    comment="Type 1 dimension for spectra channels user. One row per channel."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_user():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/confidential/dim_workflow_user.sql"
        ),
        replacements=[
            ("{job_name}", job_name)
        ]
    )
 
    # Load the initial DataFrame
    df = spark.sql(
        sl.load()
    )
 
    # Generate PK's and housekeeping
    df = df.withColumns({
        "PK_WORKFLOW_USER": xxhash64("BK_WORKFLOW_USER"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })
 
    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)
 
    df = df.select(
        "PK_WORKFLOW_USER",
        "BK_WORKFLOW_USER",
        "USER_NAME",
        "IS_DELETED",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )
 
    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_USER").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_user")
 
    return df
