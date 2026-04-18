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
job_name = "MSO"
mdp_gold_layer_processed_datetime = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC # bridge_mortgage_case_mortgage_applicant

# COMMAND ----------

@dlt.table(
    name="bridge_mortgage_case_mortgage_applicant",
    comment="Bridge between MSO Case and MSO applicant. One row per applicant per case."
)
def bridge_case_applicant():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/mso_data_model/"
            "sql_scripts/confidential/bridge_mortgage_case_mortgage_applicant.sql"
        ),
        replacements=None
    )

    # Load the initial DataFrame
    df = spark.sql(
        sl.load()
    )

    df = df.withColumn(
        "MDP_GOLD_LAYER_PROCESSED_DATETIME",
        to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    )

    df = df.select(
        "FK_MORTGAGE_CASE",
        "FK_MORTGAGE_APPLICANT",
        "IS_PRIMARY_APPLICANT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME",
    )

    return df
