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
# MAGIC # fact_mortgage_case

# COMMAND ----------

@dlt.table(
    name="fact_mortgage_case",
    comment="Fact table for mortgage cases. One row per case.",
    schema="""
            PK_FACT_MORTGAGE_CASE       bigint COMMENT '',
            FK_MORTGAGE_CASE            bigint COMMENT '',
            FK_MAIN_MORTGAGE_PRODUCT    bigint COMMENT '',
            FK_EARLIEST_COMPLETION_DATE date COMMENT '',
            FK_LATEST_COMPLETION_DATE   date COMMENT '',
            FK_CREATED_DATE             date COMMENT '',
            FK_LATEST_VALUATION_DATE    date COMMENT '',
            TOTAL_LOAN_AMOUNT           decimal(10,2) COMMENT 'Sum of the individual loan part amounts in GBP.',
            LATEST_VALUATION_AMOUNT     decimal(19,2) COMMENT '',
            MDP_GOLD_LAYER_PROCESSED_DATETIME timestamp COMMENT ''       
    """
)
def fact_mortgage_case():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/mso_data_model/"
            "sql_scripts/internal/fact_mortgage_case.sql"
        ),
        replacements=None
    )

    # Load the initial DataFrame
    df = spark.sql(
        sl.load()
    )

    # Generate PK's and housekeeping
    df = df.withColumns({
        "PK_FACT_MORTGAGE_CASE": xxhash64("BK_MORTGAGE_CASE"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    df = df.select(
        "PK_FACT_MORTGAGE_CASE",
        "FK_MORTGAGE_CASE",
        "FK_MAIN_MORTGAGE_PRODUCT",
        "FK_EARLIEST_COMPLETION_DATE",
        "FK_LATEST_COMPLETION_DATE",
        "FK_CREATED_DATE",
        "FK_LATEST_VALUATION_DATE",
        "TOTAL_LOAN_AMOUNT",
        "LATEST_VALUATION_AMOUNT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME",
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_FACT_MORTGAGE_CASE").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in fact_mortgage_case")

    return df
