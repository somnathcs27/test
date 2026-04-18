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
job_name = "MSO"
mdp_gold_layer_processed_datetime = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC # dim_mortgage_case

# COMMAND ----------

expectations = {
    "primary_keys_are_not_null": "PK_MORTGAGE_CASE IS NOT NULL",
    "business_keys_are_not_null": "BK_MORTGAGE_CASE IS NOT NULL",
}

@dlt.table(
    name="dim_mortgage_case",
    comment="Type 1 dimension for MSO mortgage cases. One row per case."
)
@dlt.expect_all_or_fail(expectations)
def dim_mortgage_case():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/mso_data_model/"
            "sql_scripts/confidential/dim_mortgage_case.sql"
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
        "PK_MORTGAGE_CASE": xxhash64("BK_MORTGAGE_CASE"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_MORTGAGE_CASE",
        "BK_MORTGAGE_CASE",
        "APPLICATION_NUMBER",
        "APPLICATION_TYPE",
        "FRIENDLY_ID",
        "CASE_STATUS",
        "CASE_STAGE",
        "CHANNEL",
        "MORTGAGE_ACCOUNT_NUMBER",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME",
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_MORTGAGE_CASE").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_mortgage_case")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #dim_mortgage_applicant

# COMMAND ----------

expectations = {
    "primary_keys_are_not_null": "PK_MORTGAGE_APPLICANT IS NOT NULL",
    "business_keys_are_not_null": "BK_MORTGAGE_APPLICANT IS NOT NULL",
}

@dlt.table(
    name="dim_mortgage_applicant",
    comment="Type 1 dimension for MSO mortgage applicants. One row per appliant."
)
@dlt.expect_all_or_fail(expectations)
def dim_mortgage_applicant():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/mso_data_model/"
            "sql_scripts/confidential/dim_mortgage_applicant.sql"
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
        "PK_MORTGAGE_APPLICANT": xxhash64("BK_MORTGAGE_APPLICANT"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_MORTGAGE_APPLICANT",
        "BK_MORTGAGE_APPLICANT",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME",
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_MORTGAGE_APPLICANT").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_mortgage_applicant")

    return df
