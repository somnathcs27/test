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
# MAGIC # dim_workflow_channel

# COMMAND ----------

# DBTITLE 1,dim_workflow_channel
expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_CHANNEL IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_CHANNEL IS NOT NULL",
}

@dlt.table(
    name="dim_workflow_channel",
    comment="Type 1 dimension for spectra channels. One row per channel."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_channel():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/dim_workflow_channel.sql"
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
        "PK_WORKFLOW_CHANNEL": xxhash64("BK_WORKFLOW_CHANNEL"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_WORKFLOW_CHANNEL",
        "BK_WORKFLOW_CHANNEL",
        "CHANNELS_NAME",
        "IS_INBOUND",
        "IS_OUTBOUND",
        "DEFAULT_OUTBOUND_CHANNEL_IDENTIFIER",
        "INITIAL_OFFERS_SLA",
        "INITIAL_OFFER_PRIORITY_SLA",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_CHANNEL").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_channel")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # dim_workflow_work_item

# COMMAND ----------

# DBTITLE 1,dim_workflow_work_item
expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_WORK_ITEM IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_WORK_ITEM IS NOT NULL",
}

@dlt.table(
    name="dim_workflow_work_item",
    comment="Type 1 dimension for spectra work items. One row per work item."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_work_item():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/dim_workflow_work_item.sql"
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
        "PK_WORKFLOW_WORK_ITEM": xxhash64("BK_WORKFLOW_WORK_ITEM"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_WORKFLOW_WORK_ITEM",
        "BK_WORKFLOW_WORK_ITEM",
        "WORK_ITEM_TYPE_NAME",
        "WORK_ITEM_STATUS_ID",
        "IS_CLOSED_WITHIN_SLA",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_WORK_ITEM").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_work_item")

    return df
	

# COMMAND ----------

# MAGIC %md
# MAGIC # dim_workflow_account

# COMMAND ----------

# DBTITLE 1,dim_workflow_account
expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_ACCOUNT IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_ACCOUNT IS NOT NULL",
}

@dlt.table(
    name="dim_workflow_account",
    comment="Type 1 dimension for spectra accounts. One row per account."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_account():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/dim_workflow_account.sql"
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
        "PK_WORKFLOW_ACCOUNT": xxhash64("BK_WORKFLOW_ACCOUNT"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_WORKFLOW_ACCOUNT",
        "BK_WORKFLOW_ACCOUNT",
        "ACCOUNT_NUMBER",   
		"SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_ACCOUNT").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_account")

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC #dim_workflow_category

# COMMAND ----------

# DBTITLE 1,dim_workflow_category
expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_CATEGORY IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_CATEGORY IS NOT NULL",
}

@dlt.table(
    name="dim_workflow_category",
    comment="Type 1 dimension for spectra catergories. One row per work catergory."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_category():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/dim_workflow_category.sql"
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
        "PK_WORKFLOW_CATEGORY": xxhash64("BK_WORKFLOW_CATEGORY"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_WORKFLOW_CATEGORY",
        "BK_WORKFLOW_CATEGORY",
        "CATEGORIES_NAME",
        "CATEGORY_TIER",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_CATEGORY").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_category")

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC # dim_workflow_outcome

# COMMAND ----------

# DBTITLE 1,dim_workflow_outcome
expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_OUTCOME IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_OUTCOME IS NOT NULL",
}

@dlt.table(
    name="dim_workflow_outcome",
    comment="Type 1 dimension for spectra outcomes. One row per work outcome."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_outcome():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/dim_workflow_outcome.sql"
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
        "PK_WORKFLOW_OUTCOME": xxhash64("BK_WORKFLOW_OUTCOME"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_WORKFLOW_OUTCOME",
        "BK_WORKFLOW_OUTCOME",
        "OUTCOME_NAME",
		"OUTCOME_SHORT_NAME",
		"OUTCOME_DESCRIPTION",	
		"IS_SLA_OFFSET",
		"EXPECTED_WORK_TIME",
		"IS_TRIGGER_CHECKPOINT",
		"IS_RECORD_OUTCOME",
		"IS_PROCESS_ASYNC",
		"IS_OFFER_PROCESS_ASYNC",
		"IS_COMMIT_PROCESS_ASYNC",
        "IS_DELETED",
		"SLA_OFFSET_DAYS",
		"HAS_DATA_FORM_PRECEDENCE",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_OUTCOME").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_outcome")

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC # dim_workflow_customer

# COMMAND ----------

# DBTITLE 1,dim_workflow_customer
expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_CUSTOMER IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_CUSTOMER IS NOT NULL",
}

@dlt.table(
    name="dim_workflow_customer",
    comment="Type 1 dimension for spectra customers. One row per work customer."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_customer():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/dim_workflow_customer.sql"
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
        "PK_WORKFLOW_CUSTOMER": xxhash64("BK_WORKFLOW_CUSTOMER"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_WORKFLOW_CUSTOMER",
        "BK_WORKFLOW_CUSTOMER",
        "CUSTOMER_NUMBER",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_CUSTOMER").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_customer")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #dim_workflow_timing

# COMMAND ----------

# DBTITLE 1,dim_workflow_timing
expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_TIMING IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_TIMING IS NOT NULL",
}

@dlt.table(
    name="dim_workflow_timing",
    comment="Type 1 dimension for spectra timings. One row per work timing."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_timing():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/dim_workflow_timing.sql"
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
        "PK_WORKFLOW_TIMING": xxhash64("BK_WORKFLOW_TIMING"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })

    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)

    df = df.select(
        "PK_WORKFLOW_TIMING",
        "BK_WORKFLOW_TIMING",
        "CATEGORY_NAME",
		"CHANNEL_NAME",
		"TIMING",
		"TIMING_START_DATE",
        "TIMING_END_DATE",
		"COMPLETION_SLA",
		"INITIAL_OFFER_SLA",
		"INITIAL_OFFER_PRIORITY_SLA",
		"IS_DELETED",		
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )

    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_TIMING").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_timing")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #dim_workflow_business_stream

# COMMAND ----------

expectations = {
    "primary_keys_are_not_null": "PK_WORKFLOW_BUSINESS_STREAM IS NOT NULL",
    "business_keys_are_not_null": "BK_WORKFLOW_BUSINESS_STREAM IS NOT NULL",
}
 
@dlt.table(
    name="dim_workflow_business_stream",
    comment="Type 1 dimension for spectra business streams. One row per business stream."
)
@dlt.expect_all_or_fail(expectations)
def dim_workflow_business_stream():
    sl = SQLLoader(
        environment_name=env_var,
        target_path=(
            f"{get_asset_bundle_root_path(env_var)}"
            "files/src/gold_models/spectra_data_model/"
            "sql_scripts/internal/dim_workflow_business_stream.sql"
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
        "PK_WORKFLOW_BUSINESS_STREAM": xxhash64("BK_WORKFLOW_BUSINESS_STREAM"),
        "MDP_GOLD_LAYER_PROCESSED_DATETIME": to_timestamp(
            lit(
                mdp_gold_layer_processed_datetime
            )
        )
    })
 
    # Add the dimension dummy rows
    df = insert_dimension_dummy_rows(df)
 
    df = df.select(
        "PK_WORKFLOW_BUSINESS_STREAM",
        "BK_WORKFLOW_BUSINESS_STREAM",
        "BUSINESS_STREAM_NAME",
        "IS_DELETED",
        "SOURCE_SYSTEM",
        "ROW_START_DATETIME",
        "ROW_END_DATETIME",
        "ROW_IS_CURRENT",
        "MDP_GOLD_LAYER_PROCESSED_DATETIME"
    )
 
    # Check duplicate PK
    duplicate_check = df.groupBy("PK_WORKFLOW_BUSINESS_STREAM").count().filter("count > 1")
    if duplicate_check.count() > 0:
        raise Exception("Duplicate primary key found in dim_workflow_channel")
 
    return df
