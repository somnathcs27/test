# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr, lead
from pyspark.sql.window import Window

# Import common ETL and metadata functions
import sys

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name="dataverse_global_option_set_metadata", 
    comment="The table stores the metadata of the global option sets in Dataverse"
)
@dlt.expect("MDP_LOAD_ID is not null", "MDP_LOAD_ID IS NOT NULL")
def dataverse_global_option_set_metadata():

    df = spark.sql(
        f"""
    SELECT   
        OptionSetName AS OPTION_SET_NAME,
        Option AS OPTION,
        IsUserLocalizedLabel AS IS_USER_LOCALIZED_LABEL,
        LocalizedLabelLanguageCode AS LOCALIZED_LABEL_LANGUAGE_CODE,
        LocalizedLabel AS LOCALIZED_LABEL,
        GlobalOptionSetName AS GLOBAL_OPTION_SET_NAME,
        EntityName AS ENTITY_NAME,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        1 AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_dataverse_int.globaloptionsetmetadata
    """
    )     

    return df


# COMMAND ----------

@dlt.table(
    name="dataverse_option_set_metadata", 
    comment="This tables stores the metadata of the option sets in Dataverse"
)
@dlt.expect("MDP_LOAD_ID is not null", "MDP_LOAD_ID IS NOT NULL")
def dataverse_option_set_metadata():

    df = spark.sql(
        f"""
    SELECT    
        EntityName AS ENTITY_NAME,
        OptionSetName AS OPTION_SET_NAME,
        Option AS OPTION,
        IsUserLocalizedLabel AS IS_USER_LOCALIZED_LABEL,
        LocalizedLabelLanguageCode AS LOCALIZED_LABEL_LANGUAGE_CODE,
        LocalizedLabel AS LOCALIZED_LABEL,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        MDP_LOAD_DATETIME AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        1 AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_dataverse_int.optionsetmetadata
    """
    ) 
    
    return df

