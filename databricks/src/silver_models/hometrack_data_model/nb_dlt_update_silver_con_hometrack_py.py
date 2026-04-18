# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------


@dlt.table(
    name = "hometrack_automated_valuation_model_update",
    comment = "Quarterly update of AVM data for the LBS mortgage portfolio content is based on the LBS request file sent to Hometrack"
)
@dlt.expect("REFERENCE is not null", "REFERENCE IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_sftp_output():

    df = spark.sql(f"""
    SELECT
        CAST(bh.Prop_0 AS BIGINT) AS REFERENCE,
        bh.Prop_1 AS ADDRESS,
        bh.Prop_2 AS ADDRESS_LINE_1,
        bh.Prop_3 AS ADDRESS_LINE_2,
        bh.Prop_4 AS ADDRESS_LINE_3,
        bh.Prop_5 AS ADDRESS_LINE_4,
        bh.Prop_6 AS ADDRESS_LINE_5,
        bh.Prop_7 AS POSTCODE,
        bh.Prop_8 AS PROP_TYPE,
        bh.Prop_9 AS PROP_STYLE,
        CAST(bh.Prop_10 AS INT) AS BEDROOMS,
        CAST(bh.Prop_11 AS DECIMAL(38,2)) AS PREVIOUS_VALUE,
        CAST(bh.Prop_12 AS DATE) AS PREVIOUS_VALUE_DATE,
        CAST(bh.Prop_13 AS DECIMAL(38,2)) AS ORIGINAL_LOAN_BALANCE,
        CAST(bh.Prop_14 AS DECIMAL(38,2)) AS CURRENT_LOAN_BALANCE,
        CAST(bh.Prop_15 AS DATE) AS DTM_TARGET_DATE,
        CAST(bh.Prop_16 AS DATE) AS REALTIME_VALUATION_DATE,
        CAST(bh.Prop_17 AS DECIMAL(38,2)) AS REALTIME_VALUATION,
        CAST(bh.Prop_18 AS DECIMAL(18,2)) AS REALTIME_CONFIDENCE_LEVEL,
        bh.Prop_19 AS REALTIME_MESSAGE,
        CAST(bh.Prop_20 AS DECIMAL(38,2)) AS RENTAL_VALUATION,
        CAST(bh.Prop_21 AS DECIMAL(18,2)) AS RENTAL_CL,
        bh.Prop_22 AS RENTAL_MESSAGE,
        CAST(bh.Prop_23 AS INT) AS HOMETRACK_DATA_BEDROOMS,
        CAST(bh.Prop_24 AS INT) AS HOMETRACK_DATA_PROP_TYPE,
        CAST(bh.Prop_25 AS INT) AS HOMETRACK_DATA_SUB_PROP_TYPE,
        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

        CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_sftp_con.output bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("REFERENCE", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df
