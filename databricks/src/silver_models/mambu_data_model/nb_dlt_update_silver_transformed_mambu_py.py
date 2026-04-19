# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window
import mdp_databricks_common.silver_layer_functions as dlf
from mdp_databricks_common.feature_toggle import feature_toggle as ft

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

from mdp_databricks_common.utils.time_utils import get_run_date

# COMMAND ----------

#get the run date from common libs
RUN_DATE = get_run_date()

# COMMAND ----------

@dlt.materialized_view(
    name="mview_mambu_transformed_fields", 
    comment="each action that takes place in the application is followed by an activity that is logged and posted on the dashboard and on the activity feed"
)
@dlt.expect("Check that LINE_OF_CREDIT_KEY is not null", "LINE_OF_CREDIT_KEY IS NOT NULL")
def mview_mambu_transformed_fields():

    df = spark.sql(
        f"""
    SELECT
        CAST(mla.ID AS STRING) AS PK_VW_MAMBU_TRANSFORMED_FIELDS,
        CAST(xxhash64(mla.ID) AS BIGINT) AS FK_FACT_MORTGAGE_PART,
        CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT,  
        CAST(cfvp.PRODUCT_ID_LA AS STRING) AS BK_MORTGAGE_PRODUCT,   
        mloc.ENCODED_KEY AS LINE_OF_CREDIT_KEY,
        CASE 
            WHEN lps.RANK1_END_DATE IS NOT NULL THEN lps.RANK1_END_DATE
            ELSE ADD_MONTHS(TRY_CAST(cfvp.ORIGINAL_START_DATE_LA AS DATE), lps.RANK1_FOR_MONTHS) 
        END AS RANK1_BALLOON_DATE,
        CASE 
            WHEN lps.RANK2_END_DATE IS NOT NULL THEN lps.RANK2_END_DATE
            ELSE ADD_MONTHS(RANK1_BALLOON_DATE, lps.RANK2_FOR_MONTHS)
        END AS RANK2_BALLOON_DATE,
        CASE
            WHEN lps.RANK3_END_DATE IS NOT NULL THEN lps.RANK3_END_DATE
            ELSE ADD_MONTHS(RANK2_BALLOON_DATE, lps.RANK3_FOR_MONTHS)
        END AS RANK3_BALLOON_DATE,
        CASE
            WHEN lps.RANK4_END_DATE IS NOT NULL THEN lps.RANK4_END_DATE
            ELSE ADD_MONTHS(RANK3_BALLOON_DATE, lps.RANK4_FOR_MONTHS)
        END AS RANK4_BALLOON_DATE,
        CASE
            WHEN lps.RANK5_END_DATE IS NOT NULL THEN lps.RANK5_END_DATE
            ELSE ADD_MONTHS(RANK4_BALLOON_DATE, lps.RANK5_FOR_MONTHS)
        END AS RANK5_BALLOON_DATE,
        CASE
            WHEN RANK1_BALLOON_DATE >= '{RUN_DATE}' THEN RANK1_BALLOON_DATE 
            WHEN RANK2_BALLOON_DATE >= '{RUN_DATE}' THEN RANK2_BALLOON_DATE
            WHEN RANK3_BALLOON_DATE >= '{RUN_DATE}' THEN RANK3_BALLOON_DATE
            WHEN RANK4_BALLOON_DATE >= '{RUN_DATE}' THEN RANK4_BALLOON_DATE
            WHEN RANK5_BALLOON_DATE >= '{RUN_DATE}' THEN RANK5_BALLOON_DATE
            ELSE '1900-01-01'
        END AS FK_NEXT_BALLOON_DATE,
        CASE
            WHEN RANK1_BALLOON_DATE >= '{RUN_DATE}' THEN 1
            WHEN RANK2_BALLOON_DATE >= '{RUN_DATE}' THEN 2
            WHEN RANK3_BALLOON_DATE >= '{RUN_DATE}' THEN 3
            WHEN RANK4_BALLOON_DATE >= '{RUN_DATE}' THEN 4
            WHEN RANK5_BALLOON_DATE >= '{RUN_DATE}' THEN 5
            ELSE 1
        END AS CURRENT_RANK,
        CAST(
            CASE
                WHEN CURRENT_RANK = 1 AND lps.RANK1_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 2 AND lps.RANK2_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 3 AND lps.RANK3_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 4 AND lps.RANK4_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 5 AND lps.RANK5_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                ELSE (0.05 + (brl.RATE/100))
            END AS DECIMAL(38,6)) AS BENCHMARK_RATE,
        CASE
            WHEN CURRENT_RANK = 1 THEN lps.RANK1_TYPE
            WHEN CURRENT_RANK = 2 THEN lps.RANK2_TYPE
            WHEN CURRENT_RANK = 3 THEN lps.RANK3_TYPE
            WHEN CURRENT_RANK = 4 THEN lps.RANK4_TYPE
            WHEN CURRENT_RANK = 5 THEN lps.RANK5_TYPE
        END AS TYPE,
        CASE
            WHEN CURRENT_RANK = 1 THEN COALESCE(lps.LAUNCH_DATE, CAST('1900-01-01' AS DATE))
            WHEN CURRENT_RANK = 2 THEN lps.RANK1_END_DATE
            WHEN CURRENT_RANK = 3 THEN lps.RANK2_END_DATE
            WHEN CURRENT_RANK = 4 THEN lps.RANK3_END_DATE
            WHEN CURRENT_RANK = 5 THEN lps.RANK4_END_DATE
        END AS RANK_START_DATE,
        CASE
            WHEN CURRENT_RANK = 1 THEN lps.RANK1_END_DATE
            WHEN CURRENT_RANK = 2 THEN lps.RANK2_END_DATE
            WHEN CURRENT_RANK = 3 THEN lps.RANK3_END_DATE
            WHEN CURRENT_RANK = 4 THEN lps.RANK4_END_DATE
            WHEN CURRENT_RANK = 5 THEN lps.RANK5_END_DATE
        END AS RANK_END_DATE,
        mla.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.mambu_loan_account AS mla
    INNER JOIN {env_var}_catalog.silver_con.mambu_custom_field_value_pivot AS cfvp
    ON mla.ENCODED_KEY = cfvp.PARENT_KEY
    AND mla.ROW_IS_CURRENT = 1
    AND cfvp.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.silver_int.lps_product AS lps
    ON cfvp.PRODUCT_ID_LA = lps.PRODUCT_CODE
    AND cfvp.ROW_IS_CURRENT = 1
    AND lps.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.silver_con.mambu_line_of_credit AS mloc
    ON mla.LINE_OF_CREDIT_KEY = mloc.ENCODED_KEY
    AND mloc.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.silver_int.reference_data_base_rate_loading AS brl
    ON brl.END_DATE >= '{RUN_DATE}'
    AND brl.ROW_IS_CURRENT = 1
    """
    )
    
    # windowSpec = dlf.get_window_spec("LINE_OF_CREDIT_KEY","ROW_START_DATETIME")   
    # df = dlf.get_row_number(df, windowSpec)   

    return df