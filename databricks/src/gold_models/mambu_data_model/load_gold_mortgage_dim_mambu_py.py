# Databricks notebook source
dbutils.widgets.text("catalog", "")
catalog_name = dbutils.widgets.get("catalog")

dbutils.widgets.text(name='env_var', defaultValue = 'dev')
env_var = dbutils.widgets.get("env_var")

dbutils.widgets.text(name='common_libs_lts_ver', defaultValue = '')
common_libs_lts_ver = dbutils.widgets.get("common_libs_lts_ver")

dbutils.widgets.text(name='common_libs_lts_ver_underscore', defaultValue = '')
common_libs_lts_ver_underscore = dbutils.widgets.get("common_libs_lts_ver_underscore")

mdp_databricks_common_library = f"/Volumes/{env_var}_catalog/common_libraries/mdp_databricks_common_{common_libs_lts_ver_underscore}/mdp_databricks_common-{common_libs_lts_ver}-py3-none-any.whl"

# COMMAND ----------

from mdp_databricks_common.dimensions.dimension_factory import DimensionFactory
from mdp_databricks_common.enums.enums import SCDType
from mdp_databricks_common.utils.time_utils import get_run_date

# COMMAND ----------

#get the run date from common libs
RUN_DATE = get_run_date()

# COMMAND ----------

# --------------------------
# dim_mortgage_account
# --------------------------
source_query = f"""
    WITH cte_original_term AS 
    (
        SELECT 
            ot.LINE_OF_CREDIT_KEY
            ,ot.ORIGINAL_TERM_LA 
        FROM
            (
            SELECT 
                mla.LINE_OF_CREDIT_KEY
                ,cfvp.ORIGINAL_TERM_LA
                ,ROW_NUMBER() OVER(PARTITION BY mla.LINE_OF_CREDIT_KEY ORDER BY cfvp.ORIGINAL_START_DATE_LA ASC,cfvp.ORIGINAL_TERM_LA DESC) AS RowNum
            FROM {catalog_name}.silver_con.mambu_loan_account mla
            INNER JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp
            ON mla.ENCODED_KEY = cfvp.PARENT_KEY 
            AND mla.ROW_IS_CURRENT = 1
            AND cfvp.ROW_IS_CURRENT = 1
            ) AS ot
        WHERE ot.RowNum = 1
    )

    SELECT
        CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT
        ,CAST(CONCAT(cfvp.ACCOUNT_NUMBER_CA, cfvp.ACCOUNT_SUFFIX_CA) AS STRING) AS ACCOUNT_NUMBER
        ,CAST(COALESCE(cfvp.SORT_CODE_CA, "Unknown") AS STRING) AS SORT_CODE
        ,CAST(COALESCE(mloc.STATE, "Unknown") AS STRING) AS ACCOUNT_STATUS
        ,CAST("Unknown" AS STRING) AS INTERMEDIARY
        ,CAST(COALESCE(cfvp.ORIGINATION_COST_CENTRE_CA, "Unknown") AS STRING) AS SALES_SOURCE
        ,CAST(COALESCE(ot.ORIGINAL_TERM_LA / 12, 0) AS TINYINT) AS ORIGINAL_TERM_YEARS
        ,CAST(COALESCE(ot.ORIGINAL_TERM_LA % 12, 0) AS TINYINT) AS ORIGINAL_TERM_MONTHS
        ,"MAMBU" AS SOURCE_SYSTEM
        ,mloc.MDP_LOAD_ID
        ,mloc.MDP_LOAD_DATETIME
        ,mloc.ROW_START_DATETIME
        ,mloc.ROW_END_DATETIME
        ,mloc.ROW_IS_CURRENT
    FROM {catalog_name}.silver_con.mambu_line_of_credit AS mloc
    INNER JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp
    ON mloc.ENCODED_KEY = cfvp.PARENT_KEY 
    AND mloc.ROW_IS_CURRENT = 1
    AND cfvp.ROW_IS_CURRENT = 1
    INNER JOIN cte_original_term AS ot
    ON mloc.ENCODED_KEY = ot.LINE_OF_CREDIT_KEY
"""

try:
    dim = DimensionFactory(catalog_name, 'gold_con','dim_mortgage_account',source_query).create(SCDType.TYPE2)
    dim.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)        

# --------------------------
# dim_mortgage_part
# --------------------------
source_query = f"""
    WITH cte_dim_mortgage_part AS 
    (
        SELECT 
            cfvp.PARENT_KEY,
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
                WHEN RANK1_BALLOON_DATE >= '{RUN_DATE}' THEN 1
                WHEN RANK2_BALLOON_DATE >= '{RUN_DATE}' THEN 2
                WHEN RANK3_BALLOON_DATE >= '{RUN_DATE}' THEN 3
                WHEN RANK4_BALLOON_DATE >= '{RUN_DATE}' THEN 4
                WHEN RANK5_BALLOON_DATE >= '{RUN_DATE}' THEN 5
                ELSE 1
            END AS CURRENT_RANK,
            CASE
                WHEN CURRENT_RANK = 1 THEN lps.RANK1_PRODUCT_TERM_GROUP
                WHEN CURRENT_RANK = 2 THEN lps.RANK2_PRODUCT_TERM_GROUP
                WHEN CURRENT_RANK = 3 THEN lps.RANK3_PRODUCT_TERM_GROUP
                WHEN CURRENT_RANK = 4 THEN lps.RANK4_PRODUCT_TERM_GROUP
                WHEN CURRENT_RANK = 5 THEN lps.RANK5_PRODUCT_TERM_GROUP
                ELSE 'Unknown'
            END AS BASIS_CATEGORY,
            CASE
                WHEN CURRENT_RANK = 1 THEN lps.RANK1_TYPE
                WHEN CURRENT_RANK = 2 THEN lps.RANK2_TYPE
                WHEN CURRENT_RANK = 3 THEN lps.RANK3_TYPE
                WHEN CURRENT_RANK = 4 THEN lps.RANK4_TYPE
                WHEN CURRENT_RANK = 5 THEN lps.RANK5_TYPE
                ELSE 'Unknown'
            END AS BASIS_TYPE
    FROM {catalog_name}.silver_con.mambu_custom_field_value_pivot cfvp
    LEFT JOIN {catalog_name}.silver_int.lps_product lps
    ON cfvp.PRODUCT_ID_LA = lps.PRODUCT_CODE
    WHERE cfvp.ROW_IS_CURRENT = 1
    AND lps.ROW_IS_CURRENT = 1
)

    SELECT
        CAST(mla.ID AS STRING) AS BK_MORTGAGE_PART,
        CAST(CONCAT(cfvp.ACCOUNT_NUMBER_LA, cfvp.ACCOUNT_SUFFIX_LA) AS STRING) AS ACCOUNT_NUMBER,
        CAST(COALESCE(cfvp.PART_NUMBER_LA, -2) AS TINYINT) AS PART_NUMBER,
        CAST(COALESCE(cfvp.SORT_CODE_LA, "Unknown") AS STRING) AS SORT_CODE,
        CAST("Unknown" AS STRING) AS ADVANCE_TYPE,
        CAST(COALESCE(cdmp.BASIS_CATEGORY, "Unknown") AS STRING) AS BASIS_CATEGORY,
        CAST(COALESCE(cdmp.BASIS_TYPE, "Unknown") AS STRING) AS BASIS_TYPE,
        CAST(COALESCE(cfvp.PURPOSE_CODE_LA, "Unknown") AS STRING) AS PURPOSE,
        CAST(COALESCE(mla.STATE, "Unknown") AS STRING) AS STATUS,
        CAST(COALESCE(mla.LOAN_NAME, "Unknown") AS STRING) AS REPAYMENT_TYPE,
        'MAMBU' AS SOURCE_SYSTEM,
        mla.MDP_LOAD_ID,
        mla.MDP_LOAD_DATETIME,
        mla.ROW_START_DATETIME,
        mla.ROW_END_DATETIME,
        mla.ROW_IS_CURRENT
    FROM {catalog_name}.silver_con.mambu_loan_account mla 
    LEFT JOIN cte_dim_mortgage_part cdmp 
    ON mla.ENCODED_KEY = cdmp.PARENT_KEY
    LEFT JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot cfvp
    ON mla.ENCODED_KEY = cfvp.PARENT_KEY
    WHERE mla.ROW_IS_CURRENT = 1
    AND cfvp.ROW_IS_CURRENT = 1
"""

try:
    dim = DimensionFactory(catalog_name, 'gold_con', 'dim_mortgage_part', source_query).create(SCDType.TYPE2)
    dim.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)    

# --------------------------
# dim_mortgage_transaction
# --------------------------
source_query = f"""
    WITH cte_transaction_payment_method AS (
        SELECT mtd.ENCODED_KEY AS TRANSACTION_DETAILS_ENCODED_KEY,
               mtc.NAME AS PAYMENT_METHOD
        FROM {catalog_name}.silver_int.mambu_transaction_details mtd
        INNER JOIN {catalog_name}.silver_int.mambu_transaction_channel mtc
            ON mtd.TRANSACTION_CHANNEL_KEY = mtc.ENCODED_KEY
        WHERE mtd.ROW_IS_CURRENT = 1
            AND mtc.ROW_IS_CURRENT = 1
    )
    SELECT CAST(xxhash64(CONCAT(COALESCE(mlt.TYPE, 'Unknown'), COALESCE(paym.PAYMENT_METHOD, 'Unknown'))) AS BIGINT) AS BK_MORTGAGE_TRANSACTION,
           COALESCE(paym.PAYMENT_METHOD, 'Unknown') AS PAYMENT_METHOD,            
           COALESCE(INITCAP(REPLACE(mlt.TYPE,'_',' ')), 'Unknown') AS TRANSACTION_TYPE,
           CAST((CASE WHEN mlt.TYPE IN ('DISBURSMENT', 'REPAYMENT', 'FEE', 'FEE_APPLIED', 'PENALTY_APPLIED', 'REPAYMENT_ADJUSTMENT', 'FEE_ADJUSTMENT', 'PENALTY_ADJUSTMENT', 'DEFERRED_INTEREST_APPLIED', 'DEFERRED_INTEREST_APPLIED_ADJUSTMENT', 'DEFERRED_INTEREST_PAID', 'DEFERRED_INTEREST_PAID_ADJUSTMENT', 'DISBURSEMENT_ADJUSTMENT', 'FEE_CHARGED', 'FEE_REDUCTION_ADJUSTMENT', 'FEES_DUE_REDUCED', 'INTEREST_APPLIED', 'INTEREST_APPLIED_ADJUSTMENT', 'INTEREST_DUE_REDUCED', 'INTEREST_REDUCTION_ADJUSTMENT', 'PENALTIES_DUE_REDUCED', 'PENALTY_REDUCTION_ADJUSTMENT', 'TRANSFER', 'TRANSFER_ADJUSTMENT', 'WRITE_OFF', 'WRITE_OFF_ADJUSTMENT', 'WITHDRAWAL_REDRAW', 'FEE_CAPITALISED', 'FEE_CAPITALISED_ADJUSTMENT', 'PAYMENT_MADE', 'REDRAW_REPAYMENT', 'PRINCIPAL_OVERPAYMENT', 'PRINCIPAL_OVERPAYMENT_ADJUSTMENT', 'WITHDRAWAL_REDRAW_ADJUSTMENT', 'IBF_INTEREST_APPLIED_ADJUSTMENT', 'REDUCE_BALANCE', 'IBF_INTEREST_APPLIED' ) THEN 'Yes' ELSE 'No' END) AS STRING) AS IS_FINANCIAL_TRANSACTION,
           CAST('MAMBU' AS STRING) AS SOURCE_SYSTEM,           
           MDP_LOAD_ID,
           MDP_LOAD_DATETIME,
           ROW_START_DATETIME,
           ROW_END_DATETIME,
           ROW_IS_CURRENT
    FROM {catalog_name}.silver_con.mambu_loan_transaction AS mlt
    LEFT OUTER JOIN cte_transaction_payment_method paym
        ON mlt.DETAILS_ENCODEDKEY_OID = paym.TRANSACTION_DETAILS_ENCODED_KEY
    WHERE mlt.ROW_IS_CURRENT = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(xxhash64(CONCAT(COALESCE(mlt.TYPE, 'Unknown'), COALESCE(paym.PAYMENT_METHOD, 'Unknown'))) AS BIGINT) ORDER BY ROW_START_DATETIME DESC) = 1
"""

try:
    dim = DimensionFactory(catalog_name, 'gold_con', 'dim_mortgage_transaction', source_query).create(SCDType.TYPE2)
    dim.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e) 

# --------------------------
# dim_mortgage_repayment
# --------------------------
source_query = f"""
    SELECT 
        CAST(xxhash64(COALESCE(STATUS, 'Unknown')) AS BIGINT) AS BK_MORTGAGE_REPAYMENT,
        COALESCE(STATUS, 'Unknown') AS STATUS,
        CAST('MAMBU' AS STRING) AS SOURCE_SYSTEM,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
    FROM {catalog_name}.silver_con.mambu_repayment
    WHERE ROW_IS_CURRENT = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(xxhash64(COALESCE(STATUS, 'Unknown')) AS BIGINT) ORDER BY ROW_START_DATETIME DESC) = 1
"""

try:
    dim = DimensionFactory(catalog_name, 'gold_con', 'dim_mortgage_repayment', source_query).create(SCDType.TYPE2)
    dim.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)