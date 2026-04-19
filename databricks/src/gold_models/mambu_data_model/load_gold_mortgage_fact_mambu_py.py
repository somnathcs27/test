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

dbutils.widgets.text("boe_base_rate", "")
boe_base_rate = float(dbutils.widgets.get("boe_base_rate"))/100

# COMMAND ----------

from mdp_databricks_common.facts.fact_factory import FactFactory
from mdp_databricks_common.enums.enums import FactType
from mdp_databricks_common.utils.time_utils import get_run_date

# COMMAND ----------

#get the run date from common libs
RUN_DATE = get_run_date()

# COMMAND ----------

# -------------------------
# fact_mortgage_account
# -------------------------
source_query = f"""
WITH cte_remaining_months AS (
    SELECT 
        mla.LINE_OF_CREDIT_KEY
        ,MAX(mla.MONTHS_BETWEEN_TODAY_AND_END_DATE) AS REMAINING_MONTHS
    FROM 
        (
            SELECT 
                mla.LINE_OF_CREDIT_KEY
                ,cfvp.ORIGINAL_START_DATE_LA
                ,cfvp.ORIGINAL_TERM_LA
                ,FLOOR(MONTHS_BETWEEN(ADD_MONTHS(TRY_CAST(cfvp.ORIGINAL_START_DATE_LA AS DATE), cfvp.ORIGINAL_TERM_LA), '{RUN_DATE}')) AS MONTHS_BETWEEN_TODAY_AND_END_DATE
            FROM {catalog_name}.silver_con.mambu_loan_account AS mla
            INNER JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp
            ON mla.ENCODED_KEY = cfvp.PARENT_KEY
            AND mla.ROW_IS_CURRENT = 1
        )mla
    GROUP BY mla.LINE_OF_CREDIT_KEY
)
,cte_advances_ytd AS (
    SELECT 
        mla.LINE_OF_CREDIT_KEY
        ,CAST(SUM(mlt.AMOUNT) AS DECIMAL(38,2)) AS ADVANCES_YTD
    FROM {catalog_name}.silver_con.mambu_loan_transaction AS mlt
    INNER JOIN {catalog_name}.silver_con.mambu_loan_account AS mla
    ON mlt.PARENT_ACCOUNT_KEY = mla.ENCODED_KEY
    AND mlt.TYPE IN ('DISBURSMENT') --only transaction types classified as an advance
    AND mlt.CREATION_DATE >= DATE_TRUNC('YEAR', '{RUN_DATE}') --all transactions in the current year
    AND mlt.ROW_IS_CURRENT = 1
    AND mla.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_int.mambu_transaction_details mtd
    ON mlt.DETAILS_ENCODEDKEY_OID = mtd.ENCODED_KEY
    AND mtd.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_int.mambu_transaction_channel mtc
    ON mtd.TRANSACTION_CHANNEL_KEY = mtc.ENCODED_KEY
    AND mtc.ROW_IS_CURRENT = 1
    WHERE COALESCE(mtc.NAME,'') <> 'Libra to Mambu Migration' --exclude migration transactions
    GROUP BY mla.LINE_OF_CREDIT_KEY
)
,cte_expected_payment_amount AS (
    SELECT rpm.PARENT_ACCOUNT_KEY, 
          (rpm.INTEREST_DUE +
           rpm.PRINCIPAL_DUE +
           rpm.FEES_DUE +
           rpm.PENALTY_DUE +
           rpm.TAX_INTEREST_DUE +
           rpm.TAX_FEES_DUE +
           rpm.TAX_PENALTY_DUE +
           rpm.ORGANIZATION_COMMISSION_DUE +
           rpm.FUNDERS_INTEREST_DUE) AS EXPECTED_PAYMENT_AMOUNT,
           rpm.DUE_DATE AS REGISTERED_PAYMENT_DATE
    FROM {catalog_name}.silver_con.mambu_repayment rpm
    -- Adjust the date to reflect the fact we're processing data from one day previous
    WHERE rpm.DUE_DATE >= DATE_ADD(CAST(FROM_UTC_TIMESTAMP(NOW(), 'Europe/London') AS DATE),-1)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rpm.PARENT_ACCOUNT_KEY ORDER BY rpm.DUE_DATE) = 1    
)
,cte_prepare_fact_mortgage_account AS (
    SELECT
         CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT
        ,CAST(mloc.ID AS BIGINT) AS BK_MORTGAGE_PROPERTY
        ,TRY_CAST(MAX(COALESCE(TO_DATE(cfvp_mloc.ORIGINAL_OPEN_DATE_CA),'1900-01-01')) AS DATE) AS FK_OPEN_DATE
        ,CAST('1900-01-01' AS DATE) AS FK_CLOSED_DATE 
        ,TRY_CAST(MAX(COALESCE(TO_DATE(epa.REGISTERED_PAYMENT_DATE),'1900-01-01')) AS DATE) AS FK_REGISTERED_PAYMENT_DATE
        ,CAST(SUM(mla.ACCRUED_INTEREST) AS DECIMAL(38,2)) AS ACCRUED_INTEREST
        ,CAST(SUM(aytd.ADVANCES_YTD) AS DECIMAL(38,2)) AS ADVANCES_YTD
        ,CAST(NULL AS DECIMAL(38,2)) AS ARREARS_BALANCE
        ,CAST(SUM(mla.PRINCIPAL_BALANCE) + SUM(mla.INTEREST_BALANCE) + SUM(mla.ACCRUED_INTEREST) AS DECIMAL(38,2)) AS CURRENT_BALANCE
        ,CAST(SUM(epa.EXPECTED_PAYMENT_AMOUNT) AS DECIMAL(38,2)) AS EXPECTED_PAYMENT_AMOUNT
        ,CAST(SUM(mla.PRINCIPAL_BALANCE) + SUM(mla.INTEREST_BALANCE) AS DECIMAL(38,2)) AS LEDGER_BALANCE
        -- 2025/06/16 - Current LTV can only be worked out with HPI data, once this is ingested into MDP we can update this placeholder
        ,CAST(NULL AS DECIMAL(38,6)) AS LOAN_TO_VALUE
        ,CAST(NULL AS DECIMAL(38,6)) AS ORIGINAL_LOAN_TO_VALUE
        ,CAST(MAX(TRY_DIVIDE(rm.REMAINING_MONTHS, 12)) AS TINYINT) AS REMAINING_YEARS
        ,CAST(MAX(rm.REMAINING_MONTHS % 12) AS TINYINT) AS REMAINING_MONTHS
        ,CAST(TRY_DIVIDE(SUM((mla.PRINCIPAL_BALANCE + mla.INTEREST_BALANCE + mla.ACCRUED_INTEREST) * mla.INTEREST_RATE),
            SUM(mla.PRINCIPAL_BALANCE + mla.INTEREST_BALANCE + mla.ACCRUED_INTEREST)) / 100 AS DECIMAL(38,6)) AS WEIGHTED_RATE
        ,CAST(MAX(meds_val.PROPERTY_PURCHASE_PRICE) AS DECIMAL(38,2)) AS PROPERTY_PURCHASE_PRICE
    FROM {catalog_name}.silver_con.mambu_line_of_credit AS mloc
    INNER JOIN {catalog_name}.silver_con.mambu_loan_account AS mla
    ON mloc.ENCODED_KEY = mla.LINE_OF_CREDIT_KEY
    AND mloc.ROW_IS_CURRENT = 1
    AND mla.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp_mloc
    ON mloc.ENCODED_KEY = cfvp_mloc.PARENT_KEY
    AND mloc.ROW_IS_CURRENT = 1
    AND cfvp_mloc.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp_mla
    ON mla.ENCODED_KEY = cfvp_mla.PARENT_KEY
    AND mla.ROW_IS_CURRENT = 1
    AND cfvp_mla.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_con.meds_valuation AS meds_val
    ON mloc.ID = meds_val.MORTGAGE_ACCOUNT_ID
    AND meds_val.ROW_IS_CURRENT = 1
    LEFT JOIN cte_remaining_months AS rm
    ON mloc.ENCODED_KEY = rm.LINE_OF_CREDIT_KEY
    LEFT JOIN cte_advances_ytd AS aytd
    ON mloc.ENCODED_KEY = aytd.LINE_OF_CREDIT_KEY
    LEFT JOIN cte_expected_payment_amount epa
    ON mla.ENCODED_KEY = epa.PARENT_ACCOUNT_KEY
    GROUP BY mloc.ID
)
    SELECT 
        CAST(xxhash64(cfma.BK_MORTGAGE_ACCOUNT) AS BIGINT) AS PK_FACT_MORTGAGE_ACCOUNT
        ,CASE
            WHEN dma.PK_MORTGAGE_ACCOUNT IS NOT NULL THEN dma.PK_MORTGAGE_ACCOUNT
            WHEN dma.PK_MORTGAGE_ACCOUNT IS NULL AND cfma.BK_MORTGAGE_ACCOUNT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_ACCOUNT
        ,CASE
            WHEN dmp.PK_MORTGAGE_PROPERTY IS NOT NULL THEN dmp.PK_MORTGAGE_PROPERTY
            WHEN dmp.PK_MORTGAGE_PROPERTY IS NULL AND cfma.BK_MORTGAGE_PROPERTY IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PROPERTY
        ,cfma.FK_OPEN_DATE
        ,cfma.FK_CLOSED_DATE
        ,cfma.FK_REGISTERED_PAYMENT_DATE            
        ,dma.ACCOUNT_NUMBER
        ,cfma.ACCRUED_INTEREST
        ,cfma.ADVANCES_YTD
        ,cfma.ARREARS_BALANCE
        ,cfma.CURRENT_BALANCE
        ,cfma.EXPECTED_PAYMENT_AMOUNT
        ,cfma.LEDGER_BALANCE
        ,cfma.LOAN_TO_VALUE
        ,cfma.ORIGINAL_LOAN_TO_VALUE
        ,cfma.REMAINING_YEARS
        ,cfma.REMAINING_MONTHS
        ,cfma.WEIGHTED_RATE
        ,cfma.PROPERTY_PURCHASE_PRICE        
    FROM cte_prepare_fact_mortgage_account AS cfma
    LEFT JOIN {catalog_name}.gold_con.dim_mortgage_account AS dma
    ON cfma.BK_MORTGAGE_ACCOUNT = dma.BK_MORTGAGE_ACCOUNT
    AND dma.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.gold_con.dim_mortgage_property AS dmp
    ON cfma.BK_MORTGAGE_PROPERTY = dmp.BK_MORTGAGE_PROPERTY
    AND dmp.ROW_IS_CURRENT = 1
"""

try:
    fact =  FactFactory(catalog_name, 'gold_con','fact_mortgage_account', source_query).create(FactType.FULL_LOAD)
    fact.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)

# COMMAND ----------

# -------------------------
# fact_mortgage_part
# -------------------------
source_query = f"""
WITH cte_expected_payment_amount AS (
    SELECT rpm.PARENT_ACCOUNT_KEY, (rpm.INTEREST_DUE +
                                rpm.PRINCIPAL_DUE +
                                rpm.FEES_DUE +
                                rpm.PENALTY_DUE +
                                rpm.TAX_INTEREST_DUE +
                                rpm.TAX_FEES_DUE +
                                rpm.TAX_PENALTY_DUE +
                                rpm.ORGANIZATION_COMMISSION_DUE +
                                rpm.FUNDERS_INTEREST_DUE) AS EXPECTED_PAYMENT_AMOUNT
    FROM {catalog_name}.silver_con.mambu_repayment rpm
    -- Adjust the date to reflect the fact we're processing data from one day previous
    WHERE rpm.DUE_DATE >= DATE_ADD(CAST(FROM_UTC_TIMESTAMP(NOW(), 'Europe/London') AS DATE),-1)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rpm.PARENT_ACCOUNT_KEY ORDER BY rpm.DUE_DATE) = 1    
)

,cte_interest_rate_setting AS (
    SELECT ACCOUNT_KEY, COALESCE(INTEREST_SPREAD,0) AS INTEREST_SPREAD
    FROM {catalog_name}.silver_int.mambu_account_interest_rate_settings
    WHERE ROW_IS_CURRENT = 1
      AND VALID_FROM_DATE <= '{RUN_DATE}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_KEY ORDER BY VALID_FROM_DATE DESC) = 1    
)

,cte_prepare_fact_mortgage_part AS (
    SELECT
        CAST(mla.ID AS STRING) AS BK_MORTGAGE_PART
        ,CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT  
        ,CAST(cfvp.PRODUCT_ID_LA AS STRING) AS BK_MORTGAGE_PRODUCT                 
        ,CASE 
            WHEN cfvp.ORIGINAL_START_DATE_LA IS NULL THEN '1900-01-01'
            ELSE COALESCE(TRY_CAST(cfvp.ORIGINAL_START_DATE_LA AS DATE),'1899-12-31')
        END AS FK_OPEN_DATE
        ,CAST(COALESCE(mla.CLOSED_DATE,'1900-01-01') AS DATE) AS FK_CLOSED_DATE
        ,CAST('1900-01-01' AS DATE) AS FK_PRODUCT_START_DATE           
        ,CAST(mla.ACCRUED_INTEREST AS DECIMAL(38,2)) AS ACCRUED_INTEREST            
        ,CAST(NULL AS DECIMAL(38,2)) AS ARREARS_BALANCE
        ,CAST((mla.PRINCIPAL_BALANCE + mla.INTEREST_BALANCE + mla.ACCRUED_INTEREST) AS DECIMAL(38,2)) AS CURRENT_BALANCE
        ,CAST((mla.PRINCIPAL_BALANCE + mla.INTEREST_BALANCE) AS DECIMAL(38,2)) AS LEDGER_BALANCE
        ,CAST((mla.INTEREST_RATE + COALESCE(irs.INTEREST_SPREAD,0))/100 AS DECIMAL(38,6)) AS CUSTOMER_RATE
        ,CAST(epa.EXPECTED_PAYMENT_AMOUNT AS DECIMAL(38,2)) AS EXPECTED_PAYMENT_AMOUNT
        ,CAST(lps.LIQUIDITY_TERM_PREMIUM AS DECIMAL(38,6)) AS LIQUIDITY_TERM_PREMIUM
        ,CAST(mla.INTEREST_RATE/100 AS DECIMAL(38,6)) AS INTEREST_RATE
        ,mla.LINE_OF_CREDIT_KEY
        ,FLOOR(MONTHS_BETWEEN(ADD_MONTHS(TRY_CAST(cfvp.ORIGINAL_START_DATE_LA AS DATE), cfvp.ORIGINAL_TERM_LA), '{RUN_DATE}')) AS REMAINING_MONTHS
        ,CASE 
            WHEN lps.RANK1_END_DATE IS NOT NULL THEN lps.RANK1_END_DATE
            ELSE ADD_MONTHS(TRY_CAST(cfvp.ORIGINAL_START_DATE_LA AS DATE), lps.RANK1_FOR_MONTHS) --product start date / penalty start date 
        END AS RANK1_BALLOON_DATE
        ,CASE 
            WHEN lps.RANK2_END_DATE IS NOT NULL THEN lps.RANK2_END_DATE
            ELSE ADD_MONTHS(RANK1_BALLOON_DATE, lps.RANK2_FOR_MONTHS)
        END AS RANK2_BALLOON_DATE
        ,CASE
            WHEN lps.RANK3_END_DATE IS NOT NULL THEN lps.RANK3_END_DATE
            ELSE ADD_MONTHS(RANK2_BALLOON_DATE, lps.RANK3_FOR_MONTHS)
        END AS RANK3_BALLOON_DATE
        ,CASE
            WHEN lps.RANK4_END_DATE IS NOT NULL THEN lps.RANK4_END_DATE
            ELSE ADD_MONTHS(RANK3_BALLOON_DATE, lps.RANK4_FOR_MONTHS)
        END AS RANK4_BALLOON_DATE
        ,CASE
            WHEN lps.RANK5_END_DATE IS NOT NULL THEN lps.RANK5_END_DATE
            ELSE ADD_MONTHS(RANK4_BALLOON_DATE, lps.RANK5_FOR_MONTHS)
        END AS RANK5_BALLOON_DATE 
        ,CASE
            WHEN RANK1_BALLOON_DATE >= '{RUN_DATE}' THEN RANK1_BALLOON_DATE 
            WHEN RANK2_BALLOON_DATE >= '{RUN_DATE}' THEN RANK2_BALLOON_DATE
            WHEN RANK3_BALLOON_DATE >= '{RUN_DATE}' THEN RANK3_BALLOON_DATE
            WHEN RANK4_BALLOON_DATE >= '{RUN_DATE}' THEN RANK4_BALLOON_DATE
            WHEN RANK5_BALLOON_DATE >= '{RUN_DATE}' THEN RANK5_BALLOON_DATE
            ELSE '1900-01-01'
        END AS FK_NEXT_BALLOON_DATE
        ,CASE
            WHEN RANK1_BALLOON_DATE >= '{RUN_DATE}' THEN 1
            WHEN RANK2_BALLOON_DATE >= '{RUN_DATE}' THEN 2
            WHEN RANK3_BALLOON_DATE >= '{RUN_DATE}' THEN 3
            WHEN RANK4_BALLOON_DATE >= '{RUN_DATE}' THEN 4
            WHEN RANK5_BALLOON_DATE >= '{RUN_DATE}' THEN 5
            ELSE 1
        END AS CURRENT_RANK        
        ,CAST(
            CASE
                WHEN CURRENT_RANK = 1 AND lps.RANK1_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 2 AND lps.RANK2_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 3 AND lps.RANK3_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 4 AND lps.RANK4_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 5 AND lps.RANK5_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                ELSE ({boe_base_rate} + (brl.RATE/100)) --Bank of England base rate injected from a parameter
            END AS DECIMAL(38,6)) AS BENCHMARK_RATE
        ,CASE
            WHEN lps.RANK5_FOR_MONTHS IS NOT NULL OR lps.RANK5_END_DATE IS NOT NULL THEN 6
            WHEN lps.RANK4_FOR_MONTHS IS NOT NULL OR lps.RANK4_END_DATE IS NOT NULL THEN 5
            WHEN lps.RANK3_FOR_MONTHS IS NOT NULL OR lps.RANK3_END_DATE IS NOT NULL THEN 4
            WHEN lps.RANK2_FOR_MONTHS IS NOT NULL OR lps.RANK2_END_DATE IS NOT NULL THEN 3
            WHEN lps.RANK1_FOR_MONTHS IS NOT NULL OR lps.RANK1_END_DATE IS NOT NULL THEN 2
            ELSE 1
        END AS FINAL_RANK
       ,cfvp.ORIGINAL_ADVANCE_AMOUNT_LA AS ORIGINAL_ADVANCE
    FROM {catalog_name}.silver_con.mambu_loan_account AS mla
    INNER JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp
    ON mla.ENCODED_KEY = cfvp.PARENT_KEY
    AND mla.ROW_IS_CURRENT = 1
    AND cfvp.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_con.mambu_line_of_credit AS mloc
    ON mla.LINE_OF_CREDIT_KEY = mloc.ENCODED_KEY
    AND mloc.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_int.lps_product AS lps
    ON cfvp.PRODUCT_ID_LA = lps.PRODUCT_CODE
    AND cfvp.ROW_IS_CURRENT = 1
    AND lps.ROW_IS_CURRENT = 1
    LEFT JOIN cte_interest_rate_setting AS irs
    ON irs.ACCOUNT_KEY = mla.ENCODED_KEY
    LEFT JOIN {catalog_name}.silver_int.reference_data_base_rate_loading AS brl
    ON brl.END_DATE >= '{RUN_DATE}'
    AND brl.ROW_IS_CURRENT = 1
    LEFT JOIN cte_expected_payment_amount epa
    ON mla.ENCODED_KEY = epa.PARENT_ACCOUNT_KEY
)
    SELECT
        CAST(xxhash64(cfmp.BK_MORTGAGE_PART) AS BIGINT) AS PK_FACT_MORTGAGE_PART
        ,CASE
            WHEN dmp.PK_MORTGAGE_PART IS NOT NULL THEN dmp.PK_MORTGAGE_PART
            WHEN dmp.PK_MORTGAGE_PART IS NULL AND cfmp.BK_MORTGAGE_PART IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PART
        ,CASE
            WHEN dma.PK_MORTGAGE_ACCOUNT IS NOT NULL THEN dma.PK_MORTGAGE_ACCOUNT
            WHEN dma.PK_MORTGAGE_ACCOUNT IS NULL AND cfmp.BK_MORTGAGE_ACCOUNT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_ACCOUNT
        ,CASE
            WHEN product.PK_MORTGAGE_PRODUCT IS NOT NULL THEN product.PK_MORTGAGE_PRODUCT
            WHEN product.PK_MORTGAGE_PRODUCT IS NULL AND cfmp.BK_MORTGAGE_PRODUCT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PRODUCT
        ,CASE
            WHEN dmp_rank_current.PK_MORTGAGE_PRODUCT_RANK IS NOT NULL THEN dmp_rank_current.PK_MORTGAGE_PRODUCT_RANK
            WHEN dmp_rank_current.PK_MORTGAGE_PRODUCT_RANK IS NULL AND cfmp.BK_MORTGAGE_PRODUCT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PRODUCT_CURRENT_RANK
        ,CASE
            WHEN dmp_rank_final.PK_MORTGAGE_PRODUCT_RANK IS NOT NULL THEN dmp_rank_final.PK_MORTGAGE_PRODUCT_RANK
            WHEN dmp_rank_final.PK_MORTGAGE_PRODUCT_RANK IS NULL AND cfmp.BK_MORTGAGE_PRODUCT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PRODUCT_FINAL_RANK
        ,cfmp.FK_NEXT_BALLOON_DATE 
        ,cfmp.FK_OPEN_DATE
        ,cfmp.FK_CLOSED_DATE
        ,cfmp.FK_PRODUCT_START_DATE
        ,dmp.ACCOUNT_NUMBER
        ,dmp.PART_NUMBER
        ,cfmp.ACCRUED_INTEREST
        ,cfmp.ARREARS_BALANCE
        ,cfmp.BENCHMARK_RATE
        ,cfmp.CURRENT_BALANCE
        ,cfmp.CURRENT_RANK
        ,cfmp.CUSTOMER_RATE
        ,cfmp.EXPECTED_PAYMENT_AMOUNT
        ,cfmp.FINAL_RANK
        ,cfmp.LEDGER_BALANCE
        ,cfmp.LIQUIDITY_TERM_PREMIUM
        ,cfmp.INTEREST_RATE    
        ,CAST((cfmp.REMAINING_MONTHS / 12) AS TINYINT) AS REMAINING_YEARS
        ,CAST((cfmp.REMAINING_MONTHS % 12) AS TINYINT) AS REMAINING_MONTHS
        ,cfmp.ORIGINAL_ADVANCE
    FROM cte_prepare_fact_mortgage_part AS cfmp
    LEFT JOIN {catalog_name}.gold_con.dim_mortgage_part AS dmp 
    ON (cfmp.BK_MORTGAGE_PART = dmp.BK_MORTGAGE_PART AND dmp.ROW_IS_CURRENT = 1)
    LEFT JOIN {catalog_name}.gold_int.dim_mortgage_product AS product
    ON (cfmp.BK_MORTGAGE_PRODUCT = product.BK_MORTGAGE_PRODUCT AND product.ROW_IS_CURRENT = 1)
    LEFT JOIN {catalog_name}.gold_con.dim_mortgage_account AS dma 
    ON (cfmp.BK_MORTGAGE_ACCOUNT = dma.BK_MORTGAGE_ACCOUNT AND dma.ROW_IS_CURRENT = 1)
    LEFT JOIN {catalog_name}.gold_int.dim_mortgage_product_rank AS dmp_rank_current
    ON (cfmp.BK_MORTGAGE_PRODUCT = dmp_rank_current.BK_MORTGAGE_PRODUCT_RANK AND dmp_rank_current.ROW_IS_CURRENT = 1 
    AND dmp_rank_current.RANK = cfmp.current_rank)
    LEFT JOIN {catalog_name}.gold_int.dim_mortgage_product_rank AS dmp_rank_final
    ON (cfmp.BK_MORTGAGE_PRODUCT = dmp_rank_final.BK_MORTGAGE_PRODUCT_RANK AND dmp_rank_final.ROW_IS_CURRENT = 1 
    AND dmp_rank_final.RANK = cfmp.final_rank)
"""

try:
    fact = FactFactory(catalog_name, 'gold_con','fact_mortgage_part', source_query).create(FactType.FULL_LOAD)
    fact.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)

# COMMAND ----------

# -------------------------
# fact_mortgage_part_test
# -------------------------
source_query = f"""
WITH cte_expected_payment_amount AS (
    SELECT rpm.PARENT_ACCOUNT_KEY, (rpm.INTEREST_DUE +
                                rpm.PRINCIPAL_DUE +
                                rpm.FEES_DUE +
                                rpm.PENALTY_DUE +
                                rpm.TAX_INTEREST_DUE +
                                rpm.TAX_FEES_DUE +
                                rpm.TAX_PENALTY_DUE +
                                rpm.ORGANIZATION_COMMISSION_DUE +
                                rpm.FUNDERS_INTEREST_DUE) AS EXPECTED_PAYMENT_AMOUNT
    FROM {catalog_name}.silver_con.mambu_repayment rpm
    -- Adjust the date to reflect the fact we're processing data from one day previous
    WHERE rpm.DUE_DATE >= DATE_ADD(CAST(FROM_UTC_TIMESTAMP(NOW(), 'Europe/London') AS DATE),-1)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rpm.PARENT_ACCOUNT_KEY ORDER BY rpm.DUE_DATE) = 1    
)

,cte_interest_rate_setting AS (
    SELECT ACCOUNT_KEY, COALESCE(INTEREST_SPREAD,0) AS INTEREST_SPREAD
    FROM {catalog_name}.silver_int.mambu_account_interest_rate_settings
    WHERE ROW_IS_CURRENT = 1
      AND VALID_FROM_DATE <= '{RUN_DATE}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_KEY ORDER BY VALID_FROM_DATE DESC) = 1    
)

,cte_prepare_fact_mortgage_part AS (
    SELECT
        CAST(mla.ID AS STRING) AS BK_MORTGAGE_PART
        ,CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT  
        ,CAST(cfvp.PRODUCT_ID_LA AS STRING) AS BK_MORTGAGE_PRODUCT                 
        ,CASE 
            WHEN cfvp.ORIGINAL_START_DATE_LA IS NULL THEN '1900-01-01'
            ELSE COALESCE(TRY_CAST(cfvp.ORIGINAL_START_DATE_LA AS DATE),'1899-12-31')
        END AS FK_OPEN_DATE
        ,CAST(COALESCE(mla.CLOSED_DATE,'1900-01-01') AS DATE) AS FK_CLOSED_DATE
        ,CAST('1900-01-01' AS DATE) AS FK_PRODUCT_START_DATE           
        ,CAST(mla.ACCRUED_INTEREST AS DECIMAL(38,2)) AS ACCRUED_INTEREST            
        ,CAST(NULL AS DECIMAL(38,2)) AS ARREARS_BALANCE
        ,CAST((mla.PRINCIPAL_BALANCE + mla.INTEREST_BALANCE + mla.ACCRUED_INTEREST) AS DECIMAL(38,2)) AS CURRENT_BALANCE
        ,CAST((mla.PRINCIPAL_BALANCE + mla.INTEREST_BALANCE) AS DECIMAL(38,2)) AS LEDGER_BALANCE
        ,CAST((mla.INTEREST_RATE + COALESCE(irs.INTEREST_SPREAD,0))/100 AS DECIMAL(38,6)) AS CUSTOMER_RATE
        ,CAST(epa.EXPECTED_PAYMENT_AMOUNT AS DECIMAL(38,2)) AS EXPECTED_PAYMENT_AMOUNT
        ,CAST(lps.LIQUIDITY_TERM_PREMIUM AS DECIMAL(38,6)) AS LIQUIDITY_TERM_PREMIUM
        ,CAST(mla.INTEREST_RATE/100 AS DECIMAL(38,6)) AS INTEREST_RATE
        ,mla.LINE_OF_CREDIT_KEY
        ,FLOOR(MONTHS_BETWEEN(ADD_MONTHS(TRY_CAST(cfvp.ORIGINAL_START_DATE_LA AS DATE), cfvp.ORIGINAL_TERM_LA), '{RUN_DATE}')) AS REMAINING_MONTHS
        ,CASE
            WHEN lps.RANK5_FOR_MONTHS IS NOT NULL OR lps.RANK5_END_DATE IS NOT NULL THEN 6
            WHEN lps.RANK4_FOR_MONTHS IS NOT NULL OR lps.RANK4_END_DATE IS NOT NULL THEN 5
            WHEN lps.RANK3_FOR_MONTHS IS NOT NULL OR lps.RANK3_END_DATE IS NOT NULL THEN 4
            WHEN lps.RANK2_FOR_MONTHS IS NOT NULL OR lps.RANK2_END_DATE IS NOT NULL THEN 3
            WHEN lps.RANK1_FOR_MONTHS IS NOT NULL OR lps.RANK1_END_DATE IS NOT NULL THEN 2
            ELSE 1
        END AS FINAL_RANK
       ,cfvp.ORIGINAL_ADVANCE_AMOUNT_LA AS ORIGINAL_ADVANCE
    FROM {catalog_name}.silver_con.mambu_loan_account AS mla
    INNER JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp
    ON mla.ENCODED_KEY = cfvp.PARENT_KEY
    AND mla.ROW_IS_CURRENT = 1
    AND cfvp.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_con.mambu_line_of_credit AS mloc
    ON mla.LINE_OF_CREDIT_KEY = mloc.ENCODED_KEY
    AND mloc.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_int.lps_product AS lps
    ON cfvp.PRODUCT_ID_LA = lps.PRODUCT_CODE
    AND cfvp.ROW_IS_CURRENT = 1
    AND lps.ROW_IS_CURRENT = 1
    LEFT JOIN cte_interest_rate_setting AS irs
    ON irs.ACCOUNT_KEY = mla.ENCODED_KEY
    LEFT JOIN cte_expected_payment_amount epa
    ON mla.ENCODED_KEY = epa.PARENT_ACCOUNT_KEY
)
,cte_transformed_fields AS (
    SELECT 
        FK_FACT_MORTGAGE_PART,
        RANK1_BALLOON_DATE,
        RANK2_BALLOON_DATE,
        RANK3_BALLOON_DATE,
        RANK4_BALLOON_DATE,
        RANK5_BALLOON_DATE,
        FK_NEXT_BALLOON_DATE,
        CURRENT_RANK,
        BENCHMARK_RATE,
        BK_MORTGAGE_PRODUCT,
        TYPE,
        RANK_START_DATE,
        RANK_END_DATE
    FROM {env_var}_catalog.silver_con.mview_mambu_transformed_fields
    WHERE ROW_IS_CURRENT = 1
)
    SELECT
        CAST(xxhash64(cfmp.BK_MORTGAGE_PART) AS BIGINT) AS PK_FACT_MORTGAGE_PART
        ,CASE
            WHEN dmp.PK_MORTGAGE_PART IS NOT NULL THEN dmp.PK_MORTGAGE_PART
            WHEN dmp.PK_MORTGAGE_PART IS NULL AND cfmp.BK_MORTGAGE_PART IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PART
        ,CASE
            WHEN dma.PK_MORTGAGE_ACCOUNT IS NOT NULL THEN dma.PK_MORTGAGE_ACCOUNT
            WHEN dma.PK_MORTGAGE_ACCOUNT IS NULL AND cfmp.BK_MORTGAGE_ACCOUNT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_ACCOUNT
        ,CASE
            WHEN product.PK_MORTGAGE_PRODUCT IS NOT NULL THEN product.PK_MORTGAGE_PRODUCT
            WHEN product.PK_MORTGAGE_PRODUCT IS NULL AND cfmp.BK_MORTGAGE_PRODUCT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PRODUCT
        ,CASE
            WHEN dmp_rank_current.PK_MORTGAGE_PRODUCT_RANK IS NOT NULL THEN dmp_rank_current.PK_MORTGAGE_PRODUCT_RANK
            WHEN dmp_rank_current.PK_MORTGAGE_PRODUCT_RANK IS NULL AND cfmp.BK_MORTGAGE_PRODUCT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PRODUCT_CURRENT_RANK
        ,CASE
            WHEN dmp_rank_final.PK_MORTGAGE_PRODUCT_RANK IS NOT NULL THEN dmp_rank_final.PK_MORTGAGE_PRODUCT_RANK
            WHEN dmp_rank_final.PK_MORTGAGE_PRODUCT_RANK IS NULL AND cfmp.BK_MORTGAGE_PRODUCT IS NOT NULL THEN -1
            ELSE -2
        END AS FK_MORTGAGE_PRODUCT_FINAL_RANK
        ,tf.FK_NEXT_BALLOON_DATE 
        ,cfmp.FK_OPEN_DATE
        ,cfmp.FK_CLOSED_DATE
        ,cfmp.FK_PRODUCT_START_DATE
        ,dmp.ACCOUNT_NUMBER
        ,dmp.PART_NUMBER
        ,cfmp.ACCRUED_INTEREST
        ,cfmp.ARREARS_BALANCE
        ,tf.BENCHMARK_RATE
        ,cfmp.CURRENT_BALANCE
        ,tf.CURRENT_RANK
        ,cfmp.CUSTOMER_RATE
        ,cfmp.EXPECTED_PAYMENT_AMOUNT
        ,cfmp.FINAL_RANK
        ,cfmp.LEDGER_BALANCE
        ,cfmp.LIQUIDITY_TERM_PREMIUM
        ,cfmp.INTEREST_RATE    
        ,CAST((cfmp.REMAINING_MONTHS / 12) AS TINYINT) AS REMAINING_YEARS
        ,CAST((cfmp.REMAINING_MONTHS % 12) AS TINYINT) AS REMAINING_MONTHS
        ,cfmp.ORIGINAL_ADVANCE
    FROM cte_prepare_fact_mortgage_part AS cfmp
    LEFT JOIN cte_transformed_fields AS tf
    ON CAST(xxhash64(cfmp.BK_MORTGAGE_PART) AS BIGINT) = tf.FK_FACT_MORTGAGE_PART
    LEFT JOIN {catalog_name}.gold_con.dim_mortgage_part AS dmp 
    ON (cfmp.BK_MORTGAGE_PART = dmp.BK_MORTGAGE_PART AND dmp.ROW_IS_CURRENT = 1)
    LEFT JOIN {catalog_name}.gold_int.dim_mortgage_product AS product
    ON (cfmp.BK_MORTGAGE_PRODUCT = product.BK_MORTGAGE_PRODUCT AND product.ROW_IS_CURRENT = 1)
    LEFT JOIN {catalog_name}.gold_con.dim_mortgage_account AS dma 
    ON (cfmp.BK_MORTGAGE_ACCOUNT = dma.BK_MORTGAGE_ACCOUNT AND dma.ROW_IS_CURRENT = 1)
    LEFT JOIN {catalog_name}.gold_int.dim_mortgage_product_rank AS dmp_rank_current
    ON (cfmp.BK_MORTGAGE_PRODUCT = dmp_rank_current.BK_MORTGAGE_PRODUCT_RANK AND dmp_rank_current.ROW_IS_CURRENT = 1 
    AND dmp_rank_current.RANK = tf.CURRENT_RANK)
    LEFT JOIN {catalog_name}.gold_int.dim_mortgage_product_rank AS dmp_rank_final
    ON (cfmp.BK_MORTGAGE_PRODUCT = dmp_rank_final.BK_MORTGAGE_PRODUCT_RANK AND dmp_rank_final.ROW_IS_CURRENT = 1 
    AND dmp_rank_final.RANK = cfmp.FINAL_RANK)
"""

try:
    fact = FactFactory(catalog_name, 'gold_con','fact_mortgage_part_test', source_query).create(FactType.FULL_LOAD)
    fact.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)

# COMMAND ----------

# -------------------------
# fact_mortgage_transaction
# -------------------------
source_query = f"""
WITH cte_transaction_payment_method AS (
    SELECT
        mtd.ENCODED_KEY AS TRANSACTION_DETAILS_ENCODED_KEY,
        mtc.NAME AS PAYMENT_METHOD
    FROM {catalog_name}.silver_int.mambu_transaction_details AS mtd
    INNER JOIN {catalog_name}.silver_int.mambu_transaction_channel AS mtc
    ON mtd.TRANSACTION_CHANNEL_KEY = mtc.ENCODED_KEY
    WHERE mtd.ROW_IS_CURRENT = 1
    AND mtc.ROW_IS_CURRENT = 1
),

cte_mambu_custom_fields AS (
    SELECT 
        mla.ENCODED_KEY AS ACCOUNT_KEY,
        mla.LINE_OF_CREDIT_KEY,
        mla.ID,
        CAST(cfvp.PRODUCT_ID_LA AS STRING) AS MORTGAGE_PRODUCT_CODE  
    FROM {catalog_name}.silver_con.mambu_loan_account AS mla
    INNER JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp
    ON mla.ENCODED_KEY = cfvp.PARENT_KEY
    WHERE mla.ROW_IS_CURRENT = 1
    AND cfvp.ROW_IS_CURRENT = 1
),

cte_mambu_benchmark_rate AS (
    SELECT PRODUCT_CODE, RANK, RANK_START_DATE, RANK_END_DATE, TYPE, BENCHMARK_RATE
    FROM (
    SELECT PRODUCT_CODE, CAST(1 AS TINYINT) AS RANK, COALESCE(LAUNCH_DATE, CAST('1900-01-01' AS DATE)) AS RANK_START_DATE, RANK1_END_DATE AS RANK_END_DATE, TYPE, BENCHMARK_RATE
    FROM {catalog_name}.silver_int.lps_product
    WHERE ROW_IS_CURRENT = 1
        AND RANK1_END_DATE > COALESCE(LAUNCH_DATE, CAST('1900-01-01' AS DATE))
    UNION
    SELECT PRODUCT_CODE, CAST(2 AS TINYINT) AS RANK, RANK1_END_DATE AS RANK_START_DATE, RANK2_END_DATE AS RANK_END_DATE, TYPE, BENCHMARK_RATE
    FROM {catalog_name}.silver_int.lps_product
    WHERE ROW_IS_CURRENT = 1
        AND RANK2_END_DATE IS NOT NULL
        AND RANK2_END_DATE > RANK1_END_DATE
    UNION
    SELECT PRODUCT_CODE, CAST(3 AS TINYINT) AS RANK, RANK2_END_DATE AS RANK_START_DATE, RANK3_END_DATE AS RANK_END_DATE, TYPE, BENCHMARK_RATE
    FROM {catalog_name}.silver_int.lps_product 
    WHERE ROW_IS_CURRENT = 1
        AND RANK3_END_DATE IS NOT NULL
        AND RANK3_END_DATE > RANK2_END_DATE
    UNION
    SELECT PRODUCT_CODE, CAST(4 AS TINYINT) AS RANK, RANK3_END_DATE AS RANK_START_DATE, RANK4_END_DATE AS RANK_END_DATE, TYPE, BENCHMARK_RATE
    FROM {catalog_name}.silver_int.lps_product 
    WHERE ROW_IS_CURRENT = 1
        AND RANK4_END_DATE IS NOT NULL
        AND RANK4_END_DATE > RANK3_END_DATE
    UNION
    SELECT PRODUCT_CODE, CAST(5 AS TINYINT) AS RANK, RANK4_END_DATE AS RANK_START_DATE, RANK5_END_DATE AS RANK_END_DATE, TYPE, BENCHMARK_RATE
    FROM {catalog_name}.silver_int.lps_product
    WHERE ROW_IS_CURRENT = 1
        AND RANK5_END_DATE IS NOT NULL 
        AND RANK5_END_DATE > RANK4_END_DATE ) x
),

cte_interest_rate_setting AS (
    SELECT ACCOUNT_KEY, COALESCE(INTEREST_SPREAD,0) AS INTEREST_SPREAD
    FROM {catalog_name}.silver_int.mambu_account_interest_rate_settings
    WHERE ROW_IS_CURRENT = 1
      AND VALID_FROM_DATE <= '{RUN_DATE}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_KEY ORDER BY VALID_FROM_DATE DESC) = 1    
),

cte_prepare_fact_mortgage_transaction AS (
    SELECT
        CAST(xxhash64(mlt.ENCODED_KEY) AS BIGINT) AS PK_FACT_MORTGAGE_TRANSACTION,
        CAST(xxhash64(CONCAT(COALESCE(mlt.TYPE, 'Unknown'), COALESCE(paym.PAYMENT_METHOD, 'Unknown'))) AS BIGINT) AS BK_MORTGAGE_TRANSACTION,
        CAST(cfs.ID AS STRING) AS BK_MORTGAGE_PART,
        CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT,
        COALESCE(mlt.ENTRY_DATE, CAST('1900-01-01' AS DATE)) AS FK_BUSINESS_DATE,
        COALESCE(mlt.CREATION_DATE, CAST('1900-01-01' AS DATE)) AS FK_PROCESSED_DATE,
        mlt.TRANSACTION_ID,
        -- [2025/05/22] - The benchmark logic is redundant but at present we don't have either reliable BoE rate history or a loading dim so can't derive a non fixed value
        CAST(
            CASE 
                WHEN bmr.TYPE = 'Fixed' THEN bmr.BENCHMARK_RATE
                ELSE ({boe_base_rate} + (brl.RATE/100)) --Bank of England base rate injected from a parameter
            END AS DECIMAL(38,6)) AS BENCHMARK_RATE,
        CAST((mla.INTEREST_RATE + irs.INTEREST_SPREAD)/100 AS DECIMAL(38,6)) AS CUSTOMER_RATE,
        CAST(prod.LIQUIDITY_TERM_PREMIUM AS DECIMAL(38,6)) AS LIQUIDITY_TERM_PREMIUM,              
        CASE 
            WHEN mlt.TYPE IN ('REPAYMENT', 'REPAYMENT_ADJUSTMENT', 'DEFERRED_INTEREST_PAID', 'DEFERRED_INTEREST_PAID_ADJUSTMENT', 'WRITE_OFF', 'WRITE_OFF_ADJUSTMENT') THEN CAST(mlt.AMOUNT AS DECIMAL(38,2))
            ELSE NULL
        END AS RECEIPT_AMOUNT,
        CASE 
            WHEN mlt.TYPE IN ('DISBURSMENT', 'DISBURSMENT_ADJUSTMENT', 'INTEREST_APPLIED', 'INTEREST_APPLIED_ADJUSTMENT', 'DEFERRED_INTEREST_APPLIED', 'FEE', 'FEE_ADJUSTMENT', 'FEE_CHARGED', 'FEE_APPLIED', 'PENALTY_APPLIED', 'PENALTY_ADJUSTMENT', 'DEFERRED_INTEREST_APPLIED_ADJUSTMENT', 'TRANSFER', 'TRANSFER_ADJUSTMENT') THEN CAST(mlt.AMOUNT AS DECIMAL(38,2))
            ELSE NULL
        END AS WITHDRAWAL_AMOUNT
    FROM {catalog_name}.silver_con.mambu_loan_transaction AS mlt
    LEFT JOIN {catalog_name}.silver_con.mambu_loan_account mla
    ON mlt.PARENT_ACCOUNT_KEY = mla.ENCODED_KEY
    AND mla.ROW_IS_CURRENT = 1
    LEFT JOIN cte_transaction_payment_method AS paym
    ON mlt.DETAILS_ENCODEDKEY_OID = paym.TRANSACTION_DETAILS_ENCODED_KEY
    LEFT JOIN cte_mambu_custom_fields AS cfs
    ON mlt.PARENT_ACCOUNT_KEY = cfs.ACCOUNT_KEY
    LEFT JOIN {catalog_name}.silver_int.lps_product AS prod 
    ON cfs.MORTGAGE_PRODUCT_CODE = prod.PRODUCT_CODE
    LEFT JOIN cte_mambu_benchmark_rate AS bmr
    ON prod.PRODUCT_CODE = bmr.PRODUCT_CODE
    AND mlt.CREATION_DATE >= bmr.RANK_START_DATE 
    AND mlt.CREATION_DATE < bmr.RANK_END_DATE
    LEFT JOIN {catalog_name}.silver_con.mambu_line_of_credit AS mloc
    ON cfs.LINE_OF_CREDIT_KEY = mloc.ENCODED_KEY
    AND mloc.ROW_IS_CURRENT = 1
    LEFT JOIN cte_interest_rate_setting AS irs
    ON irs.ACCOUNT_KEY = cfs.ACCOUNT_KEY
    LEFT JOIN {catalog_name}.silver_int.reference_data_base_rate_loading AS brl
    ON brl.END_DATE >= '{RUN_DATE}'
    AND brl.ROW_IS_CURRENT = 1    
    WHERE mlt.ROW_IS_CURRENT = 1
)
SELECT 
    cfmt.PK_FACT_MORTGAGE_TRANSACTION,
    CASE 
        WHEN dmt.PK_MORTGAGE_TRANSACTION IS NOT NULL THEN dmt.PK_MORTGAGE_TRANSACTION
        WHEN dmt.PK_MORTGAGE_TRANSACTION IS NULL AND cfmt.BK_MORTGAGE_TRANSACTION IS NOT NULL THEN -1
        ELSE -2 
    END AS FK_MORTGAGE_TRANSACTION,                
    CASE 
        WHEN dmp.PK_MORTGAGE_PART IS NOT NULL THEN dmp.PK_MORTGAGE_PART
        WHEN dmp.PK_MORTGAGE_PART IS NULL AND cfmt.BK_MORTGAGE_PART IS NOT NULL THEN -1
        ELSE -2 
    END AS FK_MORTGAGE_PART,
    CASE 
        WHEN dma.PK_MORTGAGE_ACCOUNT IS NOT NULL THEN dma.PK_MORTGAGE_ACCOUNT
        WHEN dma.PK_MORTGAGE_ACCOUNT IS NULL AND cfmt.BK_MORTGAGE_ACCOUNT IS NOT NULL THEN -1
        ELSE -2 
    END AS FK_MORTGAGE_ACCOUNT,                 
    cfmt.FK_BUSINESS_DATE,
    cfmt.FK_PROCESSED_DATE,
    cfmt.TRANSACTION_ID,
    dmp.ACCOUNT_NUMBER,
    dmp.PART_NUMBER,        
    CAST(cfmt.BENCHMARK_RATE AS DECIMAL(38,6)) AS BENCHMARK_RATE,
    CAST(cfmt.CUSTOMER_RATE AS DECIMAL(38,6)) AS CUSTOMER_RATE,
    CAST(cfmt.LIQUIDITY_TERM_PREMIUM AS DECIMAL(38,6)) AS LIQUIDITY_TERM_PREMIUM,
    CAST(cfmt.RECEIPT_AMOUNT AS DECIMAL(38,2)) AS RECEIPT_AMOUNT,
    CAST(cfmt.WITHDRAWAL_AMOUNT AS DECIMAL(38,2)) AS WITHDRAWAL_AMOUNT
FROM cte_prepare_fact_mortgage_transaction AS cfmt
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_transaction AS dmt 
ON cfmt.BK_MORTGAGE_TRANSACTION = dmt.BK_MORTGAGE_TRANSACTION
AND dmt.ROW_IS_CURRENT = 1
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_part AS dmp
ON cfmt.BK_MORTGAGE_PART = dmp.BK_MORTGAGE_PART
AND dmp.ROW_IS_CURRENT = 1
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_account AS dma
ON cfmt.BK_MORTGAGE_ACCOUNT = dma.BK_MORTGAGE_ACCOUNT
AND dma.ROW_IS_CURRENT = 1
"""

try:
    fact = FactFactory(catalog_name, 'gold_con','fact_mortgage_transaction', source_query).create(FactType.FULL_LOAD)
    fact.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)

# COMMAND ----------

# -------------------------
# fact_mortgage_transaction_test
# -------------------------
source_query = f"""
WITH cte_transaction_payment_method AS (
    SELECT
        mtd.ENCODED_KEY AS TRANSACTION_DETAILS_ENCODED_KEY,
        mtc.NAME AS PAYMENT_METHOD
    FROM {catalog_name}.silver_int.mambu_transaction_details AS mtd
    INNER JOIN {catalog_name}.silver_int.mambu_transaction_channel AS mtc
    ON mtd.TRANSACTION_CHANNEL_KEY = mtc.ENCODED_KEY
    WHERE mtd.ROW_IS_CURRENT = 1
    AND mtc.ROW_IS_CURRENT = 1
),

cte_mambu_custom_fields AS (
    SELECT 
        mla.ENCODED_KEY AS ACCOUNT_KEY,
        mla.LINE_OF_CREDIT_KEY,
        mla.ID,
        CAST(cfvp.PRODUCT_ID_LA AS STRING) AS MORTGAGE_PRODUCT_CODE  
    FROM {catalog_name}.silver_con.mambu_loan_account AS mla
    INNER JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp
    ON mla.ENCODED_KEY = cfvp.PARENT_KEY
    WHERE mla.ROW_IS_CURRENT = 1
    AND cfvp.ROW_IS_CURRENT = 1
),

cte_mambu_benchmark_rate AS (
    SELECT 
        BK_MORTGAGE_PRODUCT AS PRODUCT_CODE,
        CURRENT_RANK AS RANK,
        RANK_START_DATE,
        RANK_END_DATE,
        TYPE,
        BENCHMARK_RATE
    FROM {env_var}_catalog.silver_con.mview_mambu_transformed_fields
    WHERE ROW_IS_CURRENT = 1
),

cte_interest_rate_setting AS (
    SELECT ACCOUNT_KEY, COALESCE(INTEREST_SPREAD,0) AS INTEREST_SPREAD
    FROM {catalog_name}.silver_int.mambu_account_interest_rate_settings
    WHERE ROW_IS_CURRENT = 1
      AND VALID_FROM_DATE <= '{RUN_DATE}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_KEY ORDER BY VALID_FROM_DATE DESC) = 1    
),

cte_prepare_fact_mortgage_transaction AS (
    SELECT
        CAST(xxhash64(mlt.ENCODED_KEY) AS BIGINT) AS PK_FACT_MORTGAGE_TRANSACTION,
        CAST(xxhash64(CONCAT(COALESCE(mlt.TYPE, 'Unknown'), COALESCE(paym.PAYMENT_METHOD, 'Unknown'))) AS BIGINT) AS BK_MORTGAGE_TRANSACTION,
        CAST(cfs.ID AS STRING) AS BK_MORTGAGE_PART,
        CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT,
        COALESCE(mlt.ENTRY_DATE, CAST('1900-01-01' AS DATE)) AS FK_BUSINESS_DATE,
        COALESCE(mlt.CREATION_DATE, CAST('1900-01-01' AS DATE)) AS FK_PROCESSED_DATE,
        mlt.TRANSACTION_ID,
        CAST(
            CASE 
                WHEN bmr.TYPE = 'Fixed' THEN bmr.BENCHMARK_RATE
                ELSE ({boe_base_rate} + (brl.RATE/100))
            END AS DECIMAL(38,6)) AS BENCHMARK_RATE,
        CAST((mla.INTEREST_RATE + irs.INTEREST_SPREAD)/100 AS DECIMAL(38,6)) AS CUSTOMER_RATE,
        CAST(prod.LIQUIDITY_TERM_PREMIUM AS DECIMAL(38,6)) AS LIQUIDITY_TERM_PREMIUM,              
        CASE 
            WHEN mlt.TYPE IN ('REPAYMENT', 'REPAYMENT_ADJUSTMENT', 'DEFERRED_INTEREST_PAID', 'DEFERRED_INTEREST_PAID_ADJUSTMENT', 'WRITE_OFF', 'WRITE_OFF_ADJUSTMENT') THEN CAST(mlt.AMOUNT AS DECIMAL(38,2))
            ELSE NULL
        END AS RECEIPT_AMOUNT,
        CASE 
            WHEN mlt.TYPE IN ('DISBURSMENT', 'DISBURSMENT_ADJUSTMENT', 'INTEREST_APPLIED', 'INTEREST_APPLIED_ADJUSTMENT', 'DEFERRED_INTEREST_APPLIED', 'FEE', 'FEE_ADJUSTMENT', 'FEE_CHARGED', 'FEE_APPLIED', 'PENALTY_APPLIED', 'PENALTY_ADJUSTMENT', 'DEFERRED_INTEREST_APPLIED_ADJUSTMENT', 'TRANSFER', 'TRANSFER_ADJUSTMENT') THEN CAST(mlt.AMOUNT AS DECIMAL(38,2))
            ELSE NULL
        END AS WITHDRAWAL_AMOUNT
    FROM {catalog_name}.silver_con.mambu_loan_transaction AS mlt
    LEFT JOIN {catalog_name}.silver_con.mambu_loan_account mla
    ON mlt.PARENT_ACCOUNT_KEY = mla.ENCODED_KEY
    AND mla.ROW_IS_CURRENT = 1
    LEFT JOIN cte_transaction_payment_method AS paym
    ON mlt.DETAILS_ENCODEDKEY_OID = paym.TRANSACTION_DETAILS_ENCODED_KEY
    LEFT JOIN cte_mambu_custom_fields AS cfs
    ON mlt.PARENT_ACCOUNT_KEY = cfs.ACCOUNT_KEY
    LEFT JOIN {catalog_name}.silver_int.lps_product AS prod 
    ON cfs.MORTGAGE_PRODUCT_CODE = prod.PRODUCT_CODE
    LEFT JOIN cte_mambu_benchmark_rate AS bmr
    ON prod.PRODUCT_CODE = bmr.PRODUCT_CODE
    AND mlt.CREATION_DATE >= bmr.RANK_START_DATE 
    AND mlt.CREATION_DATE < bmr.RANK_END_DATE
    LEFT JOIN {catalog_name}.silver_con.mambu_line_of_credit AS mloc
    ON cfs.LINE_OF_CREDIT_KEY = mloc.ENCODED_KEY
    AND mloc.ROW_IS_CURRENT = 1
    LEFT JOIN cte_interest_rate_setting AS irs
    ON irs.ACCOUNT_KEY = cfs.ACCOUNT_KEY
    LEFT JOIN {catalog_name}.silver_int.reference_data_base_rate_loading AS brl
    ON brl.END_DATE >= '{RUN_DATE}'
    AND brl.ROW_IS_CURRENT = 1    
    WHERE mlt.ROW_IS_CURRENT = 1
)
SELECT 
    cfmt.PK_FACT_MORTGAGE_TRANSACTION,
    CASE 
        WHEN dmt.PK_MORTGAGE_TRANSACTION IS NOT NULL THEN dmt.PK_MORTGAGE_TRANSACTION
        WHEN dmt.PK_MORTGAGE_TRANSACTION IS NULL AND cfmt.BK_MORTGAGE_TRANSACTION IS NOT NULL THEN -1
        ELSE -2 
    END AS FK_MORTGAGE_TRANSACTION,                
    CASE 
        WHEN dmp.PK_MORTGAGE_PART IS NOT NULL THEN dmp.PK_MORTGAGE_PART
        WHEN dmp.PK_MORTGAGE_PART IS NULL AND cfmt.BK_MORTGAGE_PART IS NOT NULL THEN -1
        ELSE -2 
    END AS FK_MORTGAGE_PART,
    CASE 
        WHEN dma.PK_MORTGAGE_ACCOUNT IS NOT NULL THEN dma.PK_MORTGAGE_ACCOUNT
        WHEN dma.PK_MORTGAGE_ACCOUNT IS NULL AND cfmt.BK_MORTGAGE_ACCOUNT IS NOT NULL THEN -1
        ELSE -2 
    END AS FK_MORTGAGE_ACCOUNT,                 
    cfmt.FK_BUSINESS_DATE,
    cfmt.FK_PROCESSED_DATE,
    cfmt.TRANSACTION_ID,
    dmp.ACCOUNT_NUMBER,
    dmp.PART_NUMBER,        
    CAST(cfmt.BENCHMARK_RATE AS DECIMAL(38,6)) AS BENCHMARK_RATE,
    CAST(cfmt.CUSTOMER_RATE AS DECIMAL(38,6)) AS CUSTOMER_RATE,
    CAST(cfmt.LIQUIDITY_TERM_PREMIUM AS DECIMAL(38,6)) AS LIQUIDITY_TERM_PREMIUM,
    CAST(cfmt.RECEIPT_AMOUNT AS DECIMAL(38,2)) AS RECEIPT_AMOUNT,
    CAST(cfmt.WITHDRAWAL_AMOUNT AS DECIMAL(38,2)) AS WITHDRAWAL_AMOUNT
FROM cte_prepare_fact_mortgage_transaction AS cfmt
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_transaction AS dmt 
ON cfmt.BK_MORTGAGE_TRANSACTION = dmt.BK_MORTGAGE_TRANSACTION
AND dmt.ROW_IS_CURRENT = 1
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_part AS dmp
ON cfmt.BK_MORTGAGE_PART = dmp.BK_MORTGAGE_PART
AND dmp.ROW_IS_CURRENT = 1
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_account AS dma
ON cfmt.BK_MORTGAGE_ACCOUNT = dma.BK_MORTGAGE_ACCOUNT
AND dma.ROW_IS_CURRENT = 1
"""

try:
    fact = FactFactory(catalog_name, 'gold_con','fact_mortgage_transaction_test', source_query).create(FactType.FULL_LOAD)
    fact.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)

# COMMAND ----------

# -------------------------
# fact_mortgage_repayment
# -------------------------
source_query = f"""
WITH 
cte_prepare_fact_mortgage_repayment AS (
    SELECT
        CAST(xxhash64(rpm.ENCODED_KEY) AS BIGINT) AS PK_FACT_MORTGAGE_REPAYMENT,
        CAST(xxhash64(COALESCE(rpm.STATUS, 'Unknown')) AS BIGINT) AS BK_MORTGAGE_REPAYMENT,
        CAST(mla.ID AS STRING) AS BK_MORTGAGE_PART,
        CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT,
        CAST((
            rpm.INTEREST_DUE +
            rpm.PRINCIPAL_DUE +
            rpm.FEES_DUE +
            rpm.PENALTY_DUE +
            rpm.FUNDERS_INTEREST_DUE +
            rpm.ORGANIZATION_COMMISSION_DUE) AS DECIMAL(38,2)) AS AMOUNT,
        COALESCE(rpm.DUE_DATE, CAST('1900-01-01' AS DATE)) AS FK_DUE_DATE,
        COALESCE(rpm.LAST_PAID_DATE, CAST('1900-01-01' AS DATE)) AS FK_LAST_PAID_DATE,
        COALESCE(rpm.REPAID_DATE, CAST('1900-01-01' AS DATE)) AS FK_REPAID_DATE,
        CAST(rpm.INTEREST_DUE AS DECIMAL(38,2)) AS INTEREST_DUE,
        CAST(rpm.PRINCIPAL_DUE AS DECIMAL(38,2)) AS PRINCIPAL_DUE,
        CAST(rpm.FEES_DUE AS DECIMAL(38,2)) AS FEES_DUE,
        CAST(rpm.PENALTY_DUE AS DECIMAL(38,2)) AS PENALTY_DUE,
        CAST(rpm.FUNDERS_INTEREST_DUE AS DECIMAL(38,2)) AS FUNDERS_INTEREST_DUE,
        CAST(rpm.ORGANIZATION_COMMISSION_DUE AS DECIMAL(38,2)) AS ORGANIZATION_COMMISSION_DUE
    FROM {catalog_name}.silver_con.mambu_repayment rpm
    INNER JOIN {catalog_name}.silver_con.mambu_loan_account mla
    ON rpm.PARENT_ACCOUNT_KEY = mla.ENCODED_KEY
    AND mla.ROW_IS_CURRENT = 1
    LEFT JOIN {catalog_name}.silver_con.mambu_line_of_credit AS mloc
    ON mla.LINE_OF_CREDIT_KEY = mloc.ENCODED_KEY
    AND mloc.ROW_IS_CURRENT = 1
    WHERE rpm.ROW_IS_CURRENT = 1
)

SELECT
    cfmr.PK_FACT_MORTGAGE_REPAYMENT,
    CASE
        WHEN dmr.PK_MORTGAGE_REPAYMENT IS NOT NULL THEN dmr.PK_MORTGAGE_REPAYMENT
        WHEN dmr.PK_MORTGAGE_REPAYMENT IS NULL AND cfmr.BK_MORTGAGE_REPAYMENT IS NOT NULL THEN -1
        ELSE -2 
    END AS FK_MORTGAGE_REPAYMENT,
    CASE 
        WHEN dmp.PK_MORTGAGE_PART IS NOT NULL THEN dmp.PK_MORTGAGE_PART
        WHEN dmp.PK_MORTGAGE_PART IS NULL AND cfmr.BK_MORTGAGE_PART IS NOT NULL THEN -1
        ELSE -2 
    END AS FK_MORTGAGE_PART,
    CASE
        WHEN dma.PK_MORTGAGE_ACCOUNT IS NOT NULL THEN dma.PK_MORTGAGE_ACCOUNT
        WHEN dma.PK_MORTGAGE_ACCOUNT IS NULL AND cfmr.BK_MORTGAGE_ACCOUNT IS NOT NULL THEN -1
        ELSE -2
    END AS FK_MORTGAGE_ACCOUNT,
    cfmr.FK_DUE_DATE,
    cfmr.FK_LAST_PAID_DATE,
    cfmr.FK_REPAID_DATE,
    dmp.ACCOUNT_NUMBER,
    dmp.PART_NUMBER,
    cfmr.AMOUNT,
    cfmr.INTEREST_DUE,
    cfmr.PRINCIPAL_DUE,
    cfmr.FEES_DUE,
    cfmr.PENALTY_DUE,
    cfmr.FUNDERS_INTEREST_DUE,
    cfmr.ORGANIZATION_COMMISSION_DUE  
FROM cte_prepare_fact_mortgage_repayment cfmr
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_repayment as dmr
ON cfmr.BK_MORTGAGE_REPAYMENT = dmr.BK_MORTGAGE_REPAYMENT
AND dmr.ROW_IS_CURRENT = 1
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_part AS dmp
ON cfmr.BK_MORTGAGE_PART = dmp.BK_MORTGAGE_PART
AND dmp.ROW_IS_CURRENT = 1
LEFT JOIN {catalog_name}.gold_con.dim_mortgage_account AS dma 
ON cfmr.BK_MORTGAGE_ACCOUNT = dma.BK_MORTGAGE_ACCOUNT 
AND dma.ROW_IS_CURRENT = 1
"""

try:
    fact = FactFactory(catalog_name, 'gold_con','fact_mortgage_repayment', source_query).create(FactType.FULL_LOAD)
    fact.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)
