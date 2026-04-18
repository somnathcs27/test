# Databricks notebook source
import dlt

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "fact_saving_account_derived_data_test",
    comment = "fact saving accout with the necessary transformations" 
)
def gold_fact_saving_account_derived_data_test():
    # Execute SQL query to select and transform data
    query = f"""

        SELECT 
            SA.SAVING_ACCOUNT_ENCODED_KEY, 
            SA.SAVING_ACCOUNT_HOLDER_KEY, 
            SA.SAVING_ACCOUNT_PRODUCT_TYPE_KEY,
            SP.SAVING_PRODUCT_ENCODED_KEY,
            SA.SAVING_ACCOUNT_ID,
            SA.SAVING_ACCOUNT_APPROVED_DATE,
            SA.SAVING_ACCOUNT_ACTIVATION_DATE,
            COALESCE(SA.SAVING_ACCOUNT_BALANCE,0) AS CAPITAL_LEDGER_BALANCE,
            COALESCE(SA.SAVING_ACCOUNT_BALANCE,0) - (COALESCE(SA.SAVING_ACCOUNT_BLOCKED_BALANCE,0) + COALESCE(SA.SAVING_ACCOUNT_LOCKED_BALANCE,0)) AS CAPITAL_AVAILABLE_BALANCE,
            COALESCE(SA.SAVING_ACCOUNT_BALANCE,0) - COALESCE(ENTRIES.FUTURE_AMOUNT, 0) AS REP_CAPITAL_LEDGER_BALANCE,
            SA.SAVING_ACCRUED_INTEREST AS ACCRUED_INTEREST_GROSS,
            COALESCE(TAX_INFO.TAX_RATE, 0) AS TAX_RATE,
            COALESCE(((SA.SAVING_ACCRUED_INTEREST) * TAX_INFO.TAX_RATE / 100), 0) AS ACCRUED_INTEREST_TAX,
            COALESCE(((SA.SAVING_ACCRUED_INTEREST) - COALESCE((SA.SAVING_ACCRUED_INTEREST),0) * TAX_INFO.TAX_RATE / 100), 0) AS ACCRUED_INTEREST_NET,
            COALESCE(SA.SAVING_ACCOUNT_BLOCKED_BALANCE,0) AS SAVING_ACCOUNT_BLOCKED_BALANCE,
            COALESCE(SA.SAVING_ACCOUNT_LOCKED_BALANCE,0) AS SAVING_ACCOUNT_LOCKED_BALANCE,
            1 AS SAVING_ACCOUNT_COUNT

        FROM {env_var}_catalog.data_quality.saving_account_test_data SA
        LEFT JOIN (
            SELECT
                ST.SAVING_PARENT_ACCOUNT_KEY AS ACCOUNT_KEY,
                COALESCE(IR.SAVING_INDEX_RATE_VALUE,0) AS TAX_RATE
            FROM {env_var}_catalog.data_quality.saving_transaction_test_data AS ST
            LEFT JOIN {env_var}_catalog.data_quality.saving_index_rate_test_data AS IR
                ON ST.TAX_RATE_ENCODEDKEY_OID = IR.SAVING_INDEX_RATE_ENCODED_KEY
            LEFT JOIN {env_var}_catalog.data_quality.saving_index_rate_source_test_data AS IRS
                ON IR.SAVING_INDEX_RATE_SOURCE_ENCODED_KEY_OID = IRS.SAVING_INDEX_RATE_SOURCE_ENCODED_KEY
            WHERE IRS.SAVING_INDEX_RATE_SOURCE_TYPE IN ('TAX_RATE', 'WITHHOLDING_TAX_RATE')
        ) AS TAX_INFO
            ON SA.SAVING_ACCOUNT_ENCODED_KEY = TAX_INFO.ACCOUNT_KEY
        LEFT JOIN ( 
            SELECT 
                SAVING_PARENT_ACCOUNT_KEY AS ACCOUNT_KEY, 
                SUM(CASE WHEN CREATION_DATE > CURRENT_DATE THEN SAVING_TRANSACTION_AMOUNT ELSE 0 END) AS FUTURE_AMOUNT 
            FROM {env_var}_catalog.data_quality.saving_transaction_test_data
            WHERE SAVING_TRANSACTION_TYPE IN ('DEPOSIT', 'WITHDRAWAL') 
            GROUP BY SAVING_PARENT_ACCOUNT_KEY 
        ) AS ENTRIES 
            ON SA.SAVING_ACCOUNT_ENCODED_KEY = ENTRIES.ACCOUNT_KEY 
        LEFT JOIN {env_var}_catalog.data_quality.saving_product_test_data AS SP
            ON SA.SAVING_ACCOUNT_PRODUCT_TYPE_KEY = SP.SAVING_PRODUCT_ENCODED_KEY
        LEFT JOIN {env_var}_catalog.data_quality.saving_index_rate_source_test_data AS INXR
            ON SA.SAVING_ACCOUNT_WITHHOLDING_TAX_SOURCE_KEY = INXR.SAVING_INDEX_RATE_SOURCE_ENCODED_KEY;

            """
    df = spark.sql(query)

    return df


# COMMAND ----------

@dlt.table(
    name = "fact_saving_transaction_derived_data_test",
    comment = "fact saving transaction with the necessary transformations" 
)
def gold_fact_saving_transaction_derived_data_test():
    
    # Execute SQL query to select and transform data
    query = f"""
    SELECT ST.SAVING_TRANSACTION_ENCODED_KEY,
          ST.PRODUCT_TYPE_KEY,
          ST.SAVING_PARENT_ACCOUNT_KEY,
          ST.USER_KEY,
          PD.SAVING_TRANSACTION_KEY,
          ST.TRANSACTION_ID,
          ST.CREATION_DATE,
          COALESCE(ST.SAVING_TRANSACTION_BALANCE,0) AS SAVING_TRANSACTION_BALANCE,
          COALESCE(ST.SAVING_TRANSACTION_AMOUNT,0) AS SAVING_TRANSACTION_AMOUNT,
          COALESCE(ST.SAVING_TRANSACTION_INTEREST_AMOUNT,0) AS SAVING_TRANSACTION_INTEREST_AMOUNT,
          COALESCE(ST.SAVING_TRANSACTION_INTEREST_RATE,0) AS SAVING_TRANSACTION_INTEREST_RATE,
          COALESCE(ST.SAVING_TRANSACTION_FUNDS_AMOUNT,0) AS SAVING_TRANSACTION_FUNDS_AMOUNT,
          COALESCE(ST.SAVING_TRANSACTION_FEES_AMOUNT,0) AS SAVING_TRANSACTION_FEES_AMOUNT,
          1 AS SAVING_TRANSACTION_COUNT

    FROM {env_var}_catalog.data_quality.saving_transaction_test_data ST
      LEFT JOIN {env_var}_catalog.data_quality.saving_account_test_data SA 
        ON ST.SAVING_PARENT_ACCOUNT_KEY = SA.SAVING_ACCOUNT_ENCODED_KEY

      LEFT JOIN {env_var}_catalog.data_quality.saving_product_test_data SP 
        ON ST.PRODUCT_TYPE_KEY = SP.SAVING_PRODUCT_ENCODED_KEY

      LEFT JOIN {env_var}_catalog.data_quality.saving_payment_detail_test_data PD 
        ON ST.SAVING_TRANSACTION_ENCODED_KEY = PD.SAVING_TRANSACTION_KEY

      LEFT JOIN {env_var}_catalog.data_quality.user_test_data SU 
        ON ST.USER_KEY = SU.USER_ENCODED_KEY
    """
    df = spark.sql(query)

    return df
