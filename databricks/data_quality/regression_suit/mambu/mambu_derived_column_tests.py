# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# Initialize Spark session
spark = SparkSession.builder.appName("DLT Testing").getOrCreate()

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------


# Load the derived data
derived_data = spark.table("dev_catalog.data_quality.fact_saving_account_derived_data_test")


# Define the expected input and calculated values
saving_account_encoded_key = "8a9ca04491fcfbf60192006b997b003d"

# Perform assertions
# Test REP_CAPITAL_LEDGER_BALANCE calculation
capital_ledger_balance = derived_data.filter(col("SAVING_ACCOUNT_ENCODED_KEY") == saving_account_encoded_key).select("REP_CAPITAL_LEDGER_BALANCE").collect()[0][0]
expected_capital_ledger_balance = 1371.5 - 0.0
assert capital_ledger_balance == expected_capital_ledger_balance, f"REP_CAPITAL_LEDGER_BAL test failed! Expected: {expected_capital_ledger_balance}, Got: {capital_ledger_balance}"

print("All tests passed!")

# COMMAND ----------

# Load the derived data
derived_data = spark.table("dev_catalog.data_quality.fact_saving_account_derived_data_test")

# Define the expected input and calculated values
saving_account_encoded_key = "8a9ca13492d8f8c60192e3619aa413ec"

# Perform assertions
# Test CAPITAL_LEDGER_BALANCE calculation
capital_ledger_balance = float(derived_data.filter(col("SAVING_ACCOUNT_ENCODED_KEY") == saving_account_encoded_key).select("CAPITAL_LEDGER_BALANCE").collect()[0][0])
expected_ledger_balance = 500.61
assert capital_ledger_balance == expected_ledger_balance, f"CAPITAL_LEDGER_BALANCE test failed! Expected: {expected_ledger_balance}, Got: {capital_ledger_balance}"


# Test SAVING_ACCOUNT_BLOCKED_BALANCE calculation
saving_account_blocked_balance = float(derived_data.filter(col("SAVING_ACCOUNT_ENCODED_KEY") == saving_account_encoded_key).select("SAVING_ACCOUNT_BLOCKED_BALANCE").collect()[0][0])
expected_saving_account_blocked_balance = 50.00
assert saving_account_blocked_balance == expected_saving_account_blocked_balance, f"SAVING_ACCOUNT_BLOCKED_BALANCE test failed! Expected: {expected_saving_account_blocked_balance}, Got: {saving_account_blocked_balance}"


# Test SAVING_ACCOUNT_LOCKED_BALANCE
saving_account_locked_balance = float(derived_data.filter(col("SAVING_ACCOUNT_ENCODED_KEY") == saving_account_encoded_key).select("SAVING_ACCOUNT_LOCKED_BALANCE").collect()[0][0])
expected_saving_account_locked_balance = 00.00
assert saving_account_locked_balance == expected_saving_account_locked_balance, f"SAVING_ACCOUNT_LOCKED_BALANCE test failed! Expected: {expected_saving_account_locked_balance}, Got: {saving_account_locked_balance}"


# Test CAPITAL_AVAILABLE_BALANCE
capital_available_balance = float(derived_data.filter(col("SAVING_ACCOUNT_ENCODED_KEY") == saving_account_encoded_key).select("CAPITAL_AVAILABLE_BALANCE").collect()[0][0])
expected_capital_available_balance = 500.61 - (50.00 + 0.00)
assert capital_available_balance == expected_capital_available_balance, f"CAPITAL_AVAILABLE_BALANCE test failed! Expected: {expected_capital_available_balance}, Got: {capital_available_balance}"


print("All tests passed for CAPITAL_AVAILABLE_BALANCE and related columns!")

# COMMAND ----------


# Load the derived data
derived_data = spark.table("dev_catalog.data_quality.fact_saving_account_derived_data_test")

# Define the expected input and calculated values
saving_account_encoded_key = "8a9ca8219300d9b10193072caaa32b67"

# Perform assertions
# Test ACCRUED_INTEREST_GROSS
accrued_interest_gross = float(derived_data.filter(trim(col("SAVING_ACCOUNT_ENCODED_KEY")) == saving_account_encoded_key).select("ACCRUED_INTEREST_GROSS").collect()[0][0])
expected_accrued_interest_gross = 30.60
assert accrued_interest_gross == expected_accrued_interest_gross, f"ACCRUED_INTEREST_GROSS test failed! Expected: {expected_accrued_interest_gross}, Got: {accrued_interest_gross}"

# Test TAX_RATE
tax_rate = float(derived_data.filter(trim(col("SAVING_ACCOUNT_ENCODED_KEY")) == saving_account_encoded_key).select("TAX_RATE").collect()[0][0])
expected_tax_rate = 0.00
assert tax_rate == expected_tax_rate, f"TAX_RATE test failed! Expected: {expected_tax_rate}, Got: {tax_rate}"


# Test ACCRUED_INTEREST_TAX
accrued_interest_tax = float(derived_data.filter(trim(col("SAVING_ACCOUNT_ENCODED_KEY")) == saving_account_encoded_key).select("ACCRUED_INTEREST_TAX").collect()[0][0])
expected_accrued_interest_tax = 0.00  # Calculation: 30.60 * (0.00 / 100) = 0.00
assert accrued_interest_tax == expected_accrued_interest_tax, f"ACCRUED_INTEREST_TAX test failed! Expected: {expected_accrued_interest_tax}, Got: {accrued_interest_tax}"


# Test ACCRUED_INTEREST_NET
accrued_interest_net = float(derived_data.filter(trim(col("SAVING_ACCOUNT_ENCODED_KEY")) == saving_account_encoded_key).select("ACCRUED_INTEREST_NET").collect()[0][0])
expected_accrued_interest_net = 00.00 - 0.00 
assert accrued_interest_net == expected_accrued_interest_net, f"ACCRUED_INTEREST_NET test failed! Expected: {expected_accrued_interest_net}, Got: {accrued_interest_net}"


print("All tests passed for ACCRUED_INTEREST_GROSS, TAX_RATE, ACCRUED_INTEREST_TAX, and ACCRUED_INTEREST_NET!")


# COMMAND ----------

from pyspark.sql.functions import col, trim

# Load the transaction data
transaction_data = spark.table("dev_catalog.data_quality.fact_saving_transaction_derived_data_test")

# Define the expected input and calculated values
saving_transaction_encoded_key = "8a9ca13492d8f8c60192d966200c0105"

# Perform assertions for all measure columns
# Test SAVING_TRANSACTION_BALANCE
transaction_balance = float(transaction_data.filter(trim(col("SAVING_TRANSACTION_ENCODED_KEY")) == saving_transaction_encoded_key).select("SAVING_TRANSACTION_BALANCE").collect()[0][0])
expected_transaction_balance = 166934.55
assert transaction_balance == expected_transaction_balance, f"SAVING_TRANSACTION_BALANCE test failed! Expected: {expected_transaction_balance}, Got: {transaction_balance}"

# Test SAVING_TRANSACTION_AMOUNT
transaction_amount = float(
    transaction_data.filter(trim(col("SAVING_TRANSACTION_ENCODED_KEY")) == saving_transaction_encoded_key).select("SAVING_TRANSACTION_AMOUNT").collect()[0][0])
expected_transaction_amount = 60.9
assert transaction_amount == expected_transaction_amount, f"SAVING_TRANSACTION_AMOUNT test failed! Expected: {expected_transaction_amount}, Got: {transaction_amount}"

# Test SAVING_TRANSACTION_INTEREST_AMOUNT
transaction_interest_amount = float(
    transaction_data.filter(trim(col("SAVING_TRANSACTION_ENCODED_KEY")) == saving_transaction_encoded_key).select("SAVING_TRANSACTION_INTEREST_AMOUNT").collect()[0][0])
expected_transaction_interest_amount = 0.00
assert transaction_interest_amount == expected_transaction_interest_amount, f"SAVING_TRANSACTION_INTEREST_AMOUNT test failed! Expected: {expected_transaction_interest_amount}, Got: {transaction_interest_amount}"

# Test SAVING_TRANSACTION_INTEREST_RATE
transaction_interest_rate = float(
    transaction_data.filter(trim(col("SAVING_TRANSACTION_ENCODED_KEY")) == saving_transaction_encoded_key).select("SAVING_TRANSACTION_INTEREST_RATE").collect()[0][0])
expected_transaction_interest_rate = 0.00
assert transaction_interest_rate == expected_transaction_interest_rate, f"SAVING_TRANSACTION_INTEREST_RATE test failed! Expected: {expected_transaction_interest_rate}, Got: {transaction_interest_rate}"

# Test SAVING_TRANSACTION_FUNDS_AMOUNT
transaction_funds_amount = float(
    transaction_data.filter(trim(col("SAVING_TRANSACTION_ENCODED_KEY")) == saving_transaction_encoded_key).select("SAVING_TRANSACTION_FUNDS_AMOUNT").collect()[0][0])
expected_transaction_funds_amount = 60.9
assert transaction_funds_amount == expected_transaction_funds_amount, f"SAVING_TRANSACTION_FUNDS_AMOUNT test failed! Expected: {expected_transaction_funds_amount}, Got: {transaction_funds_amount}"

# Test SAVING_TRANSACTION_FEES_AMOUNT
transaction_fees_amount = float(
    transaction_data.filter(trim(col("SAVING_TRANSACTION_ENCODED_KEY")) == saving_transaction_encoded_key).select("SAVING_TRANSACTION_FEES_AMOUNT").collect()[0][0])
expected_transaction_fees_amount = 0.00
assert transaction_fees_amount == expected_transaction_fees_amount, f"SAVING_TRANSACTION_FEES_AMOUNT test failed! Expected: {expected_transaction_fees_amount}, Got: {transaction_fees_amount}"


print("All tests passed for SAVING_TRANSACTION_DERIVED_DATA and its measure columns!")

