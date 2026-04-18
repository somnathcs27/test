# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Maintenance
# MAGIC Breaks out the bronze schema creates by source, and the silver/gold schema creates by sensitivity.

# COMMAND ----------

dbutils.widgets.text(name='catalog', defaultValue='dev_catalog')
catalog_name = dbutils.widgets.get("catalog")

dbutils.widgets.text(name='env_var', defaultValue = 'dev')
env_var = dbutils.widgets.get("env_var")

dbutils.widgets.text(name='common_libs_lts_ver', defaultValue = '')
common_libs_lts_ver = dbutils.widgets.get("common_libs_lts_ver")

dbutils.widgets.text(name='common_libs_lts_ver_underscore', defaultValue = '')
common_libs_lts_ver_underscore = dbutils.widgets.get("common_libs_lts_ver_underscore")

mdp_databricks_common_library = f"/Volumes/{env_var}_catalog/common_libraries/mdp_databricks_common_{common_libs_lts_ver_underscore}/mdp_databricks_common-{common_libs_lts_ver}-py3-none-any.whl"

mdp_databricks_common_library_wheelhouse = f"--no-index --find-links '/Volumes/{env_var}_catalog/common_libraries/wheelhouse/' '/Volumes/{env_var}_catalog/common_libraries/mdp_databricks_common_{common_libs_lts_ver_underscore}/mdp_databricks_common-{common_libs_lts_ver}-py3-none-any.whl'"

# COMMAND ----------

# MAGIC %pip uninstall $mdp_databricks_common_library -y 
# MAGIC %pip install $mdp_databricks_common_library_wheelhouse

# COMMAND ----------

from mdp_databricks_common.gold_layer_functions import insert_default_rows_into_dimension
from mdp_databricks_common.feature_toggle import feature_toggle as ft

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC # Create & Drop Schemas

# COMMAND ----------


# PRIMARY CATALOG SCHEMAS

# Use this configuration entry to create schemas.
create_schemas = [
    "common_libraries",
    # GOLD
    "gold_int",
    "gold_con",
    "gold_sec",
    "gold_pub",
    # REC
    "reconciliation",
    # QUALITY
    "data_quality",
    #Workflow scheduling and dependency control
    "workflow_control",
    # Schema migration tactical solution
    "schema_migration"
]

for schema in create_schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")

# Use this configuration entry to drop schemas.
drop_schemas = []

for schema in drop_schemas:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema}")


# COMMAND ----------


# LOGGING CATALOG SCHEMAS

# Use this configuration entry to create schemas.
create_schemas = [
    # DLT/LDP Event log output
    "event_log"
]

for schema in create_schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env_var}_logs.{schema}")

# Use this configuration entry to drop schemas.
drop_schemas = []

for schema in drop_schemas:
    spark.sql(f"DROP SCHEMA IF EXISTS {env_var}_logs.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC # Data Quality Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.data_quality.source_to_bronze_reconciliation (
        PK_SOURCE_TO_BRONZE_RECONCILIATION BIGINT,
        TEST_TYPE STRING,
        CATALOG_NAME STRING,
        SOURCE_LATEST_LOAD_COUNT INT,
        BRONZE_SCHEMA_NAME STRING,
        BRONZE_TABLE_NAME STRING,
        BRONZE_TABLE_LATEST_LOAD_COUNT INT,
        RECORD_COUNT_DIFFERENCE INT,
        COUNT_TEST_OUTCOME STRING,
        COUNT_OUTCOME_MESSAGE STRING,
        LOAD_TYPE STRING,
        LOAD_CHECK STRING,
        LOAD_ID BIGINT,
        TEST_CREATED_DATETIME TIMESTAMP
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.data_quality.bronze_to_silver_reconciliation (
        PK_BRONZE_TO_SILVER_RECONCILIATION BIGINT,
        TEST_TYPE STRING,
        CATALOG_NAME STRING,
        BRONZE_SCHEMA_NAME STRING,
        BRONZE_TABLE_NAME STRING,
        BRONZE_TABLE_COUNT INT,
        SILVER_SCHEMA_NAME STRING,
        SILVER_TABLE_NAME STRING,
        SILVER_TABLE_COUNT INT,
        RECORD_COUNT_DIFFERENCE INT,
        COUNT_TEST_OUTCOME STRING,
        COUNT_OUTCOME_MESSAGE STRING,
        COLUMN_NAMING_TEST_OUTCOME STRING, 
        COLUMN_NAMING_OUTCOME_MESSAGE STRING, 
        HOUSEKEEPING_TEST_OUTCOME STRING, 
        HOUSEKEEPING_TEST_OUTCOME_MESSAGE STRING,
        DATA_TYPE_TEST_OUTCOME STRING,
        LOAD_ID BIGINT,
        TEST_CREATED_DATETIME TIMESTAMP
    ) 
""")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog_name}.data_quality.bronze_to_silver_exception (
    PK_BRONZE_TO_SILVER_EXCEPTION BIGINT,
    CATALOG_NAME STRING,
    SCHEMA_NAME STRING,
    TABLE_NAME STRING,
    ERROR_MESSAGE STRING,
    LOAD_ID BIGINT,
    EXCEPTION_CREATED_DATETIME TIMESTAMP
  )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.data_quality.gold_fact_quality_engineering (
        PK_GOLD_FACT_QUALITY_ENGINEERING BIGINT,
        TEST_TYPE STRING,
        CATALOG_NAME STRING,
        SCHEMA_NAME STRING,
        FACT_NAME STRING,
        FACT_COUNT INT,
        COLUMN_NAME STRING,
        FOREIGN_KEY_RELATIONSHIP_CHECK STRING,
        FOREIGN_KEY_RELATIONSHIP_CHECK_MESSAGE STRING,
        FOREIGN_KEY_NULL_CHECK STRING,
        FOREIGN_KEY_NULL_CHECK_MESSAGE STRING,
        DATA_TYPE_CHECK STRING,
        DATA_TYPE_CHECK_MESSAGE STRING,
        LOAD_ID BIGINT,
        TEST_CREATED_DATETIME TIMESTAMP
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.data_quality.gold_dimension_quality_engineering (
        PK_GOLD_DIMENSION_QUALITY_ENGINEERING BIGINT,
        TEST_TYPE STRING,
        CATALOG_NAME STRING,
        SCHEMA_NAME STRING,
        DIMENSION_NAME STRING,
        DIMENSION_COUNT INT,
        PRIMARY_KEY_CHECK STRING,
        PRIMARY_KEY_CHECK_MESSAGE STRING,
        BUSINESS_KEY_CHECK STRING,
        BUSINESS_KEY_CHECK_MESSAGE STRING,
        HOUSEKEEPING_TEST_OUTCOME STRING, 
        DATA_TYPE_CHECK STRING,
        DUMMY_RECORDS_CHECK STRING,
        COLUMN_NULL_CHECK STRING,
        COLUMNS_WITH_NULL_VALUES STRING,
        ROW_IS_CURRENT_CHECK STRING,
        COLUMN_FLAG_CHECK STRING,
        BUSINESS_KEY_DUPLICATE_CHECK STRING,
        LOAD_ID BIGINT,
        TEST_CREATED_DATETIME TIMESTAMP
    )
""")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog_name}.data_quality.gold_exception (
    PK_GOLD_EXCEPTION BIGINT,
    CATALOG_NAME STRING,
    SCHEMA_NAME STRING,
    TABLE_NAME STRING,
    ERROR_MESSAGE STRING,
    LOAD_ID BIGINT,
    EXCEPTION_CREATED_DATETIME TIMESTAMP
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC # DROP TABLES
# MAGIC
# MAGIC Use this section to drop any tables you no longer need.

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC # GOLD - Dimensions

# COMMAND ----------

# --------------------------------
# dim_customer
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.dim_customer (
    PK_CUSTOMER BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    BK_CUSTOMER STRING,
    CUSTOMER_NUMBER STRING,
    TITLE STRING,
    FORENAME STRING,
    MIDDLE_NAMES STRING,
    SURNAME STRING,
    INITIALS STRING,
    GENDER STRING,
    DATE_OF_BIRTH DATE,
    AGE TINYINT,
    NATIONALITY STRING,
    NI_NUMBER STRING,
    EMAIL_ADDRESS STRING,
    PREFERRED_CONTACT_METHOD STRING,
    TELEPHONE_HOME STRING,
    TELEPHONE_MOBILE STRING,
    TELEPHONE_WORK STRING,
    TYPE_CODE STRING,
    START_DATE DATE,
    END_DATE DATE,
    SOURCE_SYSTEM STRING,
    MDP_LOAD_ID BIGINT,
    MDP_LOAD_DATETIME TIMESTAMP,
    ROW_START_DATETIME TIMESTAMP,
    ROW_END_DATETIME TIMESTAMP,
    ROW_IS_CURRENT INT,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Customer dimension - SCD Type 2'
"""
)

insert_default_rows_into_dimension(f"{catalog_name}","gold_con", "dim_customer")

# --------------------------------
# dim_customer_address
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.dim_customer_address (
    PK_CUSTOMER_ADDRESS BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    BK_CUSTOMER_ADDRESS STRING,
    CUSTOMER_NUMBER STRING,
    USE STRING,
    TYPE STRING,
    FLAT STRING,
    BUILDING_NUMBER STRING,
    BUILDING_NAME STRING,
    STREET_NAME STRING,
    TOWN_NAME STRING,
    DISTRICT STRING,
    COUNTY STRING,
    COUNTRY STRING,
    POSTAL_CODE STRING,
    COUNTRY_CODE STRING,
    START_DATE DATE,
    END_DATE DATE,
    SOURCE_SYSTEM STRING,
    MDP_LOAD_ID BIGINT,
    MDP_LOAD_DATETIME TIMESTAMP,
    ROW_START_DATETIME TIMESTAMP,
    ROW_END_DATETIME TIMESTAMP,
    ROW_IS_CURRENT INT,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Customer address dimension - SCD Type 2'
"""
)

insert_default_rows_into_dimension(f"{catalog_name}","gold_con", "dim_customer_address")

# --------------------------------
# dim_mortgage_account
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.dim_mortgage_account (
    PK_MORTGAGE_ACCOUNT BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    BK_MORTGAGE_ACCOUNT STRING,
    ACCOUNT_NUMBER STRING,
    SORT_CODE STRING,
    ACCOUNT_STATUS STRING,
    INTERMEDIARY STRING,
    SALES_SOURCE STRING,
    ORIGINAL_TERM_YEARS TINYINT,
    ORIGINAL_TERM_MONTHS TINYINT,
    SOURCE_SYSTEM STRING,
    MDP_LOAD_ID BIGINT,
    MDP_LOAD_DATETIME TIMESTAMP,
    ROW_START_DATETIME TIMESTAMP,
    ROW_END_DATETIME TIMESTAMP,
    ROW_IS_CURRENT INT,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Mortgage Account Dimension - SCD Type 2'
"""
)

insert_default_rows_into_dimension(f"{catalog_name}", "gold_con", "dim_mortgage_account")

# --------------------------------
# dim_mortgage_transaction
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.dim_mortgage_transaction (
    PK_MORTGAGE_TRANSACTION BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    BK_MORTGAGE_TRANSACTION STRING,
    PAYMENT_METHOD STRING,        
    TRANSACTION_TYPE STRING,
    IS_FINANCIAL_TRANSACTION STRING,
    SOURCE_SYSTEM STRING,        
    MDP_LOAD_ID BIGINT,
    MDP_LOAD_DATETIME TIMESTAMP,
    ROW_START_DATETIME TIMESTAMP,
    ROW_END_DATETIME TIMESTAMP,
    ROW_IS_CURRENT INT,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Mortgage Transaction Dimension - SCD Type 2'
"""
)

insert_default_rows_into_dimension(f"{catalog_name}", "gold_con", "dim_mortgage_transaction")

# --------------------------------
# dim_mortgage_part
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS gold_con.dim_mortgage_part (
    PK_MORTGAGE_PART BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    BK_MORTGAGE_PART STRING,
    ACCOUNT_NUMBER STRING,
    PART_NUMBER TINYINT,
    SORT_CODE STRING,    
    ADVANCE_TYPE STRING,
    BASIS_CATEGORY STRING,
    BASIS_TYPE STRING,
    PURPOSE STRING,
    STATUS STRING,
    REPAYMENT_TYPE STRING,
    SOURCE_SYSTEM STRING,
    MDP_LOAD_ID BIGINT,
    MDP_LOAD_DATETIME TIMESTAMP,
    ROW_START_DATETIME TIMESTAMP,
    ROW_END_DATETIME TIMESTAMP,
    ROW_IS_CURRENT TINYINT,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Mortgage Part Dimension - SCD Type 2'
"""
)

insert_default_rows_into_dimension(f"{catalog_name}", "gold_con", "dim_mortgage_part")

# --------------------------------
# dim_mortgage_property
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS gold_con.dim_mortgage_property (
    PK_MORTGAGE_PROPERTY BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    BK_MORTGAGE_PROPERTY BIGINT,
    PURPOSE STRING,
    OWNERSHIP STRING,
    PROPERTY_TYPE STRING,
    LOCATION STRING,
    REGION STRING,
    TENURE STRING,
    YEAR_PROPERTY_BUILT INTEGER,
    NUMBER_OF_FLOORS INTEGER,
    NUMBER_OF_BEDROOMS INTEGER,
    NUMBER_OF_HABITABLE_ROOMS TINYINT,
    NUMBER_OF_BATHROOMS TINYINT,
    NUMBER_OF_RECEPTIONS INTEGER,
    NUMBER_OF_KITCHENS INTEGER,
    FLOOR_NUMBER_FLAT STRING,
    ADDRESS_TYPE STRING,
    BUILDING_NUMBER STRING,
    BUILDING_NAME STRING,
    FLAT_IDENTIFIER STRING,
    STREET STRING,
    DISTRICT STRING,
    TOWN STRING,
    COUNTY STRING,
    PROPERTY_POSTCODE STRING,
    OUTWARD_POSTCODE STRING,
    PROPERTY_ADDRESS STRING,
    SOURCE_SYSTEM STRING,
    MDP_LOAD_ID BIGINT,
    MDP_LOAD_DATETIME TIMESTAMP,
    ROW_START_DATETIME TIMESTAMP,
    ROW_END_DATETIME TIMESTAMP,
    ROW_IS_CURRENT INT,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Mortgage Property Dimension - SCD Type 2'
"""
)

insert_default_rows_into_dimension(f"{catalog_name}", "gold_con", "dim_mortgage_property")    

# --------------------------------
# dim_mortgage_repayment
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS gold_con.dim_mortgage_repayment (
    PK_MORTGAGE_REPAYMENT BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    BK_MORTGAGE_REPAYMENT STRING,
    STATUS STRING,
    SOURCE_SYSTEM STRING,        
    MDP_LOAD_ID BIGINT,
    MDP_LOAD_DATETIME TIMESTAMP,
    ROW_START_DATETIME TIMESTAMP,
    ROW_END_DATETIME TIMESTAMP,
    ROW_IS_CURRENT INT,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Mortgage Repayment Dimension - SCD Type 2'
"""
)

insert_default_rows_into_dimension(f"{catalog_name}", "gold_con", "dim_mortgage_repayment")

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC # GOLD - Facts

# COMMAND ----------

# --------------------------------
# fact_mortgage_account
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.fact_mortgage_account (
    PK_FACT_MORTGAGE_ACCOUNT BIGINT,
    FK_MORTGAGE_ACCOUNT BIGINT,
    FK_MORTGAGE_PROPERTY BIGINT,
    FK_OPEN_DATE DATE,
    FK_CLOSED_DATE DATE,
    FK_REGISTERED_PAYMENT_DATE DATE,
    ACCOUNT_NUMBER STRING,
    ACCRUED_INTEREST DECIMAL(38,2),
    ADVANCES_YTD DECIMAL(38,2),
    ARREARS_BALANCE DECIMAL(38,2),
    CURRENT_BALANCE DECIMAL(38,2),
    EXPECTED_PAYMENT_AMOUNT DECIMAL(38,2),
    LEDGER_BALANCE DECIMAL(38,2),
    LOAN_TO_VALUE DECIMAL(38,6),
    ORIGINAL_LOAN_TO_VALUE DECIMAL(38,6),
    REMAINING_YEARS TINYINT,
    REMAINING_MONTHS TINYINT,
    WEIGHTED_RATE DECIMAL(38,6),
    PROPERTY_PURCHASE_PRICE DECIMAL(38,2),
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'mambu fact mortgage account with relevant measures'
"""
)

# --------------------------------
# fact_mortgage_part
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.fact_mortgage_part (    
    PK_FACT_MORTGAGE_PART BIGINT,
    FK_MORTGAGE_PART BIGINT,
    FK_MORTGAGE_ACCOUNT BIGINT,    
    FK_MORTGAGE_PRODUCT BIGINT,
    FK_MORTGAGE_PRODUCT_CURRENT_RANK BIGINT,
    FK_MORTGAGE_PRODUCT_FINAL_RANK BIGINT,
    FK_NEXT_BALLOON_DATE DATE,  
    FK_OPEN_DATE DATE,
    FK_CLOSED_DATE DATE,
    FK_PRODUCT_START_DATE DATE,
    ACCOUNT_NUMBER STRING,
    PART_NUMBER TINYINT,
    ACCRUED_INTEREST DECIMAL(38,2),
    ARREARS_BALANCE DECIMAL(38,2),
    BENCHMARK_RATE DECIMAL(38,6),
    CURRENT_BALANCE DECIMAL(38,2),
    CURRENT_RANK TINYINT,
    CUSTOMER_RATE DECIMAL(38,6),
    EXPECTED_PAYMENT_AMOUNT DECIMAL(38,2),
    FINAL_RANK TINYINT,
    LEDGER_BALANCE DECIMAL(38,2),
    LIQUIDITY_TERM_PREMIUM DECIMAL(38,6),
    INTEREST_RATE DECIMAL(38,6),
    REMAINING_YEARS TINYINT,
    REMAINING_MONTHS TINYINT,
    ORIGINAL_ADVANCE DECIMAL(38,2),
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'mambu fact mortgage part with relevant measures'
"""
)

# --------------------------------
# fact_mortgage_account_history
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.fact_mortgage_account_history (
    FK_MONTH_END_DATE DATE,
    PK_FACT_MORTGAGE_ACCOUNT BIGINT,
    FK_MORTGAGE_ACCOUNT BIGINT,
    FK_MORTGAGE_PROPERTY BIGINT,
    FK_OPEN_DATE DATE,
    FK_CLOSED_DATE DATE,
    FK_REGISTERED_PAYMENT_DATE DATE,
    ACCOUNT_NUMBER STRING,
    ACCRUED_INTEREST DECIMAL(38,2),
    ADVANCES_YTD DECIMAL(38,2),
    ARREARS_BALANCE DECIMAL(38,2),
    CURRENT_BALANCE DECIMAL(38,2),
    EXPECTED_PAYMENT_AMOUNT DECIMAL(38,2),
    LEDGER_BALANCE DECIMAL(38,2),
    LOAN_TO_VALUE DECIMAL(38,6),
    ORIGINAL_LOAN_TO_VALUE DECIMAL(38,6),
    REMAINING_YEARS TINYINT,
    REMAINING_MONTHS TINYINT,
    WEIGHTED_RATE DECIMAL(38,6),
    PURCHASE_PRICE DECIMAL(38,2),
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'mambu fact mortgage account month end history with relevant measures'
"""
)

# --------------------------------
# fact_mortgage_part_history
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.fact_mortgage_part_history (
    FK_MONTH_END_DATE DATE,
    PK_FACT_MORTGAGE_PART BIGINT,
    FK_MORTGAGE_PART BIGINT,
    FK_MORTGAGE_ACCOUNT BIGINT,    
    FK_MORTGAGE_PRODUCT BIGINT,
    FK_MORTGAGE_PRODUCT_CURRENT_RANK BIGINT,
    FK_MORTGAGE_PRODUCT_FINAL_RANK BIGINT,  
    FK_NEXT_BALLOON_DATE DATE,  
    FK_OPEN_DATE DATE,
    FK_CLOSED_DATE DATE,
    FK_PRODUCT_START_DATE DATE,
    ACCOUNT_NUMBER STRING,
    PART_NUMBER TINYINT,        
    ACCRUED_INTEREST DECIMAL(38,2),
    ARREARS_BALANCE DECIMAL(38,2),
    BENCHMARK_RATE DECIMAL(38,6),
    CURRENT_BALANCE DECIMAL(38,2),
    CURRENT_RANK TINYINT,
    CUSTOMER_RATE DECIMAL(38,6),
    EXPECTED_PAYMENT_AMOUNT DECIMAL(38,2),
    FINAL_RANK TINYINT,
    LEDGER_BALANCE DECIMAL(38,2),
    LIQUIDITY_TERM_PREMIUM DECIMAL(38,6),
    INTEREST_RATE DECIMAL(38,6),
    REMAINING_YEARS TINYINT,
    REMAINING_MONTHS TINYINT,
    ORIGINAL_ADVANCE DECIMAL(38,2),
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP   
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'mambu fact mortgage part month end history with relevant measures'
"""
)

# --------------------------------
# fact_mortgage_transaction
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.fact_mortgage_transaction (
    PK_FACT_MORTGAGE_TRANSACTION BIGINT,        
    FK_MORTGAGE_TRANSACTION BIGINT,
    FK_MORTGAGE_PART BIGINT,
    FK_MORTGAGE_ACCOUNT BIGINT,
    FK_BUSINESS_DATE DATE,
    FK_PROCESSED_DATE DATE,
    TRANSACTION_ID BIGINT, 
    ACCOUNT_NUMBER STRING,
    PART_NUMBER TINYINT,               
    BENCHMARK_RATE DECIMAL(38,6),
    CUSTOMER_RATE DECIMAL(38,6),
    LIQUIDITY_TERM_PREMIUM DECIMAL(38,6),
    RECEIPT_AMOUNT DECIMAL(38,2),
    WITHDRAWAL_AMOUNT DECIMAL(38,2),
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'mambu fact mortgage transaction with relevant measures'
"""
)

# --------------------------------
# bridge_party_account
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.bridge_party_account (
    FK_CUSTOMER BIGINT,
    FK_ACCOUNT BIGINT,
    CUSTOMER_NUMBER STRING,
    ACCOUNT_NUMBER STRING,
    ACCOUNT_TYPE STRING,
    ACCOUNT_HOLDER_TYPE STRING,
    RELATIONSHIP_START_DATE DATE,
    RELATIONSHIP_END_DATE DATE,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'generic bridge between customers and accounts along with relationship start and end dates'
"""
)

# --------------------------------
# bridge_party_account_history
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.bridge_party_account_history (
    FK_MONTH_END_DATE DATE,
    FK_CUSTOMER BIGINT,
    FK_ACCOUNT BIGINT,
    CUSTOMER_NUMBER STRING,
    ACCOUNT_NUMBER STRING,        
    ACCOUNT_TYPE STRING,
    ACCOUNT_HOLDER_TYPE STRING,
    RELATIONSHIP_START_DATE DATE,
    RELATIONSHIP_END_DATE DATE,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'generic bridge month end history between customers and accounts along with relationship start and end dates'
"""
)

# --------------------------------------
# bridge_mortgage_case_mortgage_account
# --------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.bridge_mortgage_case_mortgage_account (
    FK_MORTGAGE_CASE BIGINT,
    FK_MORTGAGE_ACCOUNT BIGINT,
    APPLICATION_NUMBER STRING,
    ACCOUNT_NUMBER STRING,
    APPLICATION_TYPE STRING,
    CREATED_DATE DATE,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'bridge between mortgage cases and mortgage accounts'
"""
)   

# --------------------------------
# fact_customer
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.fact_customer (
    PK_FACT_CUSTOMER BIGINT,
    FK_CUSTOMER BIGINT,
    FK_CUSTOMER_ADDRESS_CORRESPONDENCE BIGINT,
    FK_CUSTOMER_ADDRESS_RESIDENTIAL BIGINT,
    FK_LATEST_MORTGAGE_ACCOUNT_CLOSE_DATE DATE,
    CUSTOMER_NUMBER STRING,
    MORTGAGE_APPLICATIONS INTEGER,
    OPEN_MORTGAGE_ACCOUNTS INTEGER,
    CLOSED_MORTGAGE_ACCOUNTS INTEGER,
    MORTGAGE_CURRENT_BALANCE_SHARE DECIMAL(38,2),
    MORTGAGE_ACCRUED_INTEREST_SHARE DECIMAL(38,2),
    OPEN_SAVING_ACCOUNTS INTEGER,
    CLOSED_SAVING_ACCOUNTS INTEGER,
    SAVING_CAPITAL_LEDGER_BALANCE_SHARE DECIMAL(38,2),
    SAVING_CAPITAL_AVAILABLE_BALANCE_SHARE DECIMAL(38,2),
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'fact customer with relevant measures'
"""
)

# --------------------------------
# fact_customer_history
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.fact_customer_history (
    FK_MONTH_END_DATE DATE,
    PK_FACT_CUSTOMER BIGINT,
    FK_CUSTOMER BIGINT,
    FK_CUSTOMER_ADDRESS_CORRESPONDENCE BIGINT,
    FK_CUSTOMER_ADDRESS_RESIDENTIAL BIGINT,
    FK_LATEST_MORTGAGE_ACCOUNT_CLOSE_DATE DATE,
    CUSTOMER_NUMBER STRING,
    MORTGAGE_APPLICATIONS INTEGER,
    OPEN_MORTGAGE_ACCOUNTS INTEGER,
    CLOSED_MORTGAGE_ACCOUNTS INTEGER,
    MORTGAGE_CURRENT_BALANCE_SHARE DECIMAL(38,2),
    MORTGAGE_ACCRUED_INTEREST_SHARE DECIMAL(38,2),
    OPEN_SAVING_ACCOUNTS INTEGER,
    CLOSED_SAVING_ACCOUNTS INTEGER,
    SAVING_CAPITAL_LEDGER_BALANCE_SHARE DECIMAL(38,2),
    SAVING_CAPITAL_AVAILABLE_BALANCE_SHARE DECIMAL(38,2),
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'fact customer month end history with relevant measures'
"""
)

# --------------------------------
# fact_mortgage_repayment
# --------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.gold_con.fact_mortgage_repayment (    
    PK_FACT_MORTGAGE_REPAYMENT BIGINT,
    FK_MORTGAGE_REPAYMENT BIGINT,
    FK_MORTGAGE_PART BIGINT,
    FK_MORTGAGE_ACCOUNT BIGINT,
    FK_DUE_DATE DATE,
    FK_LAST_PAID_DATE DATE,
    FK_REPAID_DATE DATE,
    ACCOUNT_NUMBER STRING,
    PART_NUMBER TINYINT,
    AMOUNT DECIMAL(38,2),
    INTEREST_DUE DECIMAL(38,2),
    PRINCIPAL_DUE DECIMAL(38,2),
    FEES_DUE DECIMAL(38,2),
    PENALTY_DUE DECIMAL(38,2),
    FUNDERS_INTEREST_DUE DECIMAL(38,2),
    ORGANIZATION_COMMISSION_DUE DECIMAL(38,2),
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Contains data related to mortgage repayments, the granularity is 1 row per repayment event (past and future), frequency of update is once per day'
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC # Reconciliation Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.reconciliation.gold_layer (
    TABLE_SCHEMA STRING,
    TABLE_NAME STRING,
    AFFECTED_ROW_COUNT BIGINT,
    INSERTED_ROW_COUNT BIGINT,
    UPDATED_ROW_COUNT BIGINT,
    DELETED_ROW_COUNT BIGINT,
    TOTAL_ROW_COUNT BIGINT,
    MDP_GOLD_LAYER_PROCESSED_DATETIME TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Gold layer reconciliation table'
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Record Count Views

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW etl.audit_table_gold_schema_columns AS
        SELECT
            table_schema as SCHEMA_NAME,
            table_name AS TABLE_NAME,
            column_name AS COLUMN_NAME,
            full_data_type as DATA_TYPE
        FROM
            information_schema.columns
        WHERE
            table_schema IN ('gold_int', 'gold_con')
""")

spark.sql(f"""
    CREATE OR REPLACE VIEW etl.audit_table_gold_last_successful_run AS
        WITH row_count AS (
            SELECT 
                TABLE_NAME,
                TOTAL_ROW_COUNT,
                MDP_GOLD_LAYER_PROCESSED_DATETIME,
                ROW_NUMBER() OVER(PARTITION BY TABLE_NAME ORDER BY MDP_GOLD_LAYER_PROCESSED_DATETIME DESC) AS RANK
            FROM
                {catalog_name}.reconciliation.gold_layer 
            )
        SELECT
            t.TABLE_SCHEMA AS SCHEMA_NAME,
            t.TABLE_NAME AS TABLE_NAME,
            t.TABLE_TYPE AS TABLE_TYPE,
            COALESCE((CASE 
                WHEN t.TABLE_TYPE = 'MANAGED' THEN MAX(TOTAL_ROW_COUNT)
                ELSE 0 
            END),0) AS RECORD_COUNT,          
            COUNT(c.COLUMN_NAME) AS NUMBER_OF_COLUMNS,            
            CASE 
                WHEN t.TABLE_TYPE = 'MANAGED' THEN MAX(MDP_GOLD_LAYER_PROCESSED_DATETIME) 
                ELSE  MAX(t.LAST_ALTERED) 
            END AS LAST_SUCCESSFUL_RUN
        FROM
            information_schema.tables t
        JOIN 
            information_schema.columns c
            ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND t.TABLE_NAME = c.TABLE_NAME
        LEFT JOIN row_count 
            ON t.TABLE_NAME = row_count.TABLE_NAME 
            AND row_count.RANK = 1               
        WHERE
            t.TABLE_SCHEMA IN ('gold_con', 'gold_int')
        GROUP BY
            t.TABLE_SCHEMA,
            t.TABLE_NAME ,
            t.TABLE_TYPE
""")
         



# COMMAND ----------

# MAGIC %md
# MAGIC # Workflow Scheduling and Dependency Control

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.workflow_control.mdp_data_category (
    pk_mdp_data_category BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    category_name STRING
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Workflow control table MDP data categorys'
"""
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {catalog_name}.workflow_control.job_detail (
pk_job_detail BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
fk_mdp_data_category BIGINT NOT NULL,
job_name STRING
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Workflow control table detail of included jobs'
"""
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.workflow_control.job_dependency (
    pk_job_dependency BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    fk_job_detail BIGINT,
    fk_job_detail_dependency BIGINT
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Workflow control table detail of job depdencys'
"""
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.workflow_control.completed_jobs (
    fk_job_detail BIGINT,
    job_name STRING,
    job_end_datetime TIMESTAMP,
    status STRING,
    error_detail STRING,
    trigger_file_generated TINYINT,
    trigger_file_path STRING)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')  
PARTITIONED BY(job_name, job_end_datetime)
COMMENT 'Workflow control table completed job end stamps'
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tactical Managed Table Schema Migration

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.schema_migration.mdp_flyway_schema_history(
version STRING NOT NULL,
description STRING NOT NULL,
script STRING NOT NULL,
installed_on TIMESTAMP NOT NULL
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Flyway schema history table for managing schema migrations for managed tables'
"""
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.schema_migration.mdp_flyway_schema_failures(
script STRING NOT NULL,
failed_on TIMESTAMP NOT NULL,
error_message STRING NOT NULL
)
USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Flyway schema failure table for managing schema migrations for managed tables'
"""
)
