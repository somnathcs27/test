# Databricks notebook source
import dlt

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name="extended_data_store_application_data", 
    comment="Stores application data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def extended_data_store_application_data():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.id AS STRING) AS ENCODED_KEY,
        CAST(MXT.mortgage_account_id AS STRING) AS MORTGAGE_ACCOUNT_ID,
        CAST(MXT.mso_case_identifier AS STRING) AS MSO_CASE_IDENTIFIER,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        TO_TIMESTAMP(MXT.start_date, 'dd-MMM-yy HH:mm:ss') AS START_DATE,    
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    
    FROM {env_var}_catalog.bronze_extended_data_store_con.extended_applicationdata AS MXT
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="extended_data_store_charge", 
    comment="Stores charge data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def extended_data_store_charge():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.id AS STRING) AS ENCODED_KEY,
        CAST(MXT.mortgage_account_id AS STRING) AS MORTGAGE_ACCOUNT_ID,
        CAST(MXT.deed_country_code AS STRING) AS DEED_COUNTRY_CODE,
        TO_TIMESTAMP(MXT.received_date, 'dd-MMM-yy HH:mm:ss') AS RECEIVED_DATE,
        CAST(MXT.deed_registration_flag AS BOOLEAN) AS DEED_REGISTRATION_FLAG,
        CAST(MXT.title_id AS STRING) AS TITLE_ID,
        TO_TIMESTAMP(MXT.charge_registration_date, 'dd-MMM-yy HH:mm:ss') AS CHARGE_REGISTRATION_DATE,
        CAST(MXT.unique_ref_number AS STRING) AS UNIQUE_REF_NUMBER,
        CAST(MXT.data_sync_completed AS BOOLEAN) AS DATA_SYNC_COMPLETED,
        TO_TIMESTAMP(MXT.data_sync_date, 'dd-MMM-yy HH:mm:ss') AS DATA_SYNC_DATE,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        TO_TIMESTAMP(MXT.updated_at, 'dd-MMM-yy HH:mm:ss') AS UPDATED_AT,
        CAST(MXT.updated_by AS STRING) AS UPDATED_BY,
        CAST(MXT.MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_extended_data_store_con.extended_charge AS MXT
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="extended_data_store_overseaaddress", 
    comment="Stores overseas address data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def extended_data_store_overseaaddress():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.id AS STRING) AS ENCODED_KEY,
        CAST(MXT.address_line_1 AS STRING) AS ADDRESS_LINE_1,
        CAST(MXT.address_line_2 AS STRING) AS ADDRESS_LINE_2,
        CAST(MXT.address_line_3 AS STRING) AS ADDRESS_LINE_3,
        CAST(MXT.address_line_4 AS STRING) AS ADDRESS_LINE_4,
        CAST(MXT.country_type_code AS STRING) AS COUNTRY_TYPE_CODE,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        TO_TIMESTAMP(MXT.start_date, 'dd-MMM-yy HH:mm:ss') AS START_DATE,
        CAST(MXT.MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_extended_data_store_con.extended_overseaaddress AS MXT
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="extended_data_store_property", 
    comment="Stores property data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def extended_data_store_property():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.id AS STRING) AS ENCODED_KEY,
        CAST(MXT.mortgage_account_id AS STRING) AS MORTGAGE_ACCOUNT_ID,
        CAST(MXT.property_purpose AS STRING) AS PROPERTY_PURPOSE,
        CAST(MXT.property_ownership AS STRING) AS PROPERTY_OWNERSHIP,
        CAST(MXT.property_type AS STRING) AS PROPERTY_TYPE,
        CAST(MXT.property_location AS STRING) AS PROPERTY_LOCATION,
        CAST(MXT.property_region AS STRING) AS PROPERTY_REGION,
        CAST(MXT.tenure AS STRING) AS TENURE,
        CAST(MXT.year_property_built AS SMALLINT) AS YEAR_PROPERTY_BUILT,
        CAST(MXT.new_build AS STRING) AS NEW_BUILD,
        CAST(MXT.property_floors AS SMALLINT) AS PROPERTY_FLOORS,
        CAST(MXT.property_bedrooms AS SMALLINT) AS PROPERTY_BEDROOMS,
        CAST(MXT.habitable_rooms AS SMALLINT) AS HABITABLE_ROOMS,
        CAST(MXT.property_bathrooms AS SMALLINT) AS PROPERTY_BATHROOMS,
        CAST(MXT.property_receptions AS SMALLINT) AS PROPERTY_RECEPTIONS,
        CAST(MXT.property_kitchens AS SMALLINT) AS PROPERTY_KITCHENS,
        CAST(MXT.floor_number AS SMALLINT) AS FLOOR_NUMBER,
        CAST(MXT.uk_address_id AS STRING) AS UK_ADDRESS_ID,
        CAST(MXT.oversea_address_id AS STRING) AS OVERSEA_ADDRESS_ID,
        CAST(MXT.valuation_id AS STRING) AS VALUATION_ID,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        CAST(MXT.MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_extended_data_store_con.extended_property AS MXT
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="extended_data_store_title", 
    comment="Stores title data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def extended_data_store_title():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.id AS STRING) AS ENCODED_KEY,
        CAST(MXT.prefix_and_number AS STRING) AS PREFIX_AND_NUMBER,
        CAST(MXT.property_id AS STRING) AS PROPERTY_ID,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        CAST(MXT.MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_extended_data_store_con.extended_title AS MXT
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="extended_data_store_ukaddress", 
    comment="Stores ukaddress data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def extended_data_store_ukaddress():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.id AS STRING) AS ENCODED_KEY,
        CAST(MXT.building_number AS STRING) AS BUILDING_NUMBER,
        CAST(MXT.address_type AS STRING) AS ADDRESS_TYPE,
        CAST(MXT.building_name AS STRING) AS BUILDING_NAME,
        CAST(MXT.flat AS STRING) AS FLAT,
        CAST(MXT.street AS STRING) AS STREET,
        CAST(MXT.district AS STRING) AS DISTRICT,
        CAST(MXT.town AS STRING) AS TOWN,
        CAST(MXT.county AS STRING) AS COUNTY,
        CAST(MXT.postcode AS STRING) AS POSTCODE,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        TO_TIMESTAMP(MXT.start_date, 'dd-MMM-yy HH:mm:ss') AS START_DATE,
        CAST(MXT.MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_extended_data_store_con.extended_ukaddress AS MXT
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="extended_data_store_valuation", 
    comment="Stores valuation data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def extended_data_store_valuation():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.id AS STRING) AS ENCODED_KEY,
        CAST(MXT.latest_valuation_amount AS DECIMAL(38,2)) AS LATEST_VALUATION_AMOUNT,
        TO_TIMESTAMP(MXT.latest_valuation_date, 'dd-MMM-yy HH:mm:ss') AS LATEST_VALUATION_DATE,
        CAST(MXT.latest_automated_valuation_amount AS DECIMAL(38,2)) AS LATEST_AUTOMATED_VALUATION_AMOUNT,
        TO_TIMESTAMP(MXT.latest_automated_valuation_date, 'dd-MMM-yy HH:mm:ss') AS LATEST_AUTOMATED_VALUATION_DATE,           
        CAST(MXT.property_purchase_price AS DECIMAL(38,2)) AS PROPERTY_PURCHASE_PRICE,
        CAST(MXT.valuation_type AS STRING) AS VALUATION_TYPE,
        CAST(MXT.lease_unexpired_term AS STRING) AS LEASE_UNEXPIRED_TERM,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        CAST(MXT.MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_extended_data_store_con.extended_valuation AS MXT
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="extended_data_store_mortgageaccount", 
    comment="Stores mortgage account data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that MORTGAGE_ACCOUNT_ID is not null", "MORTGAGE_ACCOUNT_ID IS NOT NULL")
def extended_data_store_mortgageaccount():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.mortgage_account_id AS STRING) AS MORTGAGE_ACCOUNT_ID,
        CAST(MXT.is_new_account_ported AS BOOLEAN) AS IS_NEW_ACCOUNT_PORTED,
        TO_TIMESTAMP(MXT.original_account_open_date, 'dd-MMM-yy HH:mm:ss') AS ORIGINAL_ACCOUNT_OPEN_DATE,
        CAST(MXT.segment AS STRING) AS SEGMENT,
        CAST(MXT.conveyancer_panel_number AS STRING) AS CONVEYANCER_PANEL_NUMBER,
        CAST(MXT.conveyancer_company_name AS STRING) AS CONVEYANCER_COMPANY_NAME,
        CAST(MXT.repayment_strategy AS STRING) AS REPAYMENT_STRATEGY,
        CAST(MXT.repayment_strategy_amount AS DECIMAL(38,2)) AS REPAYMENT_STRATEGY_AMOUNT,
        CAST(MXT.conditions_year AS SMALLINT) AS CONDITIONS_YEAR,
        CAST(MXT.ltv_at_completion AS DECIMAL(38,2)) AS LTV_AT_COMPLETION,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        CAST(MXT.MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_extended_data_store_con.extendedaccount_mortgageaccount AS MXT
    """
    )

    return df

# COMMAND ----------

@dlt.table(
    name="extended_data_store_mortgagepart", 
    comment="Stores mortgage part data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that MORTGAGE_ACCOUNT_ID is not null", "MORTGAGE_ACCOUNT_ID IS NOT NULL")
def extended_data_store_mortgagepart():

    df = spark.sql(f"""
    SELECT
        CAST(MXT.mortgage_account_id AS STRING) AS MORTGAGE_ACCOUNT_ID,
        CAST(MXT.part_number AS SMALLINT) AS PART_NUMBER,
        CAST(MXT.is_new_part_ported AS BOOLEAN) AS IS_NEW_PART_PORTED,
        CAST(MXT.ported_account_number AS STRING) AS PORTED_ACCOUNT_NUMBER,
        CAST(MXT.is_product_transfer AS BOOLEAN) AS IS_PRODUCT_TRANSFER,
        TO_TIMESTAMP(MXT.product_transfer_date, 'dd-MMM-yy HH:mm:ss') AS PRODUCT_TRANSFER_DATE,
        TO_TIMESTAMP(MXT.erc_start_date, 'dd-MMM-yy HH:mm:ss') AS ERC_START_DATE,
        TO_TIMESTAMP(MXT.transfer_of_equity_date, 'dd-MMM-yy HH:mm:ss') AS TRANSFER_OF_EQUITY_DATE,
        CAST(MXT.party_added_or_removed AS STRING) AS PARTY_ADDED_OR_REMOVED,
        CAST(MXT.product_risk_category AS STRING) AS PRODUCT_RISK_CATEGORY,
        TO_TIMESTAMP(MXT.product_start_date, 'dd-MMM-yy HH:mm:ss') AS PRODUCT_START_DATE,
        TO_TIMESTAMP(MXT.completion_date, 'dd-MMM-yy HH:mm:ss') AS COMPLETION_DATE,
        CAST(MXT.application_type AS STRING) AS APPLICATION_TYPE,
        TO_TIMESTAMP(MXT.offer_date, 'dd-MMM-yy HH:mm:ss') AS OFFER_DATE,
        CAST(MXT.purpose_code AS STRING) AS PURPOSE_CODE,
        TO_TIMESTAMP(MXT.created_at, 'dd-MMM-yy HH:mm:ss') AS CREATED_AT,
        CAST(MXT.created_by AS STRING) AS CREATED_BY,
        CAST(MXT.MDP_LOAD_ID AS STRING) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
    FROM {env_var}_catalog.bronze_extended_data_store_con.extendedaccount_mortgagepart AS MXT
    """
    )

    return df