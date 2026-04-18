# Databricks notebook source
import dlt

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

from collections import namedtuple
from pyspark.sql.types import StructType
SqlPair = namedtuple("SqlPair", ["active_sql", "null_sql"])

# COMMAND ----------

def get_sql_line(select_dict, prop_name, target_collection):
    active_sql = "    " + select_dict[prop_name].active_sql + "\n"
    null_sql = "    " + select_dict[prop_name].null_sql + "\n"
    
    if(target_collection is None):
        return null_sql
    
    if prop_name in target_collection:
        return active_sql
    else:
        return null_sql

# COMMAND ----------

def get_first_level_fields(schema, path):
    current_type = schema
    for part in path.split("."):
        try:
            field = current_type[part]
            if isinstance(field.dataType, StructType):
                current_type = field.dataType
            else:
                return None
        except (KeyError, AttributeError, TypeError, IndexError):
            return None
    return {f"{path}.{field.name}" for field in current_type.fields}

# COMMAND ----------

def get_meds_column_content_schema(env_var):
    input_df = spark.read.table(f"{env_var}_catalog.bronze_meds_con.mortgage_extended").limit(1)
    schema = input_df.schema
    
    return{
        "propertyData_set": get_first_level_fields(schema, "propertyData"),
        "title_set" : get_first_level_fields(schema, "propertyData.title"),
        "ukAddress_set" : get_first_level_fields(schema, "propertyData.ukAddress"),
        "valuation_set" : get_first_level_fields(schema, "propertyData.valuation"),
        "applicationData_set" : get_first_level_fields(schema, "applicationData"),
        "overseaAddress_set" : get_first_level_fields(schema, "propertyData.overseaAddress"),
    }

# COMMAND ----------

@dlt.table(
    name="meds_property", 
    comment="Stores property data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that MORTGAGE_ACCOUNT_ID is not null", "MORTGAGE_ACCOUNT_ID IS NOT NULL")
def meds_property():

    cols = get_meds_column_content_schema(env_var)

    # return replacement of kebab case ('-') with blank spaces and capitilisation 
    select_dict ={
        "propertyData.propertyPurpose": 
            SqlPair(
                active_sql="INITCAP(REGEXP_REPLACE(MXT.propertyData.propertyPurpose, '-', ' ')) AS PROPERTY_PURPOSE,",
                null_sql="CAST(NULL AS STRING) AS PROPERTY_PURPOSE,"
            ),
        "propertyData.propertyOwnership": 
            SqlPair(
                active_sql="INITCAP(REGEXP_REPLACE(MXT.propertyData.propertyOwnership, '-', ' ')) AS PROPERTY_OWNERSHIP,", 
                null_sql="CAST(NULL AS STRING) AS PROPERTY_OWNERSHIP,"
            ),
        "propertyData.propertyType": 
            SqlPair(
                active_sql="INITCAP(REGEXP_REPLACE(MXT.propertyData.propertyType, '-', ' ')) AS PROPERTY_TYPE,",
                null_sql="CAST(NULL AS STRING) AS PROPERTY_TYPE,"
            ),
        "propertyData.propertyLocation": 
            SqlPair(
                active_sql="MXT.propertyData.propertyLocation AS PROPERTY_LOCATION,", 
                null_sql="CAST(NULL AS STRING) AS PROPERTY_LOCATION,"
            ),
        "propertyData.propertyRegion": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.propertyRegion) AS PROPERTY_REGION,", 
                null_sql="CAST(NULL AS STRING) AS PROPERTY_REGION,"
            ),
        "propertyData.tenure": 
            SqlPair(
                active_sql="INITCAP(REGEXP_REPLACE(MXT.propertyData.tenure, '-', ' ')) AS TENURE,",
                null_sql="CAST(NULL AS STRING) AS TENURE,"
            ),
        "propertyData.yearPropertyBuilt": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.yearPropertyBuilt AS SMALLINT) AS YEAR_PROPERTY_BUILT,", 
                null_sql="CAST(NULL AS SMALLINT) AS YEAR_PROPERTY_BUILT,"
            ),
        "propertyData.propertyFloors": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.propertyFloors AS SMALLINT) AS PROPERTY_FLOORS,", 
                null_sql="CAST(NULL AS SMALLINT) AS PROPERTY_FLOORS,"
            ),
        "propertyData.propertyBedrooms": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.propertyBedrooms AS SMALLINT) AS PROPERTY_BEDROOMS,", 
                null_sql="CAST(NULL AS SMALLINT) AS PROPERTY_BEDROOMS,"
            ),
        "propertyData.habitableRooms": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.habitableRooms AS SMALLINT) AS HABITABLE_ROOMS,", 
                null_sql="CAST(NULL AS SMALLINT) AS HABITABLE_ROOMS,"
            ),
        "propertyData.propertyBathrooms": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.propertyBathrooms AS SMALLINT) AS PROPERTY_BATHROOMS,", 
                null_sql="CAST(NULL AS SMALLINT) AS PROPERTY_BATHROOMS,"
            ),
        "propertyData.propertyReceptions": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.propertyReceptions AS SMALLINT) AS PROPERTY_RECEPTIONS,", 
                null_sql="CAST(NULL AS SMALLINT) AS PROPERTY_RECEPTIONS,"
            ),
        "propertyData.propertyKitchens": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.propertyKitchens AS SMALLINT) AS PROPERTY_KITCHENS,", 
                null_sql="CAST(NULL AS SMALLINT) AS PROPERTY_KITCHENS,"
            ),
        "propertyData.floorNumber": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.floorNumber AS SMALLINT) AS FLOOR_NUMBER,", 
                null_sql="CAST(NULL AS SMALLINT) AS FLOOR_NUMBER,"
            )
    }

    df_sql = "SELECT" + "\n"
    df_sql += "CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID," + "\n"
    df_sql += get_sql_line(select_dict, "propertyData.propertyPurpose", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyOwnership", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyType", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyLocation", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyRegion", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.tenure", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.yearPropertyBuilt", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyFloors", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyBedrooms", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.habitableRooms", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyBathrooms", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyReceptions", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.propertyKitchens", cols["propertyData_set"])
    df_sql += get_sql_line(select_dict, "propertyData.floorNumber", cols["propertyData_set"])

    df_sql += f"""
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT

    FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    """

    df = spark.sql(df_sql)

    # Commented out block below shows original hard-coded SQL for reference

    #df = spark.sql(f"""
    #SELECT
    #    CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID,
    #    MXT.propertyData.propertyPurpose AS PROPERTY_PURPOSE,
    #    MXT.propertyData.propertyOwnership AS PROPERTY_OWNERSHIP,
    #    MXT.propertyData.propertyType AS PROPERTY_TYPE,
    #    MXT.propertyData.propertyLocation AS PROPERTY_LOCATION,
    #    MXT.propertyData.propertyRegion AS PROPERTY_REGION,
    #    MXT.propertyData.tenure AS TENURE,
    #    CAST(MXT.propertyData.yearPropertyBuilt AS SMALLINT) AS YEAR_PROPERTY_BUILT,
    #    CAST(MXT.propertyData.propertyFloors AS SMALLINT) AS PROPERTY_FLOORS,
    #    CAST(MXT.propertyData.propertyBedrooms AS SMALLINT) AS PROPERTY_BEDROOMS,
    #    CAST(MXT.propertyData.habitableRooms AS SMALLINT) AS HABITABLE_ROOMS,
    #    CAST(MXT.propertyData.propertyBathrooms AS SMALLINT) AS PROPERTY_BATHROOMS,
    #    CAST(MXT.propertyData.propertyReceptions AS SMALLINT) AS PROPERTY_RECEPTIONS,
    #    CAST(MXT.propertyData.propertyKitchens AS SMALLINT) AS PROPERTY_KITCHENS,        
    #    CAST(MXT.propertyData.floorNumber AS SMALLINT) AS FLOOR_NUMBER,
    #    CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
    #    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    #    CAST(1 AS INT) AS ROW_IS_CURRENT

    #FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    #"""
    #)

    return df

# COMMAND ----------

@dlt.table(
    name="meds_property_title", 
    comment="Stores property title data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that MORTGAGE_ACCOUNT_ID is not null", "MORTGAGE_ACCOUNT_ID IS NOT NULL")
def meds_property_title():

    cols = get_meds_column_content_schema(env_var)

    titleAndPrefixPresent = True if "propertyData.title.prefixAndNumber" in cols["title_set"] else False

    df_sql = "SELECT" + "\n"
    df_sql += "CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID," + "\n"

    if titleAndPrefixPresent:
        df_sql += "    propertyDataTitlePrefixAndNumber AS PREFIX_AND_NUMBER," + "\n"
    else:
        df_sql += "    CAST(NULL AS STRING) AS PREFIX_AND_NUMBER," + "\n"

    df_sql += f"""
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT

    FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT

    """

    if titleAndPrefixPresent:
        df_sql += "LATERAL VIEW explode(MXT.propertyData.title.prefixAndNumber) AS propertyDataTitlePrefixAndNumber" + "\n"

    df = spark.sql(df_sql)

    # Commented out block below shows original hard-coded SQL for reference

    #df = spark.sql(f"""
    #SELECT
    #    CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID,
    #    propertyDataTitlePrefixAndNumber AS PREFIX_AND_NUMBER,
    #    CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
    #    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    #    CAST(1 AS INT) AS ROW_IS_CURRENT

    #FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    
    #LATERAL VIEW explode(MXT.propertyData.title.prefixAndNumber) AS propertyDataTitlePrefixAndNumber
    #"""
    #)

    return df

# COMMAND ----------

@dlt.table(
    name="meds_uk_address", 
    comment="Stores uk address data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that MORTGAGE_ACCOUNT_ID is not null", "MORTGAGE_ACCOUNT_ID IS NOT NULL")
def meds_uk_address():

    cols = get_meds_column_content_schema(env_var)

    select_dict ={
        "propertyData.ukAddress.addressType": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.ukAddress.addressType) AS ADDRESS_TYPE,", 
                null_sql="CAST(NULL AS STRING) AS ADDRESS_TYPE,"
            ),
        "propertyData.ukAddress.buildingNumber": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.ukAddress.buildingNumber AS STRING) AS BUILDING_NUMBER,", 
                null_sql="CAST(NULL AS STRING) AS BUILDING_NUMBER,"
            ),
        "propertyData.ukAddress.buildingName": 
            SqlPair(
                active_sql="MXT.propertyData.ukAddress.buildingName AS BUILDING_NAME,", 
                null_sql="CAST(NULL AS STRING) AS BUILDING_NAME,"
            ),
        "propertyData.ukAddress.flat": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.ukAddress.flat) AS FLAT,", 
                null_sql="CAST(NULL AS STRING) AS FLAT,"
            ),
        "propertyData.ukAddress.street": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.ukAddress.street) AS STREET,", 
                null_sql="CAST(NULL AS STRING) AS STREET,"
            ),
        "propertyData.ukAddress.district": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.ukAddress.district) AS DISTRICT,", 
                null_sql="CAST(NULL AS STRING) AS DISTRICT,"
            ),
        "propertyData.ukAddress.town": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.ukAddress.town) AS TOWN,", 
                null_sql="CAST(NULL AS STRING) AS TOWN,"
            ),
        "propertyData.ukAddress.county": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.ukAddress.county) AS COUNTY,", 
                null_sql="CAST(NULL AS STRING) AS COUNTY,"
            ),
        "propertyData.ukAddress.postcode": 
            SqlPair(
                active_sql="MXT.propertyData.ukAddress.postcode AS POSTCODE,", 
                null_sql="CAST(NULL AS STRING) AS POSTCODE,"
            )
    }

    df_sql = "SELECT" + "\n"
    df_sql += "CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID," + "\n"
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.addressType", cols["ukAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.buildingNumber", cols["ukAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.buildingName", cols["ukAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.flat", cols["ukAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.street", cols["ukAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.district", cols["ukAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.town", cols["ukAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.county", cols["ukAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.ukAddress.postcode", cols["ukAddress_set"])

    df_sql += f"""
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
        
    FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    """

    df = spark.sql(df_sql)

    # Commented out block below shows original hard-coded SQL for reference

    #df = spark.sql(f"""
    #SELECT
    #    CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID,
    #    MXT.propertyData.ukAddress.addressType AS ADDRESS_TYPE,
    #    CAST(MXT.propertyData.ukAddress.buildingNumber AS INT) AS BUILDING_NUMBER,
    #    MXT.propertyData.ukAddress.buildingName AS BUILDING_NAME,
    #    MXT.propertyData.ukAddress.flat AS FLAT,
    #    MXT.propertyData.ukAddress.street AS STREET,
    #    MXT.propertyData.ukAddress.district AS DISTRICT,
    #    MXT.propertyData.ukAddress.town AS TOWN,
    #    MXT.propertyData.ukAddress.county AS COUNTY,
    #    MXT.propertyData.ukAddress.postcode AS POSTCODE,
    #    CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
    #    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    #    CAST(1 AS INT) AS ROW_IS_CURRENT
    
    #FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    #"""
    #)

    return df

# COMMAND ----------

@dlt.table(
    name="meds_valuation", 
    comment="Stores property valuation data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that MORTGAGE_ACCOUNT_ID is not null", "MORTGAGE_ACCOUNT_ID IS NOT NULL")
def meds_valuation():

    cols = get_meds_column_content_schema(env_var)

    select_dict ={
        "propertyData.valuation.latestValuationAmount": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.valuation.latestValuationAmount AS DECIMAL(13,2)) AS LATEST_VALUATION_AMOUNT,", 
                null_sql="CAST(NULL AS DECIMAL(13,2)) AS LATEST_VALUATION_AMOUNT,"
            ),
        "propertyData.valuation.latestValuationDate": 
            SqlPair(
                active_sql="TO_TIMESTAMP(MXT.propertyData.valuation.latestValuationDate) AS LATEST_VALUATION_DATE,", 
                null_sql="CAST(NULL AS TIMESTAMP) AS LATEST_VALUATION_DATE,"
            ),
        "propertyData.valuation.propertyPurchasePrice": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.valuation.propertyPurchasePrice AS DECIMAL(13,2)) AS PROPERTY_PURCHASE_PRICE,", 
                null_sql="CAST(NULL AS DECIMAL(13,2)) AS PROPERTY_PURCHASE_PRICE,"
            ),
        "propertyData.valuation.latestAutomatedValuationAmount": 
            SqlPair(
                active_sql="CAST(MXT.propertyData.valuation.latestAutomatedValuationAmount AS DECIMAL(13,2)) AS LATEST_AUTOMATED_VALUATION_AMOUNT,", 
                null_sql="CAST(NULL AS DECIMAL(13,2)) AS LATEST_AUTOMATED_VALUATION_AMOUNT,"
            ),
        "propertyData.valuation.latestAutomatedValuationDate": 
            SqlPair(
                active_sql="TO_TIMESTAMP(MXT.propertyData.valuation.latestAutomatedValuationDate) AS LATEST_AUTOMATED_VALUATION_DATE,", 
                null_sql="CAST(NULL AS TIMESTAMP) AS LATEST_AUTOMATED_VALUATION_DATE,"
            )
    }  

    df_sql = "SELECT" + "\n"
    df_sql += "CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID," + "\n"
    df_sql += get_sql_line(select_dict, "propertyData.valuation.latestValuationAmount", cols["valuation_set"])
    df_sql += get_sql_line(select_dict, "propertyData.valuation.latestValuationDate", cols["valuation_set"])
    df_sql += get_sql_line(select_dict, "propertyData.valuation.propertyPurchasePrice", cols["valuation_set"])
    df_sql += get_sql_line(select_dict, "propertyData.valuation.latestAutomatedValuationAmount", cols["valuation_set"])
    df_sql += get_sql_line(select_dict, "propertyData.valuation.latestAutomatedValuationDate", cols["valuation_set"])

    df_sql += f"""
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
        
    FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    """

    df = spark.sql(df_sql)

    # Commented out block below shows original hard-coded SQL for reference

    #df = spark.sql(f"""
    #SELECT
    #    CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID,
    #    CAST(MXT.propertyData.valuation.latestValuationAmount AS DECIMAL(13,2)) AS LATEST_VALUATION_AMOUNT,
    #    TO_TIMESTAMP(MXT.propertyData.valuation.latestValuationDate) AS LATEST_VALUATION_DATE,
    #    CAST(MXT.propertyData.valuation.propertyPurchasePrice AS DECIMAL(13,2)) AS PROPERTY_PURCHASE_PRICE,
    #    CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
    #    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    #    CAST(1 AS INT) AS ROW_IS_CURRENT
    
    #FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    #"""
    #)

    return df

# COMMAND ----------

@dlt.table(
    name="meds_application_data", 
    comment="Stores application data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that MORTGAGE_ACCOUNT_ID is not null", "MORTGAGE_ACCOUNT_ID IS NOT NULL")
def meds_application_data():

    cols = get_meds_column_content_schema(env_var)

    appDataAndMsoCaseIdsPresent = True if "applicationData.msoCaseIdentifiers" in cols["applicationData_set"] else False

    df_sql = "SELECT" + "\n"
    df_sql += "CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID," + "\n"

    if appDataAndMsoCaseIdsPresent:
        df_sql += "    propertyApplicationDataMsoCaseIdentifiers AS MSO_CASE_IDENTIFIER," + "\n"
    else:
        df_sql += "    CAST(NULL AS STRING) AS MSO_CASE_IDENTIFIER," + "\n"

    df_sql += f"""
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
        
    FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT

    """

    if appDataAndMsoCaseIdsPresent:
        df_sql += "LATERAL VIEW explode(MXT.applicationData.msoCaseIdentifiers) AS propertyApplicationDataMsoCaseIdentifiers" + "\n"

    df = spark.sql(df_sql)

    # Commented out block below shows original hard-coded SQL for reference

    #df = spark.sql(f"""
    #SELECT
    #    CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID,
    #    propertyApplicationDataMsoCaseIdentifiers AS MSO_CASE_IDENTIFIER,
    #    CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
    #    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    #    CAST(1 AS INT) AS ROW_IS_CURRENT
    
    #FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    
    #LATERAL VIEW explode(MXT.applicationData.msoCaseIdentifiers) AS propertyApplicationDataMsoCaseIdentifiers
    #"""
    #)

    return df

# COMMAND ----------

@dlt.table(
    name="meds_overseas_address", 
    comment="Stores overseas address data for a mortgage from the mortgage extended data store"
)
@dlt.expect("Check that MORTGAGE_ACCOUNT_ID is not null", "MORTGAGE_ACCOUNT_ID IS NOT NULL")
def meds_overseas_address():

    cols = get_meds_column_content_schema(env_var)

    select_dict ={
        "propertyData.overseaAddress.addressLine1": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.overseaAddress.addressLine1) AS ADDRESS_LINE_1,", 
                null_sql="CAST(NULL AS STRING) AS ADDRESS_LINE_1,"
            ),
        "propertyData.overseaAddress.addressLine2": 
            SqlPair(
                active_sql="INITCAP(CAST(MXT.propertyData.overseaAddress.addressLine2 AS STRING)) AS ADDRESS_LINE_2,", 
                null_sql="CAST(NULL AS STRING) AS ADDRESS_LINE_2,"
            ),
        "propertyData.overseaAddress.addressLine3": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.overseaAddress.addressLine3) AS ADDRESS_LINE_3,", 
                null_sql="CAST(NULL AS STRING) AS ADDRESS_LINE_3,"
            ),
        "propertyData.overseaAddress.addressLine4": 
            SqlPair(
                active_sql="INITCAP(MXT.propertyData.overseaAddress.addressLine4) AS ADDRESS_LINE_4,", 
                null_sql="CAST(NULL AS STRING) AS ADDRESS_LINE_4,"
            ),
        "propertyData.overseaAddress.countryTypeCode": 
            SqlPair(
                active_sql="MXT.propertyData.overseaAddress.countryTypeCode AS COUNTRY_TYPE_CODE,", 
                null_sql="CAST(NULL AS STRING) AS COUNTRY_TYPE_CODE,"
            )
    }

    df_sql = "SELECT" + "\n"
    df_sql += "CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID," + "\n"
    df_sql += get_sql_line(select_dict, "propertyData.overseaAddress.addressLine1", cols["overseaAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.overseaAddress.addressLine2", cols["overseaAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.overseaAddress.addressLine3", cols["overseaAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.overseaAddress.addressLine4", cols["overseaAddress_set"])
    df_sql += get_sql_line(select_dict, "propertyData.overseaAddress.countryTypeCode", cols["overseaAddress_set"])

    df_sql += f"""
        CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
        CAST(1 AS INT) AS ROW_IS_CURRENT
        
    FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    """

    df = spark.sql(df_sql)

    # Commented out block below shows original hard-coded SQL for reference

    #df = spark.sql(f"""
    #SELECT
    #    CAST(MXT.mortgageAccountId AS STRING) AS MORTGAGE_ACCOUNT_ID,
    #    MXT.propertyData.overseaAddress.addressLine1 AS ADDRESS_LINE_1,
    #    MXT.propertyData.ukAddress.addressLine2 AS ADDRESS_LINE_2,
    #    MXT.propertyData.ukAddress.addressLine3 AS ADDRESS_LINE_3,
    #    MXT.propertyData.ukAddress.addressLine4 AS ADDRESS_LINE_4,
    #    MXT.propertyData.ukAddress.countryTypeCode AS COUNTRY_TYPE_CODE,
    #    CAST(MXT.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
    #    TO_TIMESTAMP(MXT.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
    #    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    #    CAST(1 AS INT) AS ROW_IS_CURRENT
    
    #FROM {env_var}_catalog.bronze_meds_con.mortgage_extended AS MXT
    #"""
    #)

    return df
