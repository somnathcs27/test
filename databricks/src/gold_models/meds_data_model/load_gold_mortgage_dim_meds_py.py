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

# COMMAND ----------

# --------------------------
# dim_mortgage_property
# --------------------------
source_query = f"""
    SELECT 
        CAST(mp.MORTGAGE_ACCOUNT_ID AS BIGINT) AS BK_MORTGAGE_PROPERTY --MORTGAGE_ACCOUNT_ID needs to be replaced with the property ID (GUID) if/when available
        ,COALESCE(CAST(mp.PROPERTY_PURPOSE AS STRING), 'Unknown') AS PURPOSE
        ,COALESCE(CAST(mp.PROPERTY_OWNERSHIP AS STRING), 'Unknown') AS OWNERSHIP
        ,COALESCE(CAST(mp.PROPERTY_TYPE AS STRING), 'Unknown') AS PROPERTY_TYPE
        ,COALESCE(CAST(mp.PROPERTY_LOCATION AS STRING), 'Unknown') AS LOCATION
        ,COALESCE(CAST(mp.PROPERTY_REGION AS STRING), 'Unknown') AS REGION
        ,COALESCE(CAST(mp.TENURE AS STRING), 'Unknown') AS TENURE
        ,COALESCE(CAST(mp.YEAR_PROPERTY_BUILT AS INTEGER), -2) AS YEAR_PROPERTY_BUILT
        ,COALESCE(CAST(mp.PROPERTY_FLOORS AS INTEGER), -2) AS NUMBER_OF_FLOORS
        ,COALESCE(CAST(mp.PROPERTY_BEDROOMS AS INTEGER), -2) AS NUMBER_OF_BEDROOMS
        ,COALESCE(CAST(mp.HABITABLE_ROOMS AS TINYINT), -2) AS NUMBER_OF_HABITABLE_ROOMS
        ,COALESCE(CAST(mp.PROPERTY_BATHROOMS AS TINYINT), -2) AS NUMBER_OF_BATHROOMS
        ,COALESCE(CAST(mp.PROPERTY_RECEPTIONS AS INTEGER), -2) AS NUMBER_OF_RECEPTIONS
        ,COALESCE(CAST(mp.PROPERTY_KITCHENS AS INTEGER), -2) AS NUMBER_OF_KITCHENS
        ,COALESCE(CAST(mp.FLOOR_NUMBER AS STRING), 'Unknown') AS FLOOR_NUMBER_FLAT
        ,COALESCE(CAST(ma.ADDRESS_TYPE AS STRING), 'Unknown') AS ADDRESS_TYPE
        ,COALESCE(CAST(ma.BUILDING_NUMBER AS STRING), 'Unknown') AS BUILDING_NUMBER
        ,COALESCE(CAST(ma.BUILDING_NAME AS STRING), 'Unknown') AS BUILDING_NAME
        ,COALESCE(CAST(ma.FLAT AS STRING), 'Unknown') AS FLAT_IDENTIFIER
        ,COALESCE(CAST(ma.STREET AS STRING), 'Unknown') AS STREET
        ,COALESCE(CAST(ma.DISTRICT AS STRING), 'Unknown') AS DISTRICT
        ,COALESCE(CAST(ma.TOWN AS STRING), 'Unknown') AS TOWN
        ,COALESCE(CAST(ma.COUNTY AS STRING), 'Unknown') AS COUNTY
        ,COALESCE(CAST(ma.POSTCODE AS STRING), 'Unknown') AS PROPERTY_POSTCODE
        ,COALESCE(SUBSTR(REPLACE(ma.POSTCODE, ' ', ''), 1, LENGTH(REPLACE(ma.POSTCODE, ' ', '')) - 3), 'Unknown') AS OUTWARD_POSTCODE
        ,LTRIM(
                COALESCE(
                    CONCAT(
                        COALESCE(ma.FLAT, ''), ' '
                        ,COALESCE(ma.BUILDING_NUMBER, ''), ' '
                        ,COALESCE(ma.BUILDING_NAME, ''), ' '
                        ,COALESCE(ma.STREET, ''), ' '
                        ,COALESCE(ma.DISTRICT, ''), ' '
                        ,COALESCE(ma.TOWN, ''), ' '
                        ,COALESCE(ma.COUNTY, '')
                    ), 'Unknown'
                )) AS PROPERTY_ADDRESS
        ,CAST('MEDS' AS STRING) AS SOURCE_SYSTEM
        ,mp.MDP_LOAD_ID
        ,mp.MDP_LOAD_DATETIME
        ,mp.ROW_START_DATETIME
        ,mp.ROW_END_DATETIME
        ,mp.ROW_IS_CURRENT
    FROM {catalog_name}.silver_con.meds_property mp
    INNER JOIN {catalog_name}.silver_con.meds_uk_address ma
    ON mp.MORTGAGE_ACCOUNT_ID = ma.MORTGAGE_ACCOUNT_ID
    AND mp.ROW_IS_CURRENT = 1
    AND ma.ROW_IS_CURRENT = 1
"""

try:
    dim =  DimensionFactory(catalog_name, 'gold_con','dim_mortgage_property',source_query).create(SCDType.TYPE2)
    dim.load()
except Exception as e:
    dbutils.jobs.taskValues.set("error_detail", str(e))
    raise(e)          

