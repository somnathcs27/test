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

source_query = f"""
  SELECT 
    dvc.CUSTOMER_ID AS BK_CUSTOMER,
    COALESCE(dvc.CUSTOMER_NUMBER,0) AS CUSTOMER_NUMBER,
    COALESCE(dogm_title.LOCALIZED_LABEL,'Unknown') AS TITLE,
    COALESCE(dvc.FORENAMES,'Unknown') AS FORENAME,
    COALESCE(dvc.MIDDLE_NAME,'Unknown') AS MIDDLE_NAMES,
    COALESCE(dvc.SURNAME,'Unknown') AS SURNAME,
    COALESCE(dvc.INITIALS,'Unknown') AS INITIALS,
    COALESCE(dom.LOCALIZED_LABEL,'Unknown') AS GENDER,
    CAST(dvc.DATE_OF_BIRTH AS DATE) AS DATE_OF_BIRTH,
    IFF(TRY_CAST(dvc.AGE AS TINYINT) IS NULL,-1,COALESCE(dvc.AGE,-2)) AS AGE,
    COALESCE(dogm_nationality.LOCALIZED_LABEL,'Unknown') AS NATIONALITY,
    COALESCE(dvc.NI_NUMBER,'Unknown') AS NI_NUMBER,
    COALESCE(dvc.EMAIL_ADDRESS,'Unknown') AS EMAIL_ADDRESS,
    COALESCE(pcm.LOCALIZED_LABEL,'Unknown') AS PREFERRED_CONTACT_METHOD,
    COALESCE(dvc.TELEPHONE_HOME,'Unknown') AS TELEPHONE_HOME,
    COALESCE(dvc.TELEPHONE_MOBILE,'Unknown') AS TELEPHONE_MOBILE,
    COALESCE(dvc.TELEPHONE_WORK,'Unknown') AS TELEPHONE_WORK,
    COALESCE(dvc.CUSTOMER_TYPE_CODE,'Unknown') AS TYPE_CODE, --this code links to dataverse_option_set_metadata (not yet implemented)
    dvc.CUSTOMER_START_DATE AS START_DATE,
    dvc.CUSTOMER_END_DATE AS END_DATE,
    'Dataverse' AS SOURCE_SYSTEM,
    dvc.MDP_LOAD_ID AS MDP_LOAD_ID,
    dvc.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME,
    dvc.ROW_START_DATETIME AS ROW_START_DATETIME,
    dvc.ROW_END_DATETIME AS ROW_END_DATETIME,
    dvc.ROW_IS_CURRENT AS ROW_IS_CURRENT    
  FROM {catalog_name}.silver_con.dataverse_contact dvc
  LEFT JOIN {catalog_name}.silver_int.dataverse_option_set_metadata dom
  ON dom.ENTITY_NAME = 'contact' 
  AND dom.option_set_name = 'gendercode' 
  AND dvc.GENDER = dom.OPTION
  LEFT JOIN {catalog_name}.silver_int.dataverse_option_set_metadata pcm
  ON pcm.ENTITY_NAME = 'contact' 
  AND pcm.option_set_name = 'preferredcontactmethodcode' 
  AND dvc.PREFERRED_CONTACT_METHOD = pcm.OPTION
  LEFT JOIN {catalog_name}.silver_int.dataverse_global_option_set_metadata dogm_title
  ON dogm_title.ENTITY_NAME = 'contact'  
  AND dogm_title.GLOBAL_OPTION_SET_NAME = 'lbs_title' 
  AND dogm_title.OPTION = dvc.TITLE
  LEFT JOIN {catalog_name}.silver_int.dataverse_global_option_set_metadata dogm_nationality
  ON dogm_nationality.ENTITY_NAME = 'contact'  
  AND dogm_nationality.GLOBAL_OPTION_SET_NAME = 'lbs_nationality' 
  AND dogm_nationality.OPTION = dvc.LBS_NATIONALITY
  LEFT JOIN {catalog_name}.silver_con.dataverse_country dc
  ON dc.COUNTRY_ID = dvc.COUNTRY 
  AND dc.ROW_IS_CURRENT = 1
  WHERE dvc.ROW_IS_CURRENT = 1
"""

try:
  dim =  DimensionFactory(catalog_name, 'gold_con','dim_customer',source_query).create(SCDType.TYPE2)
  dim.load()
except Exception as e:
  dbutils.jobs.taskValues.set("error_detail", str(e))
  raise(e)      


# COMMAND ----------

source_query = f"""
  SELECT 
    CAST(da.ADDRESS_ID AS STRING) AS BK_CUSTOMER_ADDRESS
    ,COALESCE(con.CUSTOMER_NUMBER,0) AS CUSTOMER_NUMBER
    ,COALESCE(addr_use.LOCALIZED_LABEL,'Unknown') AS USE
    ,CAST(COALESCE(addr_type.LOCALIZED_LABEL,'Unknown') AS STRING) AS TYPE
    ,CAST(COALESCE(da.LBS_ADDRESS1_FLAT,'Unknown') AS STRING) AS FLAT
    ,CAST(COALESCE(da.LBS_ADDRESS1_BUILDING_NUMBER,'Unknown') AS STRING) AS BUILDING_NUMBER
    ,CAST(
        CASE 
          WHEN addr_type.LOCALIZED_LABEL = 'BFPO Address' THEN COALESCE(da.LBS_ADDRESS1_BFPO_CARE_OF_SURNAME,'Unknown')
          WHEN addr_type.LOCALIZED_LABEL = 'UK Address' THEN COALESCE(da.LBS_ADDRESS1_BUILDING_NAME,'Unknown')
          ELSE 'Unknown'
          END AS STRING
          ) AS BUILDING_NAME
    ,CAST(
        CASE 
          WHEN addr_type.LOCALIZED_LABEL = 'UK Address' THEN COALESCE(da.LBS_ADDRESS1_LINE1,'Unknown')
          WHEN addr_type.LOCALIZED_LABEL = 'Overseas Address' THEN COALESCE(da.LBS_ADDRESS1_OVERSEAS_LINE1,'Unknown')
          WHEN addr_type.LOCALIZED_LABEL = 'BFPO Address' THEN COALESCE(da.LBS_ADDRESS1_BFPO_SERVICE_NUMBER,'Unknown')
          ELSE 'Unknown'
        END AS STRING 
        )AS STREET_NAME
    ,CAST( 
        CASE 
          WHEN addr_type.LOCALIZED_LABEL = 'UK Address' THEN COALESCE(da.LBS_ADDRESS1_CITY,'Unknown')
          WHEN addr_type.LOCALIZED_LABEL = 'Overseas Address' THEN COALESCE(da.LBS_ADDRESS1_OVERSEAS_LINE2,'Unknown') 
          WHEN addr_type.LOCALIZED_LABEL = 'BFPO Address' THEN COALESCE(da.LBS_ADDRESS1_BFPO_RANK,'Unknown')
          ELSE 'Unknown'
        END AS STRING
      )AS TOWN_NAME
    ,CAST( 
        CASE 
          WHEN addr_type.LOCALIZED_LABEL = 'UK Address' THEN COALESCE(da.LBS_ADDRESS1_STATE_OR_PROVINCE,'Unknown') 
          WHEN addr_type.LOCALIZED_LABEL = 'Overseas Address' THEN COALESCE(da.LBS_ADDRESS1_OVERSEAS_LINE3,'Unknown') 
          WHEN addr_type.LOCALIZED_LABEL = 'BFPO Address' THEN COALESCE(da.LBS_ADDRESS1_BFPO_UNIT,'Unknown')
          ELSE 'Unknown'
        END AS STRING
    )AS DISTRICT
    ,CAST( 
        CASE 
          WHEN addr_type.LOCALIZED_LABEL = 'UK Address' THEN COALESCE(da.LBS_ADDRESS1_COUNTY,'Unknown') 
          WHEN addr_type.LOCALIZED_LABEL = 'Overseas Address' THEN COALESCE(da.LBS_ADDRESS1_OVERSEAS_LINE4,'Unknown') 
          WHEN addr_type.LOCALIZED_LABEL = 'BFPO Address' THEN COALESCE(da.LBS_ADDRESS1_BFPO_OPERATION_NAME,'Unknown')
          ELSE 'Unknown'
        END AS STRING
    )AS COUNTY
    ,CAST(COALESCE(dc.COUNTRY_NAME ,'Unknown') AS STRING) AS COUNTRY
    ,CAST(COALESCE(da.LBS_ADDRESS1_POSTAL_CODE ,'Unknown') AS STRING) AS POSTAL_CODE
    ,COALESCE(dc.LBS_COUNTRY_CODE, 'Unknown') AS COUNTRY_CODE
    ,CAST(da.LBS_ADDRESS1_START_DATE AS DATE) AS START_DATE
    ,TRY_CAST(da.LBS_ADDRESS1_END_DATE AS DATE) AS END_DATE
    ,'Dataverse' AS SOURCE_SYSTEM
    ,da.MDP_LOAD_ID AS MDP_LOAD_ID
    ,da.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    ,da.ROW_START_DATETIME AS ROW_START_DATETIME
    ,da.ROW_END_DATETIME AS ROW_END_DATETIME
    ,da.ROW_IS_CURRENT AS ROW_IS_CURRENT     
  FROM {catalog_name}.silver_con.dataverse_address da
  LEFT JOIN {catalog_name}.silver_int.dataverse_global_option_set_metadata addr_type
  ON da.POSTCODE_ADDRESS_FILE_STATUS = addr_type.OPTION 
  AND addr_type.OPTION_SET_NAME = 'lbs_address1_typecode'
  AND addr_type.ROW_IS_CURRENT = 1
  LEFT JOIN {catalog_name}.silver_int.dataverse_global_option_set_metadata addr_use
  ON da.LBS_ADDRESS1_ADDRESS_USE = addr_use.OPTION 
  AND addr_use.OPTION_SET_NAME = 'lbs_address1_addressuse'
  AND addr_use.ROW_IS_CURRENT = 1
  LEFT JOIN {catalog_name}.silver_con.dataverse_country dc
  ON da.LBS_ADDRESS1_COUNTRY = dc.COUNTRY_ID
  AND dc.ROW_IS_CURRENT = 1
  LEFT JOIN {catalog_name}.silver_con.dataverse_contact con
  ON da.LBS_CUSTOMER_ID = con.CONTACT_ID
  AND con.ROW_IS_CURRENT = 1
  WHERE da.LBS_CUSTOMER_ID IS NOT NULL --exclude addresses with no customer
  AND da.LBS_CUSTOMER_ID_ENTITY_TYPE = 'contact' --only contact addresses
  AND da.ROW_IS_CURRENT = 1
  """

try:
  dim =  DimensionFactory(catalog_name, 'gold_con','dim_customer_address',source_query).create(SCDType.TYPE2)
  dim.load()
except Exception as e:
  dbutils.jobs.taskValues.set("error_detail", str(e))
  raise(e) 
