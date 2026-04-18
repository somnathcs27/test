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

from mdp_databricks_common.facts.fact_factory import FactFactory
from mdp_databricks_common.enums.enums import FactType

# COMMAND ----------

# DBTITLE 1,REFRESH bridge_party_account
source_query = f"""
   SELECT 
      COALESCE(dc.PK_CUSTOMER, -2) AS FK_CUSTOMER,
      dsa.PK_SAVING_ACCOUNT AS FK_ACCOUNT,
      dc.CUSTOMER_NUMBER,
      dsa.SAVING_ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
      CAST('Saving' AS STRING) AS ACCOUNT_TYPE,
      COALESCE(CAST(dca.CUSTOMER_ACCOUNT_HOLDER_TYPE AS STRING), 'Unknown') AS ACCOUNT_HOLDER_TYPE,
      TRY_CAST(dca.LBS_START_DATE AS DATE) AS RELATIONSHIP_START_DATE,
      TRY_CAST(dca.LBS_END_DATE AS DATE) AS RELATIONSHIP_END_DATE
    -- Join together Dataverse customers to their customer account links
    FROM {catalog_name}.gold_con.dim_customer dc
    INNER JOIN {catalog_name}.silver_con.dataverse_customer_account dca
      ON dc.BK_CUSTOMER  = dca.CUSTOMER_ACCOUNT_KEY
    -- inner join to the savings account - we only want to return records that have a customer to account relationship
    INNER JOIN {catalog_name}.gold_con.dim_saving_account dsa
      ON dca.ACCOUNT_NUMBER_AND_SUFFIX = dsa.SAVING_ACCOUNT_NUMBER_AND_SUFFIX 
    WHERE dc.ROW_IS_CURRENT = 1
      AND dca.ROW_IS_CURRENT = 1
      AND dsa.ROW_IS_CURRENT = 1

    UNION 

    SELECT 
      COALESCE(dc.PK_CUSTOMER, -2) AS FK_CUSTOMER,
      dma.PK_MORTGAGE_ACCOUNT AS FK_ACCOUNT,
      dc.CUSTOMER_NUMBER,
      dma.ACCOUNT_NUMBER,
      CAST('Mortgage' AS STRING) AS ACCOUNT_TYPE,           
      COALESCE(CAST(dca.CUSTOMER_ACCOUNT_HOLDER_TYPE AS STRING), 'Unknown') AS ACCOUNT_HOLDER_TYPE,
      TRY_CAST(dca.LBS_START_DATE AS DATE) AS RELATIONSHIP_START_DATE,
      TRY_CAST(dca.LBS_END_DATE AS DATE) AS RELATIONSHIP_END_DATE
    -- Join together Dataverse customers to their customer account links
    FROM {catalog_name}.gold_con.dim_customer dc
    INNER JOIN {catalog_name}.silver_con.dataverse_customer_account dca
      ON dc.BK_CUSTOMER  = dca.CUSTOMER_ACCOUNT_KEY
    -- inner join to the mortgage account - we only want to return records that have a customer to account relationship
    INNER JOIN {catalog_name}.gold_con.dim_mortgage_account dma
      ON dca.ACCOUNT_NUMBER_AND_SUFFIX = dma.ACCOUNT_NUMBER
      AND dca.ACCOUNT_SORTCODE = dma.SORT_CODE
    WHERE dc.ROW_IS_CURRENT = 1
      AND dca.ROW_IS_CURRENT = 1
      AND dma.ROW_IS_CURRENT = 1
"""

try:
  fact = FactFactory(catalog_name, 'gold_con', 'bridge_party_account', source_query).create(FactType.FULL_LOAD)
  fact.load()
except Exception as e:
  dbutils.jobs.taskValues.set("error_detail", str(e))
  raise(e)  
 
