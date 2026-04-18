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

# DBTITLE 1,REFRESH bridge_mortgage_case_mortgage_account
source_query = f"""
  SELECT 
      dma.PK_MORTGAGE_ACCOUNT AS FK_MORTGAGE_ACCOUNT
    ,dmc.PK_MORTGAGE_CASE AS FK_MORTGAGE_CASE
    ,fmc.FK_CREATED_DATE AS CREATED_DATE
    ,dma.ACCOUNT_NUMBER
    ,dmc.APPLICATION_NUMBER
    ,dmc.APPLICATION_TYPE
  FROM {catalog_name}.silver_con.meds_application_data app
  INNER JOIN {catalog_name}.gold_con.dim_mortgage_account dma
  ON app.MORTGAGE_ACCOUNT_ID = dma.BK_MORTGAGE_ACCOUNT
  AND app.ROW_IS_CURRENT = 1
  AND dma.ROW_IS_CURRENT = 1
  INNER JOIN {catalog_name}.gold_con.dim_mortgage_case dmc
  ON app.MSO_CASE_IDENTIFIER = dmc.FRIENDLY_ID
  AND app.ROW_IS_CURRENT = 1
  AND dmc.ROW_IS_CURRENT = 1
  INNER JOIN {catalog_name}.gold_int.fact_mortgage_case fmc
  ON dmc.PK_MORTGAGE_CASE = fmc.FK_MORTGAGE_CASE
"""

try:
  fact = FactFactory(catalog_name, 'gold_con', 'bridge_mortgage_case_mortgage_account', source_query).create(FactType.FULL_LOAD)
  fact.load()
except Exception as e:
  dbutils.jobs.taskValues.set("error_detail", str(e))
  raise(e)
