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

from datetime import datetime
from zoneinfo import ZoneInfo

# Define the timezone to Europe/London
tz = ZoneInfo('Europe/London')

# Check if today is the first day of the month
if datetime.now(tz).day == 1:

    try:
        fact = FactFactory(
            catalog_name=catalog_name,
            table_schema='gold_con',
            table_name='bridge_party_account',
            source_query=None,
            periodic_snapshot_table_name='bridge_party_account_history'
        ).create(FactType.PERIODIC_SNAPSHOT_MONTH)
        fact.load()
    except Exception as e:
        dbutils.jobs.taskValues.set("error_detail", str(e))
        raise(e) 
    
else:
    print("Today is not the first day of the month. Skipping the load process.")

