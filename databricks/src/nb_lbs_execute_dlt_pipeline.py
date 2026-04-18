# Databricks notebook source
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("child_pipeline", "")
dbutils.widgets.text("env_var", "")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

if catalog_name == 'dev_catalog':
    %pip uninstall '/Volumes/dev_catalog/common_libraries/mdp_databricks_common_3_3_0/mdp_databricks_common-3.3.0-py3-none-any.whl' -y
    %pip install '/Volumes/dev_catalog/common_libraries/mdp_databricks_common_3_3_0/mdp_databricks_common-3.3.0-py3-none-any.whl'
elif catalog_name == 'sit_catalog':
    %pip uninstall '/Volumes/sit_catalog/common_libraries/mdp_databricks_common_3_3_0/mdp_databricks_common-3.3.0-py3-none-any.whl' -y
    %pip install '/Volumes/sit_catalog/common_libraries/mdp_databricks_common_3_3_0/mdp_databricks_common-3.3.0-py3-none-any.whl'
elif catalog_name == 'prod_catalog':
    %pip uninstall '/Volumes/prod_catalog/common_libraries/mdp_databricks_common_3_3_0/mdp_databricks_common-3.3.0-py3-none-any.whl' -y
    %pip install '/Volumes/prod_catalog/common_libraries/mdp_databricks_common_3_3_0/mdp_databricks_common-3.3.0-py3-none-any.whl'
else:
    raise Exception(f"Catalog name not recognised: {catalog_name}")

dbutils.library.restartPython()

# COMMAND ----------

import mdp_databricks_common.dlt_pipeline_execution_functions as p

env_var = dbutils.widgets.get("env_var")
job_name = dbutils.widgets.get("child_pipeline")

p.execute_dlt_pipeline(dbutils, job_name, env_var)
