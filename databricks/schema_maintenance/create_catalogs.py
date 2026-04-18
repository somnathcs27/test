# Databricks notebook source
# MAGIC %md
# MAGIC # Catalog Maintenance
# MAGIC Manages the creation of any new catalogs

# COMMAND ----------

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
# MAGIC # Create Catalogs
# MAGIC The cell below is intended to create based upon a distinct list that can grow over time
# MAGIC

# COMMAND ----------

# Initial array to create the base catalogs I.E. non environmentally specific catalogs
base_catalogs = [
    "_catalog",
    "_logs"
]

# Append the environment name to the front of each of our base catalogs
create_catalogs = [f"{env_var}{catalog}" for catalog in base_catalogs]

#create any catalogs that don't already exist
for catalog in create_catalogs:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")


# Use this configuration entry to drop catalogs.
drop_base_catalogs = []

drop_catalogs = [f"{env_var}{catalog}" for catalog in drop_base_catalogs]

for catalog in drop_catalogs:
    spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")    

