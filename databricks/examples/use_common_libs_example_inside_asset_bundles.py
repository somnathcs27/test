# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example of Common Libraries in Asset Bundles (non DLT)
# MAGIC This notebook is designed to be run inside an asset bundle, where the mdp_databricks_common lib has been pulled in via asset bundle config.
# MAGIC This is a non-DLT example where the whl file can be imported in the asset bundle config, instead of in a separate notebook as is necessary for DLT.

# COMMAND ----------

import mdp_databricks_common.bronze_data_load_functions as dlf

# The loopback module is a demo module with a hello_world() trivial function. Not to be used in production code.
import mdp_databricks_common.loopback as lb

# The next two lines just list the contents of one of the modules - not needed to run anything, but useful for finding out which functions are available.
module_contents = dir(dlf)
print(module_contents)

# This line calls a function from the imported loopback module.
lb.hello_world()
