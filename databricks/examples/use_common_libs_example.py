# Databricks notebook source
# MAGIC %md
# MAGIC # Common Library Example
# MAGIC This notebook documents how to use the common libraries in notebooks.
# MAGIC
# MAGIC There are two modes for using common libs:
# MAGIC 1. Development mode (outside asset-bundles)
# MAGIC 2. Production mode (inside asset-bundles)
# MAGIC
# MAGIC ## Production Mode
# MAGIC
# MAGIC When running in Production mode (can be done on dev/SIT/Prod) the library dependencies are defined in the asset bundle config file in a section like this:
# MAGIC ```
# MAGIC name: my_cunning_bundle
# MAGIC libraries:
# MAGIC   - whl: dbfs:/Volumes/dev_catalog/common_libraries/mdp_databricks_common_0_2/mdp_databricks_common-0.2-py3-none-any.whl
# MAGIC ```
# MAGIC
# MAGIC It should therefore be possible to simply import using code like this:
# MAGIC ```
# MAGIC import mdp_databricks_common.bronze_data_load_functions as dlf
# MAGIC ```
# MAGIC
# MAGIC ## Development Mode
# MAGIC
# MAGIC However, before deploying your code in the context of an asset bundle if you need a common library, you'll need to install in the notebook using %pip magic commands like this:
# MAGIC
# MAGIC ```
# MAGIC %pip install '/Volumes/dev_catalog/common_libraries/mdp_databricks_common_0_2/mdp_databricks_common-0.2-py3-none-any.whl'
# MAGIC dbutils.library.restartPython()
# MAGIC ```
# MAGIC
# MAGIC After the %pip install, the modules can be imported as normal.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked Examples
# MAGIC See the cells below for a working demonstration of this. You should be able to run the cells and see it run.

# COMMAND ----------

# MAGIC %pip install '/Volumes/dev_catalog/common_libraries/mdp_databricks_common_0_2/mdp_databricks_common-0.2-py3-none-any.whl'
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mdp_databricks_common.bronze_data_load_functions as dlf

# The loopback module is a demo module with a hello_world() trivial function. Not to be used in production code.
import mdp_databricks_common.loopback as lb

# The next two lines just list the contents of one of the modules - not needed to run anything, but useful for finding out which functions are available.
module_contents = dir(dlf)
print(module_contents)

# This line calls a function from the imported loopback module.
lb.hello_world()
