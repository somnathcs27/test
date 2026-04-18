# Databricks notebook source
# MAGIC %md
# MAGIC ## Example of Common Libraries in DLT with Asset Bundles
# MAGIC  - This notebook is designed to be run inside an asset bundle, where the mdp_databricks_common lib has been pulled in via an include notebook.
# MAGIC  - The asset-bundle is in resources_examples/common_lib_delta_live_table_example_deploy.py
# MAGIC  - The DLT asset-bundle includes a notebook called include_mdp_databricks_common_0_2.py which does a pip install on the lib
# MAGIC  - The include notebook is referenced alongside this one in the asset-bundle config.
# MAGIC  - The code below creates a "hello-world" materialized view dev_catalog.commonlib_target.hello_world_dlt which contains a string pulled out from a function in the loopback module to prove this works.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common-libs Do's and Dont's for MDP
# MAGIC ### Do's
# MAGIC   - Do use mdp_databricks_common library for code which is used by lots of different sources.
# MAGIC   - Do use mdp_databricks_common library for code which reusable and doesn't change very often (once every month or so)
# MAGIC   - Do include the mdp_databricks_common library using the pattern illustrated in the resources_examples/common_lib_delta_live_table_example_deploy.yml file.
# MAGIC ### Dont's
# MAGIC   - Don't use mdp_databricks_common library for code which is bespoked to one source. That kind of code should go in the same folder/python-context as the code you're running.
# MAGIC   - Don't put bespoke code in a root folder (e.g., not the src folder)
# MAGIC   - Don't use the ANS "special repository" pattern (/Workspace/Repos/notebooks/databricks/*) as this is a totally different source of code than the asset bundle, and it relies on someone remembering to hand-click "pull" in the "special repository" to keep it up to date.

# COMMAND ----------

import mdp_databricks_common.bronze_data_load_functions as dlf

# The loopback module is a demo module with a hello_world() trivial function. Not to be used in production code.
import mdp_databricks_common.loopback as lb
import dlt

@dlt.table
def hello_world_dlt():
    # The next two lines just list the contents of one of the modules - not needed to run anything, but useful for finding out which functions are available.
    module_contents = dir(dlf)
    print(module_contents)

    # This line calls a function from the imported loopback module (but you won't see this output anywhere in DLT)
    lb.hello_world()
    lib_message = lb.hello_world_string() 

    # The next line illustrates how to pull in the injected illustration par:    
    injected_par = spark.conf.get("illustration_injected_par")

    # Then construct a message to prove this works in the output...
    output_message = f"lib message: {lib_message}; injected_par: {injected_par}"

    # Put the string proving the mdp_databricks_common library works in the output table...
    return spark.createDataFrame([(output_message,)], ["message"])

