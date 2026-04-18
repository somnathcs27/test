# Databricks notebook source
# MAGIC %md
# MAGIC # Summary
# MAGIC Refreshes Foreign Catalog schema.

# COMMAND ----------

dbutils.widgets.text(name='env_var', defaultValue = 'dev')
env_var = dbutils.widgets.get("env_var")

# COMMAND ----------

spark.sql(f"REFRESH FOREIGN SCHEMA {env_var}_mdp_workflow_control_catalog.control")