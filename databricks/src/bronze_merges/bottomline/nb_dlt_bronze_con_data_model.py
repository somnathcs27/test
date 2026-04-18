# Databricks notebook source
import mdp_databricks_common.bronze_data_load_functions as dlf

env_var = spark.conf.get("environment_name")
datalake_name = spark.conf.get("datalake_name_con")
file_system = 'raw-sensitive'
source_uc_schema = 'raw_bottomline_con'

dlf.execute_main_bronze_workflow(
   source_uc_schema = source_uc_schema, 
   file_system = file_system, 
   env_var = env_var, 
   datalake_name = datalake_name, 
   spark_config = spark.conf,
   dbutils = dbutils
)
