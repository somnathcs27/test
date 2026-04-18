# Databricks notebook source
# MAGIC %md
# MAGIC # Population of Workflow Control Metadata
# MAGIC Populates all meta data for the workflow control framework

# COMMAND ----------

dbutils.widgets.text(name='catalog', defaultValue='dev_catalog')
catalog_name = dbutils.widgets.get("catalog")

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

# Merge data into MDP data category table
merge_query = f"""
    WITH cte_mdp_data_category ( category_name) AS 
    (
    SELECT x.job_name
    FROM (VALUES('Customer'),
                ('Mortgage'),
                ('Saving')) AS x(job_name)
    )
    MERGE INTO {catalog_name}.workflow_control.mdp_data_category AS t
    USING cte_mdp_data_category AS s
    ON t.category_name = s.category_name
    WHEN NOT MATCHED THEN
    INSERT (category_name)
    VALUES (s.category_name)
    WHEN NOT MATCHED BY SOURCE THEN DELETE"""

spark.sql(merge_query)

# COMMAND ----------

# Merge data into the job detail table
merge_query = f"""
    WITH cte_job_detail ( fk_mdp_data_category, job_name ) AS 
    (
    SELECT mdp.pk_mdp_data_category AS fk_mdp_data_category,
            y.job_name
    FROM (
    SELECT x.mdp_data_category, x.job_name
    FROM (VALUES('Customer', '{env_var}_load_gold_mdp_dim_customer' ),
                ('Customer', '{env_var}_load_gold_mdp_fact_customer' ),
                ('Mortgage', '{env_var}_load_gold_mdp_mortgage' ),
                ('Mortgage', '{env_var}_load_gold_mdp_bridge_party_account' ),
                ('Mortgage', '{env_var}_load_gold_mdp_bridge_mortgage_case_mortgage_account' ),
                ('Saving', '{env_var}_load_gold_mdp_savings' )
          ) AS x (mdp_data_category, job_name )

    ) y
    INNER JOIN {catalog_name}.workflow_control.mdp_data_category mdp
      ON y.mdp_data_category = mdp.category_name
    )
    MERGE INTO {catalog_name}.workflow_control.job_detail AS t
    USING cte_job_detail AS s
    ON t.job_name = s.job_name
    WHEN MATCHED THEN
    UPDATE SET t.fk_mdp_data_category = s.fk_mdp_data_category
    WHEN NOT MATCHED THEN 
    INSERT ( fk_mdp_data_category, job_name )
    VALUES ( s.fk_mdp_data_category, s.job_name )
    WHEN NOT MATCHED BY SOURCE THEN DELETE"""

spark.sql(merge_query)

# COMMAND ----------

# Merge data into the job dependency table
merge_query = f"""
    WITH cte_job_dependency ( fk_job_detail, fk_job_detail_dependency ) AS 
    (
    SELECT jd.pk_job_detail AS fk_job_detail,
            jdd.pk_job_detail AS fk_job_detail_dependency
    FROM ( SELECT x.job_name, x.dependent_job_name
            FROM (VALUES ('{env_var}_load_gold_mdp_bridge_party_account', '{env_var}_load_gold_mdp_dim_customer' ),
                        ('{env_var}_load_gold_mdp_bridge_party_account', '{env_var}_load_gold_mdp_mortgage' ),
                        ('{env_var}_load_gold_mdp_bridge_party_account', '{env_var}_load_gold_mdp_savings' ),
                        ('{env_var}_load_gold_mdp_fact_customer', '{env_var}_load_gold_mdp_bridge_party_account' ),
                        ('{env_var}_load_gold_mdp_bridge_mortgage_case_mortgage_account', '{env_var}_load_gold_mdp_mortgage' )
                  ) AS x (job_name, dependent_job_name )
    ) y
    INNER JOIN {catalog_name}.workflow_control.job_detail jd
      ON y.job_name = jd.job_name
    INNER JOIN {catalog_name}.workflow_control.job_detail jdd
      ON y.dependent_job_name = jdd.job_name
    )
    MERGE INTO {catalog_name}.workflow_control.job_dependency AS t
    USING cte_job_dependency AS s
    ON t.fk_job_detail = s.fk_job_detail
    AND t.fk_job_detail_dependency = s.fk_job_detail_dependency
    WHEN NOT MATCHED THEN 
    INSERT ( fk_job_detail, fk_job_detail_dependency )
        VALUES ( s.fk_job_detail, s.fk_job_detail_dependency )
    WHEN NOT MATCHED BY SOURCE THEN DELETE"""

spark.sql(merge_query)
