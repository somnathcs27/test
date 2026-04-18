# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

env_var = dbutils.secrets.get(scope = 'akv_secret_scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "complaint_analysis_input",
    comment = "The complaints records that are needed to be input into the AI engine"
)
def bronze_complaint_analysis_input():

    df = spark.sql(f"""
SELECT  
`COMPLAINT_ID`,
`CREATED_DATE` AS `CREATED_DATETIMESTAMP`,
`LAST_CHANGED_DATE` AS `LAST_UPDATED_DATETIMESTAMP`,
`COMPLAINT_REASON`,
`INVESTIGATION_NOTES`,
`COMPLAINT_ACTION_TAKEN`
FROM {env_var}_catalog.bronze_enquire_analysis_con.complaint_update_table
""")

    return df
