# Databricks notebook source
# COMMAND ----------

# DBTITLE 1,Define all our expectations as a metadata table
# In this example, we'll store our rules as a delta table for more flexibility & reusability. 
# While this isn't directly related to Unit test, it can also help for programatical analysis/reporting.
catalog = "main"
schema = dbName = db = "dbdemos_sdp_unit_test"

data = [
 # tag/table name      name              constraint
 ("user_bronze_sdp",  "correct_schema", "_rescued_data IS NULL"),
 ("user_silver_sdp",  "valid_id",       "id IS NOT NULL AND id > 0"),
 ("spend_silver_sdp", "valid_id",       "id IS NOT NULL AND id > 0"),
 ("user_gold_sdp",    "valid_age",      "age IS NOT NULL"),
 ("user_gold_sdp",    "valid_income",   "annual_income IS NOT NULL"),
 ("user_gold_sdp",    "valid_score",    "spending_core IS NOT NULL")
]
#Typically only run once, this doesn't have to be part of the SDP pipeline.
spark.createDataFrame(data=data, schema=["tag", "name", "constraint"]).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.expectations")

