#Return the rules matching the tag as a format ready for SDP annotation.
from pyspark.sql.functions import expr, col

def get_rules(tag):
  from pyspark.sql import SparkSession
  spark = SparkSession.getActiveSession()
  catalog = "main"
  schema = "sdp_ut_dev"
  
  """
    loads data quality rules from csv file
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.table(f"{catalog}.{schema}.expectations").where(f"tag = '{tag}'")
  for row in df.collect():
    rules[row['name']] = row['constraint']
  return rules
