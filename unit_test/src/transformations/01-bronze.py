from pyspark import pipelines as dp
from config import get_rules


spark.conf.set("pipelines.incompatibleViewCheck.enabled", "false")

@dp.table(comment="Raw user data")
@dp.expect_all_or_drop(get_rules('user_bronze_sdp')) #get the rules from our centralized table.
def user_bronze_sdp():
  return spark.read.table("raw_user_data")