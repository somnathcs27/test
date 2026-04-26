from pyspark import pipelines as dp
from pyspark.sql.functions import count


# # Fetch parameters from DLT pipeline
# env = spark.conf.get("pipeline.env", "dev")
# catalog = spark.conf.get("pipeline.catalog", "main")
# schema = spark.conf.get("pipeline.schema", f"spark_declarative_pipeline_{env}")

@dp.materialized_view(
    name="TEST_user_gold_sdp",
    comment="TEST: check that gold table only contains unique customer id",
    private=True
)
@dp.expect_or_fail("pk_must_be_unique", "duplicate = 1")
def TEST_user_gold_sdp():
    return (
        spark.read.table("user_gold_sdp")
        .groupBy("id")
        .agg(count("*").alias("duplicate"))
    )