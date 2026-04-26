from pyspark import pipelines as dp

@dp.table(
    name="bronze_customers",
    comment="Raw customer bronze layer"
)
def bronze_customers():
    return spark.read.table("raw.customers")