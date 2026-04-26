from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.materialized_view(
    name="silver_customers"
)
def silver_customers():
    return (
        spark.read.table("bronze_customers")
        .filter(col("customer_id").isNotNull())
    )