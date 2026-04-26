from pyspark import pipelines as dp

@dp.materialized_view(
    name="gold_customer_summary"
)
def gold_customer_summary():
    return spark.sql("""
        SELECT country, COUNT(*) as total_customers
        FROM silver_customers
        GROUP BY country
    """)