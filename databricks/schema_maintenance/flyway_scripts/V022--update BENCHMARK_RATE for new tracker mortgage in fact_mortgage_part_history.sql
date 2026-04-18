-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"fact_mortgage_part_history", "column_name":"BENCHMARK_RATE"}
UPDATE {catalog_name}.gold_con.fact_mortgage_part_history 
SET BENCHMARK_RATE = 0.037 
WHERE PK_MORTGAGE_PART = -4711277670904430354 AND FK_MONTH_END_DATE = DATE('2025-12-31')