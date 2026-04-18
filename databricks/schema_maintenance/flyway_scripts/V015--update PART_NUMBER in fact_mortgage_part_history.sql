-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"fact_mortgage_part_history", "column_name":"PART_NUMBER"}
MERGE INTO {catalog_name}.gold_con.fact_mortgage_part_history AS a
USING {catalog_name}.gold_con.dim_mortgage_part AS b
ON a.FK_MORTGAGE_PART = b.PK_MORTGAGE_PART
AND a.PART_NUMBER IS NULL
WHEN MATCHED THEN
  UPDATE SET a.PART_NUMBER = b.PART_NUMBER