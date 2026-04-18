-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"fact_mortgage_part_history", "column_name":"ACCOUNT_NUMBER"}
MERGE INTO {catalog_name}.gold_con.fact_mortgage_part_history AS a
USING {catalog_name}.gold_con.dim_mortgage_part AS b
ON a.FK_MORTGAGE_PART = b.PK_MORTGAGE_PART
AND a.ACCOUNT_NUMBER IS NULL
WHEN MATCHED THEN
  UPDATE SET a.ACCOUNT_NUMBER = b.ACCOUNT_NUMBER