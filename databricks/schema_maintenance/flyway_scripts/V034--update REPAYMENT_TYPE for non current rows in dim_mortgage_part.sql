-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_mortgage_part", "column_name":"REPAYMENT_TYPE"}
UPDATE {catalog_name}.gold_con.dim_mortgage_part
  SET REPAYMENT_TYPE = 'Unknown'
WHERE REPAYMENT_TYPE IS NULL