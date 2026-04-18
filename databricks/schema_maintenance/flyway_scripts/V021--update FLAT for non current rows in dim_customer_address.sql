-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_customer_address", "column_name":"FLAT"}
UPDATE {catalog_name}.gold_con.dim_customer_address
SET FLAT = 'Unknown'
WHERE FLAT IS NULL