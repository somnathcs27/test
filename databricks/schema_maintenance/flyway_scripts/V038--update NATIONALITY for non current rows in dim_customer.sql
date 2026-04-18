-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_customer", "column_name":"NATIONALITY"}
UPDATE {catalog_name}.gold_con.dim_customer
SET NATIONALITY = 'Unknown'
WHERE NATIONALITY IS NULL