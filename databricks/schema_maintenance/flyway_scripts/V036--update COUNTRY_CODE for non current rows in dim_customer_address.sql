-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_customer_address", "column_name":"COUNTRY_CODE"}
UPDATE {catalog_name}.gold_con.dim_customer_address
SET COUNTRY_CODE = 'Unknown'
WHERE COUNTRY_CODE IS NULL