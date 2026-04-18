-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_customer", "column_name":"CUSTOMER_NUMBER"}
ALTER TABLE {catalog_name}.gold_con.dim_customer
ALTER COLUMN CUSTOMER_NUMBER
COMMENT 'Unique number up to 10 digits long allocated to an individual customer. Each customer should have only one Customer Number.'