-- {"check_type":"column_not_present", "table_schema":"gold_con", "table_name":"dim_mortgage_part", "column_name":"REPAYMENT_TYPE"}
ALTER TABLE {catalog_name}.gold_con.dim_mortgage_part
ADD COLUMN REPAYMENT_TYPE STRING AFTER STATUS;