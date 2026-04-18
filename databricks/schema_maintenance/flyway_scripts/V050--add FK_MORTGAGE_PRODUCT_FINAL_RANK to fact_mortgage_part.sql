-- {"check_type":"column_not_present", "table_schema":"gold_con", "table_name":"fact_mortgage_part", "column_name":"FK_MORTGAGE_PRODUCT_FINAL_RANK"}
ALTER TABLE {catalog_name}.gold_con.fact_mortgage_part
ADD COLUMN FK_MORTGAGE_PRODUCT_FINAL_RANK BIGINT AFTER FK_MORTGAGE_PRODUCT_CURRENT_RANK;
