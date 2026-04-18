-- {"check_type":"column_not_present", "table_schema":"gold_con", "table_name":"fact_mortgage_account", "column_name":"PROPERTY_PURCHASE_PRICE"}
ALTER TABLE {catalog_name}.gold_con.fact_mortgage_account
ADD COLUMN PROPERTY_PURCHASE_PRICE DECIMAL(38,2) AFTER WEIGHTED_RATE;