-- {"check_type":"column_not_present", "table_schema":"gold_con", "table_name":"dim_mortgage_transaction", "column_name":"IS_FINANCIAL_TRANSACTION"}
ALTER TABLE {catalog_name}.gold_con.dim_mortgage_transaction
ADD COLUMN IS_FINANCIAL_TRANSACTION STRING AFTER TRANSACTION_TYPE;