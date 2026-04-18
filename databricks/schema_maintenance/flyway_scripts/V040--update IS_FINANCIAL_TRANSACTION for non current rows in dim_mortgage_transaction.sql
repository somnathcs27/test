-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_mortgage_transaction", "column_name":"IS_FINANCIAL_TRANSACTION"}
UPDATE {catalog_name}.gold_con.dim_mortgage_transaction
  SET IS_FINANCIAL_TRANSACTION = 'Unknown'
WHERE IS_FINANCIAL_TRANSACTION IS NULL