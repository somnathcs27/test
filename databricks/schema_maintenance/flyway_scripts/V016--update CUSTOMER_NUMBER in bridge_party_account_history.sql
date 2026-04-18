-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"bridge_party_account_history", "column_name":"CUSTOMER_NUMBER"}
MERGE INTO {catalog_name}.gold_con.bridge_party_account_history AS a
USING {catalog_name}.gold_con.dim_customer AS b
ON a.FK_CUSTOMER = b.PK_CUSTOMER
AND a.CUSTOMER_NUMBER IS NULL
WHEN MATCHED THEN
  UPDATE SET a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER