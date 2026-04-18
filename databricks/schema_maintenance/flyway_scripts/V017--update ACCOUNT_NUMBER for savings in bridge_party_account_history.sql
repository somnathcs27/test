-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"bridge_party_account_history", "column_name":"ACCOUNT_NUMBER"}
MERGE INTO {catalog_name}.gold_con.bridge_party_account_history AS a
USING {catalog_name}.gold_con.dim_saving_account AS b
ON a.FK_ACCOUNT = b.PK_SAVING_ACCOUNT
AND a.ACCOUNT_TYPE = 'Saving'
AND a.ACCOUNT_NUMBER IS NULL
WHEN MATCHED THEN
  UPDATE SET a.ACCOUNT_NUMBER = b.SAVING_ACCOUNT_NUMBER  