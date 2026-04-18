-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_customer_address", "column_name":"FLAT"}
MERGE INTO {catalog_name}.gold_con.dim_customer_address AS dca
USING {catalog_name}.silver_con.dataverse_address AS da  
ON da.ADDRESS_ID = dca.BK_CUSTOMER_ADDRESS
AND da.ROW_IS_CURRENT = 1
AND dca.ROW_IS_CURRENT = 1
WHEN MATCHED THEN
  UPDATE SET dca.FLAT = CAST(COALESCE(da.LBS_ADDRESS1_FLAT,'Unknown') AS STRING)