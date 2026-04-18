-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_customer_address", "column_name":"COUNTRY_CODE"}
MERGE INTO {catalog_name}.gold_con.dim_customer_address AS dca
USING (SELECT
da.ADDRESS_ID,
dc.LBS_COUNTRY_CODE
FROM {catalog_name}.silver_con.dataverse_address da
LEFT JOIN {catalog_name}.silver_con.dataverse_country dc
ON da.LBS_ADDRESS1_COUNTRY = dc.COUNTRY_ID
AND dc.ROW_IS_CURRENT = 1
WHERE da.ROW_IS_CURRENT = 1) da
ON da.ADDRESS_ID = dca.BK_CUSTOMER_ADDRESS
AND dca.ROW_IS_CURRENT = 1
WHEN MATCHED THEN
  UPDATE SET dca.COUNTRY_CODE = CAST(COALESCE(da.LBS_COUNTRY_CODE,'Unknown') AS STRING)