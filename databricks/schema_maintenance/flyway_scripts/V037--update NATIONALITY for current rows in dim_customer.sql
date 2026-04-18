-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_customer", "column_name":"NATIONALITY"}
MERGE INTO {catalog_name}.gold_con.dim_customer AS dc
USING (SELECT
dvc.CUSTOMER_ID AS CUSTOMER_ID,
COALESCE(dogm_nationality.LOCALIZED_LABEL,'Unknown') AS NATIONALITY
FROM {catalog_name}.silver_con.dataverse_contact dvc
LEFT JOIN {catalog_name}.silver_int.dataverse_global_option_set_metadata dogm_nationality
ON dogm_nationality.ENTITY_NAME = 'contact'  
AND dogm_nationality.GLOBAL_OPTION_SET_NAME = 'lbs_nationality' 
AND dogm_nationality.OPTION = dvc.LBS_NATIONALITY
WHERE dvc.ROW_IS_CURRENT = 1) dvc
ON dvc.CUSTOMER_ID = dc.BK_CUSTOMER
AND dc.ROW_IS_CURRENT = 1
WHEN MATCHED THEN
  UPDATE SET dc.NATIONALITY = dvc.NATIONALITY