-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"fact_mortgage_part_history", "column_name":"ORIGINAL_ADVANCE"}
MERGE INTO {catalog_name}.gold_con.fact_mortgage_part_history AS a
USING (SELECT a.PK_MORTGAGE_PART, cfvp.ORIGINAL_ADVANCE_AMOUNT_LA AS ORIGINAL_ADVANCE
       FROM {catalog_name}.gold_con.dim_mortgage_part a
       INNER JOIN {catalog_name}.silver_con.mambu_loan_account b
         ON a.BK_MORTGAGE_PART = b.ID
       INNER JOIN {catalog_name}.silver_con.mambu_custom_field_value_pivot AS cfvp
         ON b.ENCODED_KEY = cfvp.PARENT_KEY
       WHERE cfvp.ROW_IS_CURRENT = 1
         AND b.ROW_IS_CURRENT = 1) x
ON a.FK_MORTGAGE_PART = x.PK_MORTGAGE_PART
AND a.ORIGINAL_ADVANCE IS NULL
WHEN MATCHED THEN
  UPDATE SET a.ORIGINAL_ADVANCE = x.ORIGINAL_ADVANCE