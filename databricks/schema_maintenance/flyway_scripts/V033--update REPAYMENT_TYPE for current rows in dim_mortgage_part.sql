-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"dim_mortgage_part", "column_name":"REPAYMENT_TYPE"}
MERGE INTO {catalog_name}.gold_con.dim_mortgage_part AS dmp
USING (SELECT CAST(a.ID AS STRING) AS BK_MORTGAGE_PART,
              CAST(COALESCE(a.LOAN_NAME, "Unknown") AS STRING) AS REPAYMENT_TYPE
      FROM {catalog_name}.silver_con.mambu_loan_account a
      WHERE a.ROW_IS_CURRENT = 1) x
ON dmp.BK_MORTGAGE_PART = x.BK_MORTGAGE_PART
AND dmp.ROW_IS_CURRENT = 1
WHEN MATCHED THEN
  UPDATE SET dmp.REPAYMENT_TYPE = x.REPAYMENT_TYPE
