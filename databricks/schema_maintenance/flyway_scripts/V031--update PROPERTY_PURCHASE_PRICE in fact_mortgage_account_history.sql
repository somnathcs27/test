-- {"check_type":"column_present", "table_schema":"gold_con", "table_name":"fact_mortgage_account_history", "column_name":"PROPERTY_PURCHASE_PRICE"}
MERGE INTO {catalog_name}.gold_con.fact_mortgage_account_history AS a
USING (SELECT a.PK_MORTGAGE_PROPERTY, b.PROPERTY_PURCHASE_PRICE
      FROM {catalog_name}.gold_con.dim_mortgage_property a
      INNER JOIN {catalog_name}.silver_con.meds_valuation b
        ON a.BK_MORTGAGE_PROPERTY = b.MORTGAGE_ACCOUNT_ID
      WHERE b.ROW_IS_CURRENT = 1)  AS b
ON a.FK_MORTGAGE_PROPERTY = b.PK_MORTGAGE_PROPERTY
AND a.PROPERTY_PURCHASE_PRICE IS NULL
WHEN MATCHED THEN
  UPDATE SET a.PROPERTY_PURCHASE_PRICE = b.PROPERTY_PURCHASE_PRICE;