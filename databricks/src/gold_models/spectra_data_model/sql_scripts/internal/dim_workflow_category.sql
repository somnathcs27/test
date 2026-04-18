WITH CTE_Category_Tiers AS (
  SELECT
    sct.CAT_ID,
    sct.CATEGORY,
    sct.TIER
  FROM {env_var}_catalog.bronze_sap_reference_data_int.spectra_category_tiers sct
  GROUP BY sct.CAT_ID, sct.CATEGORY, sct.TIER
)
 
SELECT
  c.CATEGORIES_ID AS BK_WORKFLOW_CATEGORY,
  c.CATEGORIES_NAME,
  ct.tier AS CATEGORY_TIER,
  'SPECTRA' AS SOURCE_SYSTEM,
  getdate () AS ROW_START_DATETIME,
  CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
  1 AS ROW_IS_CURRENT
FROM {env_var}_catalog.silver_int.spectra_categories c
LEFT JOIN CTE_Category_Tiers ct
ON c.categories_id = ct.CAT_ID
;