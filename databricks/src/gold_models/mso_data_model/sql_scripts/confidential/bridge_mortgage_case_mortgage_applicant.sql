WITH CTE_BKCase AS (
  SELECT
    dmc.BK_MORTGAGE_CASE
  FROM {env_var}_catalog.gold_con.dim_mortgage_case dmc
  WHERE dmc.ROW_IS_CURRENT = 1
)

,CTE_BKApplicant AS (
  SELECT
    dma.BK_MORTGAGE_APPLICANT
  FROM {env_var}_catalog.gold_con.dim_mortgage_applicant dma
  WHERE dma.ROW_IS_CURRENT = 1
)

,CTE_BridgeMortgageCaseMortgageApplicant AS (
  SELECT
    cbc.BK_MORTGAGE_CASE,
    cba.BK_MORTGAGE_APPLICANT,
    CASE
      WHEN a.APPLICANT_ID IS NULL THEN 'Null'
      WHEN a.APPLICANT_ID IS NOT NULL AND a.APPLICANT_INDEX IS NULL THEN 'Unknown'
      WHEN a.APPLICANT_INDEX = 1 THEN 'Yes'
      ELSE 'No'
    END AS IS_PRIMARY_APPLICANT
  FROM {env_var}_catalog.silver_con.mso_case c
  LEFT JOIN {env_var}_catalog.silver_con.mso_applicant a
  ON a.CASE_ID = c.CASE_ID
  LEFT JOIN CTE_BKCase cbc
  ON cbc.BK_MORTGAGE_CASE = c.CASE_ID
  LEFT JOIN CTE_BKApplicant cba
  ON cba.BK_MORTGAGE_APPLICANT = a.APPLICANT_ID
  AND a.ROW_END_DATETIME IS NULL -- Current rows only
  WHERE c.ROW_END_DATETIME IS NULL -- Current rows only
)

SELECT
  CASE
    WHEN dmc.PK_MORTGAGE_CASE IS NOT NULL THEN dmc.PK_MORTGAGE_CASE
    WHEN dmc.PK_MORTGAGE_CASE IS NULL AND cbmcma.BK_MORTGAGE_CASE IS NOT NULL THEN -1
    ELSE -2
  END AS FK_MORTGAGE_CASE,
  CASE
    WHEN dma.PK_MORTGAGE_APPLICANT IS NOT NULL THEN dma.PK_MORTGAGE_APPLICANT
    WHEN dma.PK_MORTGAGE_APPLICANT IS NULL AND cbmcma.BK_MORTGAGE_APPLICANT IS NOT NULL THEN -1
    ELSE -2
  END AS FK_MORTGAGE_APPLICANT,
  cbmcma.IS_PRIMARY_APPLICANT
FROM CTE_BridgeMortgageCaseMortgageApplicant cbmcma
LEFT JOIN {env_var}_catalog.gold_con.dim_mortgage_case dmc
ON dmc.BK_MORTGAGE_CASE = cbmcma.BK_MORTGAGE_CASE
AND dmc.ROW_IS_CURRENT = 1 -- Current rows only
LEFT JOIN {env_var}_catalog.gold_con.dim_mortgage_applicant dma
ON dma.BK_MORTGAGE_APPLICANT = cbmcma.BK_MORTGAGE_APPLICANT
AND dma.ROW_IS_CURRENT = 1 -- Current rows only