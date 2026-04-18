WITH CTE_BKDimMortgageProduct AS (
  SELECT
    c.CASE_ID AS BK_MORTGAGE_CASE,
    sp.PRODUCT_CODE AS BK_MORTGAGE_PRODUCT
  FROM {env_var}_catalog.silver_con.mso_case c
  LEFT JOIN {env_var}_catalog.silver_int.mso_selected_product sp
  ON sp.CASE_ID = c.CASE_ID
  AND sp.ROW_END_DATETIME IS NULL -- Current rows only
  WHERE c.ROW_END_DATETIME IS NULL -- Current rows only
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.CASE_ID
    ORDER BY 
      (COALESCE(sp.CAPITAL_AMOUNT, 0) + COALESCE(sp.INTEREST_ONLY_AMOUNT, 0)) DESC,
      sp.TERM_IN_MONTHS DESC,
      sp.PRODUCT_ID DESC
  ) = 1
)

,CTE_BKEarliestCompletionDate AS (
  SELECT
    c.CASE_ID AS BK_MORTGAGE_CASE,
    r.COMPLETION_DATETIME
  FROM {env_var}_catalog.silver_con.mso_case c
  LEFT JOIN {env_var}_catalog.silver_int.mso_remittance r
  ON r.CASE_ID = c.CASE_ID
  AND r.ROW_END_DATETIME IS NULL
  WHERE c.ROW_END_DATETIME IS NULL -- Current rows only
  AND r.REMITTANCE_STATUS IN ('Released', 'Returned')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.CASE_ID
    ORDER BY r.COMPLETION_DATETIME ASC,
    r.REMITTANCE_ID ASC
  ) = 1
)

,CTE_BKLatestCompletionDate AS (
  SELECT
    c.CASE_ID AS BK_MORTGAGE_CASE,
    r.COMPLETION_DATETIME
  FROM {env_var}_catalog.silver_con.mso_case c
  LEFT JOIN {env_var}_catalog.silver_int.mso_remittance r
  ON r.CASE_ID = c.CASE_ID
  AND r.ROW_END_DATETIME IS NULL
  WHERE c.ROW_END_DATETIME IS NULL -- Current rows only
  AND r.REMITTANCE_STATUS IN ('Released', 'Returned')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.CASE_ID
    ORDER BY r.COMPLETION_DATETIME DESC,
    r.REMITTANCE_ID DESC
  ) = 1
)

,CTE_LatestValuation AS (
  SELECT 
    BK_MORTGAGE_CASE,
    VALUATION_AMOUNT,
    BK_LATEST_VALUATION_DATE
  FROM (
    SELECT
      avm.CASE_ID AS BK_MORTGAGE_CASE,
      avm.VALUATION_AMOUNT,
      '1' AS IS_APPROVED,
      CAST(COALESCE(avm.REPORT_DATETIME, '1900-01-01') AS DATE) AS BK_LATEST_VALUATION_DATE
    FROM {env_var}_catalog.silver_int.mso_automated_valuation_model avm
    WHERE avm.ROW_END_DATETIME IS NULL -- Current rows only
    AND avm.STATUS = 'Acceptable'
    AND avm.VALUATION_AMOUNT >1
    UNION ALL
    SELECT 
      BK_MORTGAGE_CASE,
      VALUATION_AMOUNT,
      IS_APPROVED,
      BK_LATEST_VALUATION_DATE
    FROM (
      SELECT
        vr.CASE_ID AS BK_MORTGAGE_CASE,
        CASE WHEN COALESCE(vr.VALUATION_AMOUNT, 0)=0 THEN GREATEST(vrd.VALUATION_IN_PRESENT_CONDITION, vrd.RETENTION_VALUATION_POST_WORK) 
        ELSE vr.VALUATION_AMOUNT END AS VALUATION_AMOUNT,
        CASE WHEN vr.VALUATION_STATUS= 'Approved' THEN '1' ELSE '0' END AS IS_APPROVED,
        CAST(COALESCE(vr.REPORT_DATETIME, '1900-01-01') AS DATE) AS BK_LATEST_VALUATION_DATE,
        CAST(COALESCE(vrsh.CREATED_DATETIME, '1900-01-01') AS DATE) AS CREATED_DATE,
        rdmvso.RANK as STATUS_ORDER_RANK,
        vr.VALUATION_REPORT_ID
      FROM {env_var}_catalog.silver_int.mso_valuation_report vr
      LEFT JOIN {env_var}_catalog.silver_con.mso_valuation_report_status_history vrsh
      ON vrsh.VALUATION_REPORT_ID = vr.VALUATION_REPORT_ID
      LEFT JOIN {env_var}_catalog.silver_int.mso_valuation_report_detail vrd 
      on vrd.VALUATION_REPORT_ID = vr.VALUATION_REPORT_ID
      LEFT JOIN {env_var}_catalog.silver_int.reference_data_mso_valuation_status_order rdmvso
      on REPLACE(rdmvso.VALUATION_STATUS, ' ' , '') = REPLACE(vr.VALUATION_STATUS, ' ' , '')
      WHERE vr.REPORT_DATETIME IS NOT NULL
      AND vr.VALUATION_AMOUNT <>1
    )
      QUALIFY ROW_NUMBER() OVER (PARTITION BY BK_MORTGAGE_CASE ORDER BY IS_APPROVED DESC, CREATED_DATE DESC, BK_LATEST_VALUATION_DATE DESC, STATUS_ORDER_RANK ASC, VALUATION_REPORT_ID ASC) = 1
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY BK_MORTGAGE_CASE ORDER BY IS_APPROVED DESC, BK_LATEST_VALUATION_DATE DESC) = 1
)

,CTE_FactMortgageCase AS (
  SELECT
    c.CASE_ID AS BK_MORTGAGE_CASE,
    COALESCE(cbdmp.BK_MORTGAGE_PRODUCT, -1) AS BK_MAIN_MORTGAGE_PRODUCT,
    /* 
      CAST to a date to get the smart key, 
      not checking for invalid dates, 
      because they should have already bombed out at silver
    */
    CAST(
      COALESCE(
        cbecd.COMPLETION_DATETIME,
        '1900-01-01'
      )
      AS DATE
    ) AS BK_EARLIEST_COMPLETION_DATE,
    CAST(
      COALESCE(
        cblcd.COMPLETION_DATETIME,
        '1900-01-01'
      )
      AS DATE
    ) AS BK_LATEST_COMPLETION_DATE,
    CAST(
      COALESCE(
        c.CREATED_DATETIME,
        '1900-01-01'
      )
      AS DATE
    ) AS BK_CREATED_DATE,
    CAST(
      COALESCE(
        clv.BK_LATEST_VALUATION_DATE,
        '1900-01-01'
      )
      AS DATE
    ) AS BK_LATEST_VALUATION_DATE,
    lr.TOTAL_LOAN_AMOUNT,
    clv.VALUATION_AMOUNT AS LATEST_VALUATION_AMOUNT
  FROM {env_var}_catalog.silver_con.mso_case c
  LEFT JOIN {env_var}_catalog.silver_int.mso_loan_requirement lr
  ON lr.CASE_ID = c.CASE_ID
  AND lr.ROW_END_DATETIME IS NULL
  LEFT JOIN CTE_BKDimMortgageProduct cbdmp
  ON cbdmp.BK_MORTGAGE_CASE = c.CASE_ID
  LEFT JOIN CTE_BKEarliestCompletionDate cbecd
  ON cbecd.BK_MORTGAGE_CASE = c.CASE_ID
  LEFT JOIN CTE_BKLatestCompletionDate cblcd
  ON cblcd.BK_MORTGAGE_CASE = c.CASE_ID
  LEFT JOIN CTE_LatestValuation clv
  ON clv.BK_MORTGAGE_CASE = c.CASE_ID
  WHERE c.ROW_END_DATETIME IS NULL -- Current rows only
)

SELECT
  cfmc.BK_MORTGAGE_CASE AS BK_MORTGAGE_CASE,
  CASE
    WHEN dmc.PK_MORTGAGE_CASE IS NOT NULL THEN dmc.PK_MORTGAGE_CASE 
    WHEN dmc.PK_MORTGAGE_CASE IS NULL AND cfmc.BK_MORTGAGE_CASE IS NOT NULL THEN -1 
    ELSE -2 
  END AS FK_MORTGAGE_CASE,
  CASE
    WHEN dmp.PK_MORTGAGE_PRODUCT IS NOT NULL THEN dmp.PK_MORTGAGE_PRODUCT
    WHEN dmp.PK_MORTGAGE_PRODUCT IS NULL AND cfmc.BK_MAIN_MORTGAGE_PRODUCT IS NULL THEN -1
    ELSE -2
  END AS FK_MAIN_MORTGAGE_PRODUCT,
  CAST(
    CASE
      WHEN dce.PK_DATE IS NOT NULL THEN dce.PK_DATE
      WHEN dce.PK_DATE IS NULL AND cfmc.BK_EARLIEST_COMPLETION_DATE <> CAST('1900-01-01' AS DATE) THEN '1900-01-01'
      ELSE '1899-12-31'
    END 
    AS DATE
  ) AS FK_EARLIEST_COMPLETION_DATE,
  CAST(
    CASE
      WHEN dcl.PK_DATE IS NOT NULL THEN dcl.PK_DATE
      WHEN dcl.PK_DATE IS NULL AND cfmc.BK_EARLIEST_COMPLETION_DATE <> CAST('1900-01-01' AS DATE) THEN '1900-01-01'
      ELSE '1899-12-31'
    END 
    AS DATE
  ) AS FK_LATEST_COMPLETION_DATE,
  CAST(
    CASE
      WHEN dccd.PK_DATE IS NOT NULL THEN dccd.PK_DATE
      WHEN dccd.PK_DATE IS NULL AND cfmc.BK_CREATED_DATE <> CAST('1900-01-01' AS DATE) THEN '1900-01-01'
      ELSE '1899-12-31'
    END 
    AS DATE
  ) AS FK_CREATED_DATE,
  CAST(
    CASE
      WHEN dclvd.PK_DATE IS NOT NULL THEN dclvd.PK_DATE
      WHEN dclvd.PK_DATE IS NULL AND cfmc.BK_LATEST_VALUATION_DATE <> CAST('1900-01-01' AS DATE) THEN '1900-01-01'
      ELSE '1899-12-31'
    END 
    AS DATE
  ) AS FK_LATEST_VALUATION_DATE,
  cfmc.TOTAL_LOAN_AMOUNT,
  cfmc.LATEST_VALUATION_AMOUNT
FROM CTE_FactMortgageCase cfmc
LEFT JOIN {env_var}_catalog.gold_con.dim_mortgage_case dmc
ON dmc.BK_MORTGAGE_CASE = cfmc.BK_MORTGAGE_CASE
AND dmc.ROW_IS_CURRENT = 1
LEFT JOIN {env_var}_catalog.gold_int.dim_mortgage_product dmp
ON dmp.BK_MORTGAGE_PRODUCT = CAST(cfmc.BK_MAIN_MORTGAGE_PRODUCT AS STRING)
AND dmp.ROW_IS_CURRENT = 1
LEFT JOIN {env_var}_catalog.gold_int.dim_calendar dce
ON dce.PK_DATE = cfmc.BK_EARLIEST_COMPLETION_DATE
LEFT JOIN {env_var}_catalog.gold_int.dim_calendar dcl
ON dcl.PK_DATE = cfmc.BK_LATEST_COMPLETION_DATE
LEFT JOIN {env_var}_catalog.gold_int.dim_calendar dccd
ON dccd.PK_DATE = cfmc.BK_CREATED_DATE
LEFT JOIN {env_var}_catalog.gold_int.dim_calendar dclvd
ON dclvd.PK_DATE = cfmc.BK_LATEST_VALUATION_DATE