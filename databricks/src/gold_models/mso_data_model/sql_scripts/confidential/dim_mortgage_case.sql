SELECT
  c.CASE_ID AS BK_MORTGAGE_CASE,
  COALESCE(c.FRIENDLY_ID_NUMBER, -1) AS APPLICATION_NUMBER,
  COALESCE(lr.APPLICATION_TYPE, 'Unknown') AS APPLICATION_TYPE,
  COALESCE(c.FRIENDLY_ID, 'Unknown') AS FRIENDLY_ID,
  -- This regex adds spaces into a PascalCase string
  COALESCE(
    LTRIM(
      REGEXP_REPLACE(
        c.LATEST_CASE_STATUS,
        "([A-Z])",
        " $0"
      )
    ), 
    'Unknown'
  ) AS CASE_STATUS,
  -- This regex adds spaces into a PascalCase string
  COALESCE(
    LTRIM(
      REGEXP_REPLACE(
        c.CASE_STAGE,
        "([A-Z])",
        " $0"
      )
    ), 
    'Unknown'
  ) AS CASE_STAGE,
  COALESCE(c.CHANNEL, 'Unknown') AS CHANNEL,
  COALESCE(c.MORTGAGE_ACCOUNT_NUMBER, -1) AS MORTGAGE_ACCOUNT_NUMBER, 
  '{job_name}' AS SOURCE_SYSTEM,
  c.ROW_START_DATETIME AS ROW_START_DATETIME,
  c.ROW_END_DATETIME AS ROW_END_DATETIME,
  CASE
    WHEN c.ROW_END_DATETIME IS NULL THEN 1
    ELSE 0
  END AS ROW_IS_CURRENT
FROM {env_var}_catalog.silver_con.mso_case c
LEFT JOIN {env_var}_catalog.silver_int.mso_loan_requirement lr
ON lr.CASE_ID = c.CASE_ID
AND lr.ROW_END_DATETIME IS NULL -- Current rows only
WHERE c.ROW_END_DATETIME IS NULL -- Current rows only