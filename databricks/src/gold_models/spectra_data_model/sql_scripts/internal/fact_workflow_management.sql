WITH CTE_BKWorkflowWorkAccount AS (
  SELECT
    de.DATA_ENTITY_ID AS BK_WORKFLOW_ACCOUNT,
    de.WORK_ITEM_ID
  FROM {env_var}_catalog.silver_int.spectra_work_items swi
  LEFT JOIN {env_var}_catalog.silver_int.spectra_work_item_data_entities de
  ON swi.WORK_ITEM_ID = de.WORK_ITEM_ID
  WHERE de.DATA_ELEMENT_ID = 1197
  AND de.ROW_END_DATETIME IS NULL
)
,
CTE_BKWorkflowWorkCustomer AS (
  SELECT
    de.DATA_ENTITY_ID AS BK_WORKFLOW_ACCOUNT,
    de.WORK_ITEM_ID
  FROM {env_var}_catalog.silver_int.spectra_work_items swi
  LEFT JOIN {env_var}_catalog.silver_int.spectra_work_item_data_entities de
  ON swi.WORK_ITEM_ID = de.WORK_ITEM_ID
  WHERE de.DATA_ELEMENT_ID = 1209
  AND de.ROW_END_DATETIME IS NULL
)
 
SELECT
fwi.WORK_ITEMS_ID AS BK_WORKFLOW_WORK_ITEM,
CASE
    WHEN cha.PK_WORKFLOW_CHANNEL IS NOT NULL THEN cha.PK_WORKFLOW_CHANNEL
    WHEN cha.PK_WORKFLOW_CHANNEL IS NULL AND swi.CHANNELS_ID IS NOT NULL THEN -1
    ELSE -2
  END AS FK_WORKFLOW_CHANNEL,
CAST(
    CASE
      WHEN dccr.PK_DATE IS NOT NULL THEN dccr.PK_DATE
      WHEN dccr.PK_DATE IS NULL AND CAST(swi.CREATION_TIME AS DATE) <> CAST('1900-01-01' AS DATE) THEN '1900-01-01'
      ELSE '1899-12-31'
    END
    AS DATE
  ) AS FK_WORKFLOW_CREATED_DATE,
  CAST(
    CASE
      WHEN dcr.PK_DATE IS NOT NULL THEN dcr.PK_DATE
      WHEN dcr.PK_DATE IS NULL AND CAST(swi.RECEIVED_TIME AS DATE) <> CAST('1900-01-01' AS DATE) THEN '1900-01-01'
      ELSE '1899-12-31'
    END
    AS DATE
  ) AS FK_WORKFLOW_RECEIVED_DATE,
  CAST(
    CASE
      WHEN dccl.PK_DATE IS NOT NULL THEN dccl.PK_DATE
      WHEN dccl.PK_DATE IS NULL AND CAST(swi.CLOSED_TIME AS DATE) <> CAST('1900-01-01' AS DATE) THEN '1900-01-01'
      ELSE '1899-12-31'
    END
    AS DATE
  ) AS FK_WORKFLOW_CLOSED_DATE,
  coalesce(DATE_FORMAT(swi.CREATION_TIME, 'HH:mm:ss'), 'Unknown') AS WORKFLOW_CREATED_TIME,
  coalesce(DATE_FORMAT(swi.RECEIVED_TIME, 'HH:mm:ss'), 'Unknown') AS WORKFLOW_RECEIVED_TIME,
  coalesce(DATE_FORMAT(swi.CLOSED_TIME, 'HH:mm:ss'), 'Unknown') AS WORKFLOW_CLOSED_TIME
 
--  swi.WORK_ITEM_ID AS FK_WORKFLOW_WORK_ITEM,
 -- ibs.COMPANIES_ID AS FK_WORKFLOW_INITIAL_BUSINESS_STREAM,
 -- ic.CATEGORIES_ID AS FK_WORKFLOW_INITIAL_CATEGORY,
 -- lbs.COMPANIES_ID AS FK_WORKFLOW_LATEST_BUSINESS_STREAM,
  --lc.CATEGORIES_ID AS FK_WORKFLOW_LATEST_CATEGORY,
  --cha.BK_WORKFLOW_CHANNEL AS FK_WORKFLOW_CHANNEL
 -- so.OUTCOME_ID AS FK_WORKFLOW_OUTCOME,
 -- COALESCE(acc.BK_WORKFLOW_ACCOUNT, -1) AS FK_WORKFLOW_ACCOUNT,
 -- COALESCE(cus.BK_WORKFLOW_ACCOUNT, -1) AS FK_WORKFLOW_CUSTOMER,
 
--  cru.USERS_ID AS FK_WORKFLOW_CREATED_BY,
--  clu.USERS_ID AS FK_WORKFLOW_CLOSED_BY,
 
--   COALESCE(dct.ID, -1) AS FK_WORKFLOW_CATEGORY_TIMING,
--   swi.WORK_ITEM_ID,
--   swi.HELD_UNTIL AS WORK_HELD_UNTIL,
--   fwi.OPEN_DURATION_ELAPSED_TIME,
--   fo.HANDLE_DURATION_ET,
--   dct.TIMING
 
 
FROM {env_var}_catalog.silver_int.spectra_fact_work_items fwi
LEFT JOIN {env_var}_catalog.silver_int.spectra_work_items swi
ON fwi.WORK_ITEMS_ID = swi.WORK_ITEM_ID
-- LEFT JOIN {env_var}_catalog.silver_int.spectra_fact_offers fo
-- ON fwi.WORK_ITEMS_ID = fo.WORK_ITEM_ID
-- LEFT JOIN {env_var}_catalog.silver_int.spectra_companies ibs
-- ON fwi.INITIAL_COMPANY_TID = ibs.COMPANIES_TID
-- LEFT JOIN {env_var}_catalog.silver_int.spectra_companies lbs
-- ON fwi.LAST_COMPANY_TID = lbs.COMPANIES_TID
-- LEFT JOIN {env_var}_catalog.silver_int.spectra_categories ic
-- ON fwi.INITIAL_CATEGORY_TID = ic.CATEGORIES_TID
-- LEFT JOIN {env_var}_catalog.silver_int.spectra_categories lc
-- ON fwi.LAST_CATEGORY_TID = lc.CATEGORIES_TID
LEFT JOIN {env_var}_catalog.gold_int.dim_workflow_channel cha
ON swi.CHANNELS_ID = cha.BK_WORKFLOW_CHANNEL
-- LEFT JOIN {env_var}_catalog.silver_int.spectra_outcomes so
-- ON swi.OUTCOMES_TID = so.OUTCOME_TID
-- LEFT JOIN CTE_BKWorkflowWorkAccount acc
-- ON swi.WORK_ITEM_ID = acc.WORK_ITEM_ID
-- LEFT JOIN CTE_BKWorkflowWorkCustomer cus
-- ON swi.WORK_ITEM_ID = cus.WORK_ITEM_ID
-- LEFT JOIN {env_var}_catalog.silver_con.spectra_users cru
-- ON swi.CREATED_BY_TID = cru.USERS_TID
-- LEFT JOIN {env_var}_catalog.silver_con.spectra_users clu
-- ON fwi.CLOSED_BY_TID = clu.USERS_TID
LEFT JOIN {env_var}_catalog.gold_int.dim_calendar dccr
ON CAST(swi.CREATION_TIME AS DATE) = dccr.PK_DATE
LEFT JOIN {env_var}_catalog.gold_int.dim_calendar dcr
ON CAST(swi.RECEIVED_TIME AS DATE) = dcr.PK_DATE
LEFT JOIN {env_var}_catalog.gold_int.dim_calendar dccl
ON CAST(swi.CLOSED_TIME AS DATE) = dccl.PK_DATE
-- LEFT JOIN {env_var}_catalog.bronze_sap_reference_data_int.spectra_category_timings  dct
-- ON swi.COMPANY_TID = dct.COMPANY_ID
-- and swi.CATEGORIES_TID = dct.CATEGORY_ID
-- and swi.CHANNELS_ID = dct.CHANNEL_ID
;