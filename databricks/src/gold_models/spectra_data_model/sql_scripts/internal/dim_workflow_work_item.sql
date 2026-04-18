SELECT
    swi.WORK_ITEM_ID AS BK_WORKFLOW_WORK_ITEM,
    swi.WORK_ITEM_TYPE_ID,
    swit.WORK_ITEM_NAME AS WORK_ITEM_TYPE_NAME,
    swi.WORK_ITEM_STATUS_ID,
    CASE 
        WHEN swi.SLA = 1 THEN 'Yes'
        WHEN swi.SLA = 0 THEN 'No'
        WHEN swi.SLA IS NULL THEN 'Unknown'
    END AS IS_CLOSED_WITHIN_SLA,
    'SPECTRA' AS SOURCE_SYSTEM,
    getdate() AS ROW_START_DATETIME,
    CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
    1 AS ROW_IS_CURRENT
FROM {env_var}_catalog.silver_int.spectra_work_items swi
LEFT JOIN {env_var}_catalog.silver_int.spectra_work_item_types swit
    ON swi.WORK_ITEM_TYPE_ID = swit.WORK_ITEM_TYPE_ID
;