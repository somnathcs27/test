SELECT 
	ccc.COMPANY_CATEGORY_CHANNEL_ID AS BK_WORKFLOW_TIMING,
	c.COMPANIES_NAME AS BUSINESS_STREAM_NAME,
	cat.CATEGORIES_NAME AS CATEGORY_NAME,
	ch.CHANNELS_NAME AS CHANNEL_NAME,
	CAST(sct.TIMING AS INT) AS TIMING,
	CAST(sct.TIMING_START_DATE AS DATE) AS TIMING_START_DATE,
	CAST(sct.TIMING_END_DATE AS DATE) AS TIMING_END_DATE,
	ccc.COMPLETION_SLA,
	ccc.INITIAL_OFFER_SLA,
	ccc.INITIAL_OFFER_PRIORITY_SLA,
	CASE WHEN ccc.IS_DELETED = 1 THEN 'Yes' WHEN ccc.IS_DELETED	 = '0' THEN 'No' ELSE 'Unknown' End AS IS_DELETED,
	'SPECTRA' AS SOURCE_SYSTEM,
	getdate() AS ROW_START_DATETIME,
	CAST(NULL AS TIMESTAMP) AS ROW_END_DATETIME,
	1 AS ROW_IS_CURRENT
FROM {env_var}_catalog.silver_int.spectra_company_category_channels ccc
LEFT JOIN {env_var}_catalog.silver_int.spectra_company_categories cc
ON ccc.COMPANY_CATEGORY_ID = cc.COMPANY_CATEGORIES_ID
LEFT JOIN {env_var}_catalog.silver_int.spectra_channels ch
ON ccc.CHANNEL_ID = ch.CHANNELS_ID
LEFT JOIN {env_var}_catalog.silver_int.spectra_companies c
ON cc.COMPANY_ID = c.COMPANIES_ID
LEFT JOIN {env_var}_catalog.silver_int.spectra_categories cat
ON cc.CATEGORY_ID = cat.CATEGORIES_ID
LEFT JOIN {env_var}_catalog.bronze_sap_reference_data_int.spectra_category_timings sct
ON sct.CATEGORY_ID = cat.CATEGORIES_TID
AND sct.COMPANY_ID = c.COMPANIES_TID
AND sct.CHANNEL_ID = ch.CHANNELS_ID