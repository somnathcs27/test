/*LF Note: Before you tell me this is a mess, 
Spark SQL doesn't have the concept of an UNPIVOT clause
without using Python. We've taken a UNION ALL approach here.

This query will take all 6 Key Value pairs out of the flat structure
and create one row per key value pair per deleted record.*/
WITH CTE_Unpivot AS (
  SELECT
    dr.TableName,
    dr.MDP_ROW_HASH,
    dr.KeyName1 AS KeyName,
    dr.KeyValue1 AS KeyValue,
    dr.LastUpdatedDate,
    dr.DateDeletedFromPublished,
    dr.MDP_LOAD_ID,
    dr.MDP_LOAD_DATETIME,
    dr.__START_AT,
    dr.__END_AT
  FROM {env_var}_catalog.bronze_mso_con.deleted_record dr

  UNION ALL

  SELECT
    dr.TableName,
    dr.MDP_ROW_HASH,
    dr.KeyName2 AS KeyName,
    dr.KeyValue2 AS KeyValue,
    dr.LastUpdatedDate,
    dr.DateDeletedFromPublished,
    dr.MDP_LOAD_ID,
    dr.MDP_LOAD_DATETIME,
    dr.__START_AT,
    dr.__END_AT
  FROM {env_var}_catalog.bronze_mso_con.deleted_record dr

  UNION ALL

  SELECT
    dr.TableName,
    dr.MDP_ROW_HASH,
    dr.KeyName3 AS KeyName,
    dr.KeyValue3 AS KeyValue,
    dr.LastUpdatedDate,
    dr.DateDeletedFromPublished,
    dr.MDP_LOAD_ID,
    dr.MDP_LOAD_DATETIME,
    dr.__START_AT,
    dr.__END_AT
  FROM {env_var}_catalog.bronze_mso_con.deleted_record dr

  UNION ALL

  SELECT
    dr.TableName,
    dr.MDP_ROW_HASH,
    dr.KeyName4 AS KeyName,
    dr.KeyValue4 AS KeyValue,
    dr.LastUpdatedDate,
    dr.DateDeletedFromPublished,
    dr.MDP_LOAD_ID,
    dr.MDP_LOAD_DATETIME,
    dr.__START_AT,
    dr.__END_AT
  FROM {env_var}_catalog.bronze_mso_con.deleted_record dr

  UNION ALL

  SELECT
    dr.TableName,
    dr.MDP_ROW_HASH,
    dr.KeyName5 AS KeyName,
    dr.KeyValue5 AS KeyValue,
    dr.LastUpdatedDate,
    dr.DateDeletedFromPublished,
    dr.MDP_LOAD_ID,
    dr.MDP_LOAD_DATETIME,
    dr.__START_AT,
    dr.__END_AT
  FROM {env_var}_catalog.bronze_mso_con.deleted_record dr

  UNION ALL

  SELECT
    dr.TableName,
    dr.MDP_ROW_HASH,
    dr.KeyName6 AS KeyName,
    dr.KeyValue6 AS KeyValue,
    dr.LastUpdatedDate,
    dr.DateDeletedFromPublished,
    dr.MDP_LOAD_ID,
    dr.MDP_LOAD_DATETIME,
    dr.__START_AT,
    dr.__END_AT
  FROM {env_var}_catalog.bronze_mso_con.deleted_record dr
)

SELECT
  CAST(up.MDP_ROW_HASH AS STRING) AS MDP_ROW_HASH,
  CAST(up.TableName AS STRING) AS TABLE_NAME,
  CAST(up.KeyName AS STRING) AS KEY_NAME,
  CAST(up.KeyValue AS STRING) AS KEY_VALUE,
  CAST(up.DateDeletedFromPublished AS TIMESTAMP) AS DATE_DELETED_FROM_PUBLISHED,
  CAST(up.LastUpdatedDate AS TIMESTAMP) AS LAST_UPDATED_DATETIME,
  CAST(up.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
  CAST(up.MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
  CAST(up.__START_AT AS TIMESTAMP) AS ROW_START_DATETIME,
  CAST(up.__END_AT AS TIMESTAMP) AS ROW_END_DATETIME
FROM CTE_Unpivot up
WHERE up.__END_AT IS NULL -- Equivalent of ROW_IS_CURRENT=1