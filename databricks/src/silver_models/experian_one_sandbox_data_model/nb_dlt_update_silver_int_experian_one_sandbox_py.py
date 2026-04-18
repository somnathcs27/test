# Databricks notebook source
# DBTITLE 1,Import libraries
from pyspark import pipelines as dp
import dlt
from pyspark.sql.functions import row_number, col, expr, lead, to_timestamp, when, last, max
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf
from mdp_databricks_common.feature_toggle import feature_toggle as ft

# Import common ETL and metadata functions
import sys

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

# DBTITLE 1,Create supporting functions
def add_housekeeping_columns(df, partition_keys):

    # Ensure partition_keys is always a list
    if isinstance(partition_keys, str):
        partition_keys = [partition_keys]

    # Define the ascending window specification
    windowSpecAsc = Window.partitionBy(partition_keys).orderBy(
        col("ROW_START_DATETIME").asc(),
        col("MDP_LOAD_ID").asc()
    )

    # Define the descending window specification
    windowSpecDesc = Window.partitionBy(partition_keys).orderBy(
        col("ROW_START_DATETIME").desc(),
        col("MDP_LOAD_ID").desc()
    )

    # Add ROW_END_DATETIME and ROW_IS_CURRENT according to the descending windowSpec for ROW_IS_CURRENT and the ascending windowSpec for ROW_END_DATETIME
    df = (
        df.withColumn("ROW_END_DATETIME", lead("ROW_START_DATETIME").over(windowSpecAsc))
        .withColumn("row_num", row_number().over(windowSpecDesc))
        .withColumn("ROW_IS_CURRENT", expr("CASE WHEN row_num = 1 THEN 1 ELSE 0 END"))
        .drop("row_num")
    )

    return df   


def add_housekeeping_columns_filename_based(df, partition_keys, start_time_col="ROW_START_DATETIME", load_id_col="MDP_LOAD_ID"):

    # Ensure partition_keys is always a list
    if isinstance(partition_keys, str):
        partition_keys = [partition_keys]

    # Ascending window for ROW_END_DATETIME
    windowSpecAsc = (
        Window.partitionBy(*partition_keys)
              .orderBy(col(start_time_col).asc(), col(load_id_col).asc())
    )

    # Descending window for ROW_IS_CURRENT
    windowSpecDesc = (
        Window.partitionBy(*partition_keys)
              .orderBy(col(start_time_col).desc(), col(load_id_col).desc())
    )

    df = (
        df.withColumn("ROW_END_DATETIME", lead(start_time_col).over(windowSpecAsc))
          .withColumn("row_num", row_number().over(windowSpecDesc))
          .withColumn("ROW_IS_CURRENT", expr("CASE WHEN row_num = 1 THEN 1 ELSE 0 END"))
          .drop("row_num")
    )

    return df

# COMMAND ----------

# DBTITLE 1,template
# MAGIC %skip
# MAGIC @dp.table(
# MAGIC     name="experian_sandbox_variations",
# MAGIC     comment=""
# MAGIC )
# MAGIC @dlt.expect_all_or_drop({
# MAGIC     "valid_customer_id": "CUSTOMER_ID IS NOT NULL"
# MAGIC })
# MAGIC def experian_sandbox_variations():
# MAGIC
# MAGIC     df = spark.sql(
# MAGIC         f"""
# MAGIC     SELECT
# MAGIC
# MAGIC         """
# MAGIC     )
# MAGIC
# MAGIC     df = add_housekeeping_columns_filename_based()
# MAGIC
# MAGIC     return df

# COMMAND ----------

# DBTITLE 1,experian_sandbox_da_audit
@dp.table(
    name="experian_sandbox_da_audit",
    comment=""
)

def experian_sandbox_da_audit():

    df = spark.sql(
        f"""
    SELECT
        ids_da_audit                             AS IDS_DA_AUDIT
        ,arridx_application                      AS ARRIDX_APPLICATION
        ,adt_dt                                  AS ADT_DT
        ,adt_tm                                  AS ADT_TM
        ,daau_dt_vldty_prd                       AS DAAU_DT_VLDTY_PRD
        ,daau_gt_dt                              AS DAAU_GT_DT
        ,daca_dt_vldty_prd                       AS DACA_DT_VLDTY_PRD
        ,daca_gt_dt                              AS DACA_GT_DT
        ,dadel_dt_vldty_prd                      AS DADEL_DT_VLDTY_PRD
        ,dadel_gt_dt                             AS DADEL_GT_DT
        ,dadet_dt_vldty_prd                      AS DADET_DT_VLDTY_PRD
        ,dadet_gt_dt                             AS DADET_GT_DT
        ,daex_dt_vldty_prd                       AS DAEX_DT_VLDTY_PRD
        ,daex_gt_dt                              AS DAEX_GT_DT
        ,dain_dt_vldty_prd                       AS DAIN_DT_VLDTY_PRD
        ,dain_gt_dt                              AS DAIN_GT_DT
        ,dcsn_ctgry                              AS DCSN_CTGRY
        ,rsn_cd_tbl_1                            AS RSN_CD_TBL_1
        ,rsn_cd_tbl_10                           AS RSN_CD_TBL_10
        ,rsn_cd_tbl_11                           AS RSN_CD_TBL_11
        ,rsn_cd_tbl_12                           AS RSN_CD_TBL_12
        ,rsn_cd_tbl_13                           AS RSN_CD_TBL_13
        ,rsn_cd_tbl_14                           AS RSN_CD_TBL_14
        ,rsn_cd_tbl_15                           AS RSN_CD_TBL_15
        ,rsn_cd_tbl_16                           AS RSN_CD_TBL_16
        ,rsn_cd_tbl_17                           AS RSN_CD_TBL_17
        ,rsn_cd_tbl_18                           AS RSN_CD_TBL_18
        ,rsn_cd_tbl_19                           AS RSN_CD_TBL_19
        ,rsn_cd_tbl_2                            AS RSN_CD_TBL_2
        ,rsn_cd_tbl_20                           AS RSN_CD_TBL_20
        ,rsn_cd_tbl_3                            AS RSN_CD_TBL_3
        ,rsn_cd_tbl_4                            AS RSN_CD_TBL_4
        ,rsn_cd_tbl_5                            AS RSN_CD_TBL_5
        ,rsn_cd_tbl_6                            AS RSN_CD_TBL_6
        ,rsn_cd_tbl_7                            AS RSN_CD_TBL_7
        ,rsn_cd_tbl_8                            AS RSN_CD_TBL_8
        ,rsn_cd_tbl_9                            AS RSN_CD_TBL_9
        ,sys_createdate                          AS SYS_CREATEDATE
        ,usr_id                                  AS USR_ID
        ,ids_application                         AS IDS_APPLICATION
        ,'N'                                     AS IS_DELETED
        ,'NOT DELETED'                           AS IS_DELETED_REASON
        ,MDP_LOAD_ID                             AS MDP_LOAD_ID
        ,MDP_LOAD_DATETIME                       AS ROW_START_DATETIME
        ,_METADATA.file_name                     AS MDPTRN_FILENAME
        ,regexp_extract(
            MDPTRN_FILENAME
            ,'(\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{3})'
            , 0
        ) AS MDPTRN_FILENAME_TIMESTAMP
        ,date_format(
            to_timestamp(MDPTRN_FILENAME_TIMESTAMP, 'yyyy-MM-dd-HH-mm-ss-SSS')
            , 'yyyy-MM-dd HH:mm:ss.SSS'
        ) AS MDPTRN_UPSERT_TIMESTAMP

    FROM {env_var}_catalog.bronze_experian_sandbox_int.da_audit
        """
    )

    df = add_housekeeping_columns_filename_based(df, "IDS_DA_AUDIT", "MDPTRN_UPSERT_TIMESTAMP", "MDP_LOAD_ID")

    df = df.drop("MDPTRN_FILENAME", "MDPTRN_FILENAME_TIMESTAMP", "MDPTRN_UPSERT_TIMESTAMP")

    return df
