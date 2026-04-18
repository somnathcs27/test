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

# DBTITLE 1,experian_sandbox_variations
@dp.table(
    name="experian_sandbox_variations",
    comment=""
)

def dataverse_contact():

    df = spark.sql(
        f"""
    SELECT
        dt_ltst_updt                               AS DT_LTST_UPDT
        ,ids_variations                            AS IDS_VARIATIONS
        ,accnt_nmbr                                AS ACCNT_NMBR
        ,cl_accnt_nmbr                             AS CL_ACCNT_NMBR
        ,cl_clmt_dt_cpd                            AS CL_CLMT_DT_CPD
        ,cl_clmt_dt_prsnt                          AS CL_CLMT_DT_PRSNT
        ,dc_accnt_nmbr                             AS DC_ACCNT_NMBR
        ,dccu1ad_dstrct                            AS DCCU1AD_DSTRCT
        ,dccu1ad_flt                                AS DCCU1AD_FLT
        ,dccu1ad_frwrd_addrss_lnk                  AS DCCU1AD_FRWRD_ADDRSS_LNK
        ,dccu1ad_hsnm                               AS DCCU1AD_HSNM
        ,dccu1ad_hsnmbr                             AS DCCU1AD_HSNMBR
        ,dccu1ad_pstcd                              AS DCCU1AD_PSTCD
        ,dccu1ad_strt                               AS DCCU1AD_STRT
        ,dccu1_dt_of_brth                           AS DCCU1_DT_OF_BRTH
        ,dccu1_frnm                                 AS DCCU1_FRNM
        ,dccu1_gn_awy_or_dcsd_flg                   AS DCCU1_GN_AWY_OR_DCSD_FLG
        ,dccu1_othr_intls                           AS DCCU1_OTHR_INTLS
        ,dccu1_srnm                                 AS DCCU1_SRNM
        ,dccu1_ttl                                  AS DCCU1_TTL
        ,dccu2ad_dstrct                             AS DCCU2AD_DSTRCT
        ,dccu2ad_flt                                AS DCCU2AD_FLT
        ,dccu2ad_frwrd_addrss_lnk                   AS DCCU2AD_FRWRD_ADDRSS_LNK
        ,dccu2ad_hsnm                               AS DCCU2AD_HSNM
        ,dccu2ad_hsnmbr                             AS DCCU2AD_HSNMBR
        ,dccu2ad_pstcd                              AS DCCU2AD_PSTCD
        ,dccu2ad_strt                               AS DCCU2AD_STRT
        ,dccu2_dt_of_brth                           AS DCCU2_DT_OF_BRTH
        ,dccu2_frnm                                 AS DCCU2_FRNM
        ,dccu2_gn_awy_or_dcsd_flg                   AS DCCU2_GN_AWY_OR_DCSD_FLG
        ,dccu2_othr_intls                           AS DCCU2_OTHR_INTLS
        ,dccu2_srnm                                 AS DCCU2_SRNM
        ,dccu2_ttl                                  AS DCCU2_TTL
        ,dccu3ad_dstrct                             AS DCCU3AD_DSTRCT
        ,dccu3ad_flt                                AS DCCU3AD_FLT
        ,dccu3ad_frwrd_addrss_lnk                   AS DCCU3AD_FRWRD_ADDRSS_LNK
        ,dccu3ad_hsnm                               AS DCCU3AD_HSNM
        ,dccu3ad_hsnmbr                             AS DCCU3AD_HSNMBR
        ,dccu3ad_pstcd                              AS DCCU3AD_PSTCD
        ,dccu3ad_strt                               AS DCCU3AD_STRT
        ,dccu3_dt_of_brth                           AS DCCU3_DT_OF_BRTH
        ,dccu3_frnm                                 AS DCCU3_FRNM
        ,dccu3_gn_awy_or_dcsd_flg                   AS DCCU3_GN_AWY_OR_DCSD_FLG
        ,dccu3_othr_intls                           AS DCCU3_OTHR_INTLS
        ,dccu3_srnm                                 AS DCCU3_SRNM
        ,dccu3_ttl                                  AS DCCU3_TTL
        ,dccu4ad_dstrct                             AS DCCU4AD_DSTRCT
        ,dccu4ad_flt                                AS DCCU4AD_FLT
        ,dccu4ad_frwrd_addrss_lnk                   AS DCCU4AD_FRWRD_ADDRSS_LNK
        ,dccu4ad_hsnm                               AS DCCU4AD_HSNM
        ,dccu4ad_hsnmbr                             AS DCCU4AD_HSNMBR
        ,dccu4ad_pstcd                              AS DCCU4AD_PSTCD
        ,dccu4ad_strt                               AS DCCU4AD_STRT
        ,dccu4_dt_of_brth                           AS DCCU4_DT_OF_BRTH
        ,dccu4_frnm                                 AS DCCU4_FRNM
        ,dccu4_gn_awy_or_dcsd_flg                   AS DCCU4_GN_AWY_OR_DCSD_FLG
        ,dccu4_othr_intls                           AS DCCU4_OTHR_INTLS
        ,dccu4_srnm                                 AS DCCU4_SRNM
        ,dccu4_ttl                                  AS DCCU4_TTL
        ,dc_dcm_dt_cpd                              AS DC_DCM_DT_CPD
        ,dc_dcm_dt_prsnt                            AS DC_DCM_DT_PRSNT
        ,dc_dcm_dt_vwr_rqrd_flg                     AS DC_DCM_DT_VWR_RQRD_FLG
        ,dc_dlph_cll_dt                             AS DC_DLPH_CLL_DT
        ,dc_is_cstmrs_mtchng                        AS DC_IS_CSTMRS_MTCHNG
        ,ex_accnt_nmbr                              AS EX_ACCNT_NMBR
        ,ex_arrrs_in_mnths                          AS EX_ARRRS_IN_MNTHS
        ,ex_bhvrl_scr                               AS EX_BHVRL_SCR
        ,ex_bhvrl_scr_dt                            AS EX_BHVRL_SCR_DT
        ,ex_cptlfrmtrstfnd_amnt                     AS EX_CPTLFRMTRSTFND_AMNT
        ,excu1_dt_of_brth                           AS EXCU1_DT_OF_BRTH
        ,excu1_frnm                                 AS EXCU1_FRNM
        ,excu1_srnm                                 AS EXCU1_SRNM
        ,excu2_dt_of_brth                           AS EXCU2_DT_OF_BRTH
        ,excu2_frnm                                 AS EXCU2_FRNM
        ,excu2_srnm                                 AS EXCU2_SRNM
        ,excu3_dt_of_brth                           AS EXCU3_DT_OF_BRTH
        ,excu3_frnm                                 AS EXCU3_FRNM
        ,excu3_srnm                                 AS EXCU3_SRNM
        ,excu4_dt_of_brth                           AS EXCU4_DT_OF_BRTH
        ,excu4_frnm                                 AS EXCU4_FRNM
        ,excu4_srnm                                 AS EXCU4_SRNM
        ,ex_endwmntplcy_amnt                        AS EX_ENDWMNTPLCY_AMNT
        ,ex_exstng_mrtgg_dt_cpd                     AS EX_EXSTNG_MRTGG_DT_CPD
        ,ex_exstng_mrtgg_dt_prsnt                   AS EX_EXSTNG_MRTGG_DT_PRSNT
        ,ex_exstng_mrtgg_sgmnt                      AS EX_EXSTNG_MRTGG_SGMNT
        ,ex_int_prchs_typ                           AS EX_INT_PRCHS_TYP
        ,ex_intcmpltndt                             AS EX_INTCMPLTNDT
        ,ex_invstmnttrsts_amnt                      AS EX_INVSTMNTTRSTS_AMNT
        ,ex_is_amnt                                 AS EX_IS_AMNT
        ,ex_lndng_typ                               AS EX_LNDNG_TYP
        ,ex_lttng_typ                               AS EX_LTTNG_TYP
        ,ex_lttng_typ_gmd_fd                        AS EX_LTTNG_TYP_GMD_FD
        ,exlo1_c_i_blnc                             AS EXLO1_C_I_BLNC
        ,exlo1_crrnt_blnc                           AS EXLO1_CRRNT_BLNC
        ,exlo1_erc_indctr_prsnt                     AS EXLO1_ERC_INDCTR_PRSNT
        ,exlo1_i_blnc                               AS EXLO1_I_BLNC
        ,exlo1_offst_bss                            AS EXLO1_OFFST_BSS
        ,exlo1_orgnl_trm                            AS EXLO1_ORGNL_TRM
        ,exlo1_pymnt_amnt                           AS EXLO1_PYMNT_AMNT
        ,exlo1_prtbl                                AS EXLO1_PRTBL
        ,exlo1_rmnng_trm                            AS EXLO1_RMNNG_TRM
        ,exlo1_rpymnt_typ                           AS EXLO1_RPYMNT_TYP
        ,exlo1_swtchbl                              AS EXLO1_SWTCHBL
        ,exlo10_c_i_blnc                            AS EXLO10_C_I_BLNC
        ,exlo10_crrnt_blnc                          AS EXLO10_CRRNT_BLNC
        ,exlo10_erc_indctr_prsnt                    AS EXLO10_ERC_INDCTR_PRSNT
        ,exlo10_i_blnc                              AS EXLO10_I_BLNC
        ,exlo10_offst_bss                           AS EXLO10_OFFST_BSS
        ,exlo10_orgnl_trm                           AS EXLO10_ORGNL_TRM
        ,exlo10_pymnt_amnt                          AS EXLO10_PYMNT_AMNT
        ,exlo10_prtbl                               AS EXLO10_PRTBL
        ,exlo10_rmnng_trm                           AS EXLO10_RMNNG_TRM
        ,exlo10_rpymnt_typ                          AS EXLO10_RPYMNT_TYP
        ,exlo10_swtchbl                             AS EXLO10_SWTCHBL
        ,exlo2_c_i_blnc                             AS EXLO2_C_I_BLNC
        ,exlo2_crrnt_blnc                           AS EXLO2_CRRNT_BLNC
        ,exlo2_erc_indctr_prsnt                     AS EXLO2_ERC_INDCTR_PRSNT
        ,exlo2_i_blnc                               AS EXLO2_I_BLNC
        ,exlo2_offst_bss                            AS EXLO2_OFFST_BSS
        ,exlo2_orgnl_trm                            AS EXLO2_ORGNL_TRM
        ,exlo2_pymnt_amnt                           AS EXLO2_PYMNT_AMNT
        ,exlo2_prtbl                                AS EXLO2_PRTBL
        ,exlo2_rmnng_trm                            AS EXLO2_RMNNG_TRM
        ,exlo2_rpymnt_typ                           AS EXLO2_RPYMNT_TYP
        ,exlo2_swtchbl                              AS EXLO2_SWTCHBL
        ,exlo3_c_i_blnc                             AS EXLO3_C_I_BLNC
        ,exlo3_crrnt_blnc                           AS EXLO3_CRRNT_BLNC
        ,exlo3_erc_indctr_prsnt                     AS EXLO3_ERC_INDCTR_PRSNT
        ,exlo3_i_blnc                               AS EXLO3_I_BLNC
        ,exlo3_offst_bss                            AS EXLO3_OFFST_BSS
        ,exlo3_orgnl_trm                            AS EXLO3_ORGNL_TRM
        ,exlo3_pymnt_amnt                           AS EXLO3_PYMNT_AMNT
        ,exlo3_prtbl                                AS EXLO3_PRTBL
        ,exlo3_rmnng_trm                            AS EXLO3_RMNNG_TRM
        ,exlo3_rpymnt_typ                           AS EXLO3_RPYMNT_TYP
        ,exlo3_swtchbl                              AS EXLO3_SWTCHBL
        ,exlo4_c_i_blnc                             AS EXLO4_C_I_BLNC
        ,exlo4_crrnt_blnc                           AS EXLO4_CRRNT_BLNC
        ,exlo4_erc_indctr_prsnt                     AS EXLO4_ERC_INDCTR_PRSNT
        ,exlo4_i_blnc                               AS EXLO4_I_BLNC
        ,exlo4_offst_bss                            AS EXLO4_OFFST_BSS
        ,exlo4_orgnl_trm                            AS EXLO4_ORGNL_TRM
        ,exlo4_pymnt_amnt                           AS EXLO4_PYMNT_AMNT
        ,exlo4_prtbl                                AS EXLO4_PRTBL
        ,exlo4_rmnng_trm                            AS EXLO4_RMNNG_TRM
        ,exlo4_rpymnt_typ                           AS EXLO4_RPYMNT_TYP
        ,exlo4_swtchbl                              AS EXLO4_SWTCHBL
        ,exlo5_c_i_blnc                             AS EXLO5_C_I_BLNC
        ,exlo5_crrnt_blnc                           AS EXLO5_CRRNT_BLNC
        ,exlo5_erc_indctr_prsnt                     AS EXLO5_ERC_INDCTR_PRSNT
        ,exlo5_i_blnc                               AS EXLO5_I_BLNC
        ,exlo5_offst_bss                            AS EXLO5_OFFST_BSS
        ,exlo5_orgnl_trm                            AS EXLO5_ORGNL_TRM
        ,exlo5_pymnt_amnt                           AS EXLO5_PYMNT_AMNT
        ,exlo5_prtbl                                AS EXLO5_PRTBL
        ,exlo5_rmnng_trm                            AS EXLO5_RMNNG_TRM
        ,exlo5_rpymnt_typ                           AS EXLO5_RPYMNT_TYP
        ,exlo5_swtchbl                              AS EXLO5_SWTCHBL
        ,exlo6_c_i_blnc                             AS EXLO6_C_I_BLNC
        ,exlo6_crrnt_blnc                           AS EXLO6_CRRNT_BLNC
        ,exlo6_erc_indctr_prsnt                     AS EXLO6_ERC_INDCTR_PRSNT
        ,exlo6_i_blnc                               AS EXLO6_I_BLNC
        ,exlo6_offst_bss                            AS EXLO6_OFFST_BSS
        ,exlo6_orgnl_trm                            AS EXLO6_ORGNL_TRM
        ,exlo6_pymnt_amnt                           AS EXLO6_PYMNT_AMNT
        ,exlo6_prtbl                                AS EXLO6_PRTBL
        ,exlo6_rmnng_trm                            AS EXLO6_RMNNG_TRM
        ,exlo6_rpymnt_typ                           AS EXLO6_RPYMNT_TYP
        ,exlo6_swtchbl                              AS EXLO6_SWTCHBL
        ,exlo7_c_i_blnc                             AS EXLO7_C_I_BLNC
        ,exlo7_crrnt_blnc                           AS EXLO7_CRRNT_BLNC
        ,exlo7_erc_indctr_prsnt                     AS EXLO7_ERC_INDCTR_PRSNT
        ,exlo7_i_blnc                               AS EXLO7_I_BLNC
        ,exlo7_offst_bss                            AS EXLO7_OFFST_BSS
        ,exlo7_orgnl_trm                            AS EXLO7_ORGNL_TRM
        ,exlo7_pymnt_amnt                           AS EXLO7_PYMNT_AMNT
        ,exlo7_prtbl                                AS EXLO7_PRTBL
        ,exlo7_rmnng_trm                            AS EXLO7_RMNNG_TRM
        ,exlo7_rpymnt_typ                           AS EXLO7_RPYMNT_TYP
        ,exlo7_swtchbl                              AS EXLO7_SWTCHBL
        ,exlo8_c_i_blnc                             AS EXLO8_C_I_BLNC
        ,exlo8_crrnt_blnc                           AS EXLO8_CRRNT_BLNC
        ,exlo8_erc_indctr_prsnt                     AS EXLO8_ERC_INDCTR_PRSNT
        ,exlo8_i_blnc                               AS EXLO8_I_BLNC
        ,exlo8_offst_bss                            AS EXLO8_OFFST_BSS
        ,exlo8_orgnl_trm                            AS EXLO8_ORGNL_TRM
        ,exlo8_pymnt_amnt                           AS EXLO8_PYMNT_AMNT
        ,exlo8_prtbl                                AS EXLO8_PRTBL
        ,exlo8_rmnng_trm                            AS EXLO8_RMNNG_TRM
        ,exlo8_rpymnt_typ                           AS EXLO8_RPYMNT_TYP
        ,exlo8_swtchbl                              AS EXLO8_SWTCHBL
        ,exlo9_c_i_blnc                             AS EXLO9_C_I_BLNC
        ,exlo9_crrnt_blnc                           AS EXLO9_CRRNT_BLNC
        ,exlo9_erc_indctr_prsnt                     AS EXLO9_ERC_INDCTR_PRSNT
        ,exlo9_i_blnc                               AS EXLO9_I_BLNC
        ,exlo9_offst_bss                            AS EXLO9_OFFST_BSS
        ,exlo9_orgnl_trm                            AS EXLO9_ORGNL_TRM
        ,exlo9_pymnt_amnt                           AS EXLO9_PYMNT_AMNT
        ,exlo9_prtbl                                AS EXLO9_PRTBL
        ,exlo9_rmnng_trm                            AS EXLO9_RMNNG_TRM
        ,exlo9_rpymnt_typ                           AS EXLO9_RPYMNT_TYP
        ,exlo9_swtchbl                              AS EXLO9_SWTCHBL
        ,ex_mrtgg_in_arrrs                          AS EX_MRTGG_IN_ARRRS
        ,ex_orgnl_accnt_opn_dt                      AS EX_ORGNL_ACCNT_OPN_DT
        ,ex_othr_amnt                               AS EX_OTHR_AMNT
        ,ex_ownrshp_typ                             AS EX_OWNRSHP_TYP
        ,ex_pnsnpln_amnt                            AS EX_PNSNPLN_AMNT
        ,ex_prmmbnds_amnt                           AS EX_PRMMBNDS_AMNT
        ,ex_prprty_prps                             AS EX_PRPRTY_PRPS
        ,ex_slfthrprprty_amnt                       AS EX_SLFTHRPRPRTY_AMNT
        ,ex_slfscrtyprprty_amnt                     AS EX_SLFSCRTYPRPRTY_AMNT
        ,ex_svngs_amnt                              AS EX_SVNGS_AMNT
        ,ex_slf_bld_stg_cd                          AS EX_SLF_BLD_STG_CD
        ,ex_stcksndshrs_amnt                        AS EX_STCKSNDSHRS_AMNT
        ,ex_tnr                                     AS EX_TNR
        ,ex_ukftslstdscrtsndshrs_amnt               AS EX_UKFTSLSTDSCRTSNDSHRS_AMNT
        ,ex_unttrsts_amnt                         	AS EX_UNTTRSTS_AMNT
        ,ex_yr_prprty_blt                         	AS EX_YR_PRPRTY_BLT
        ,ho_accnt_nmbr                            	AS HO_ACCNT_NMBR
        ,ho_dt_of_avm                             	AS HO_DT_OF_AVM
        ,ho_hmtrck_dt_cpd                         	AS HO_HMTRCK_DT_CPD
        ,ho_hmtrck_dt_prsnt                       	AS HO_HMTRCK_DT_PRSNT
        ,ho_rntl_cnfdnc_lvl                       	AS HO_RNTL_CNFDNC_LVL
        ,ho_rntl_vl_amnt                          	AS HO_RNTL_VL_AMNT
        ,ho_vltn_amnt                             	AS HO_VLTN_AMNT
        ,ho_vltn_cnfdnc_lvl                       	AS HO_VLTN_CNFDNC_LVL
        ,sys_createdate                           	AS SYS_CREATEDATE
        ,ex_ab_i_amnt_hldng_fld                   	AS EX_AB_I_AMNT_HLDNG_FLD
        ,ex_ab_ln_amnt_hldng_fld                  	AS EX_AB_LN_AMNT_HLDNG_FLD
        ,ex_prprty_lctn_gmd_fd                    	AS EX_PRPRTY_LCTN_GMD_FD
        ,dc_us_dlph_br_dt_flg                     	AS DC_US_DLPH_BR_DT_FLG
        ,ho_bdrms                                 	AS HO_BDRMS
        ,ho_hmtrck_dt_bdrms                       	AS HO_HMTRCK_DT_BDRMS
        ,ho_hmtrck_dt_prprty_sb_typ               	AS HO_HMTRCK_DT_PRPRTY_SB_TYP
        ,ho_hmtrck_dt_prprty_typ                  	AS HO_HMTRCK_DT_PRPRTY_TYP
        ,ho_hmtrck_prvs_vl                        	AS HO_HMTRCK_PRVS_VL
        ,ho_hmtrck_prvs_vl_dt                     	AS HO_HMTRCK_PRVS_VL_DT
        ,ho_prprty_styl                           	AS HO_PRPRTY_STYL
        ,ho_prprty_typ                            	AS HO_PRPRTY_TYP
        ,'N'                                        AS IS_DELETED
        ,'NOT DELETED'                              AS IS_DELETED_REASON
        ,MDP_LOAD_ID                              	AS MDP_LOAD_ID  
        ,MDP_LOAD_DATETIME                          AS ROW_START_DATETIME

    FROM {env_var}_catalog.bronze_experian_sandbox_con.variations
        """
    )

    df = add_housekeeping_columns(df, "IDS_VARIATIONS")

    return df
