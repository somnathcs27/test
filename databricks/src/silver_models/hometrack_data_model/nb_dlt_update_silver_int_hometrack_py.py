# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "hometrack_coastal",
    comment = "data relating to the coastal hazard scores for low, medium and high emissions"
)
@dlt.expect("UPRN is not null", "UPRN IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_coastal():

    df = spark.sql(f"""
    SELECT
        bh.reference AS REFERENCE,
        bh.UPRN AS UPRN,
        bh.PCA AS PCA,
        bh.PCD AS PCD,
        bh.Region AS REGION,
        CAST(bh.coast_bl AS INT) AS COASTAL_HAZARDS_BASELINE_SCORE,
        CAST(bh.coast_20l AS INT) AS COASTAL_HAZARDS_SCORE_2020_LOW_EMMISSIONS,
        CAST(bh.coast_20m AS INT) AS COASTAL_HAZARDS_SCORE_2020_MED_EMMISSIONS,
        CAST(bh.coast_20h AS INT) AS COASTAL_HAZARDS_SCORE_2020_HIGH_EMMISSIONS,
        CAST(bh.coast_50l AS INT) AS COASTAL_HAZARDS_SCORE_2050_LOW_EMMISSIONS,
        CAST(bh.coast_50m AS INT) AS COASTAL_HAZARDS_SCORE_2050_MED_EMMISSIONS,
        CAST(bh.coast_50h AS INT) AS COASTAL_HAZARDS_SCORE_2050_HIGH_EMMISSIONS,
        CAST(bh.coast_80l AS INT) AS COASTAL_HAZARDS_SCORE_2080_LOW_EMMISSIONS,
        CAST(bh.coast_80m AS INT) AS COASTAL_HAZARDS_SCORE_2080_MED_EMMISSIONS,
        CAST(bh.coast_80h AS INT) AS COASTAL_HAZARDS_SCORE_2080_HIGH_EMMISSIONS,

        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

        CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_int.coastal bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UPRN", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "hometrack_epc_rating",
    comment = "Data Relating to the rating given for the EPC (Energy Performance Certificate)"
)
@dlt.expect("UPRN is not null", "UPRN IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_epc_rating():

    df = spark.sql(f"""
    SELECT
        bh.reference AS REFERENCE,
        bh.UPRN AS UPRN,
        bh.PCA AS PCA,
        bh.PCD AS PCD,
        bh.Region AS REGION,
        bh.LMK_KEY AS LMK_KEY,
        bh.ADDRESS AS ADDRESS,
        bh.POSTCODE AS POSTCODE,
        CAST(bh.inspectionDate AS TIMESTAMP) AS INSPECTION_DATETIME,
        CAST(bh.lodgementDate AS TIMESTAMP) AS LODGEMENT_DATETIME,
        CAST(bh.reportDate AS DATE) AS REPORT_DATE,
        bh.propertyType AS PROPERTY_TYPE,
        bh.propertyStyle AS PROPERTY_STYLE,
        CAST(bh.totalFloorArea AS DECIMAL(18,2)) AS TOTAL_FLOOR_AREA,
        CAST(bh.numberTotalRooms AS INT) AS NUMBER_TOTAL_ROOMS,
        CAST(bh.numberHeatedRooms AS INT) AS NUMBER_HEATED_ROOMS,
        bh.energyRatingCurrent AS ENERGY_RATING_CURRENT,
        bh.energyRatingPotential AS ENERGY_RATING_POTENTIAL,
        CAST(bh.energyEfficiencyCurrent AS INT) AS ENERGY_EFFICIENCY_CURRENT,
        CAST(bh.energyEfficiencyPotential AS INT) AS ENERGY_EFFICIENCY_POTENTIAL,
        CAST(bh.environmentImpactCurrent AS INT) AS ENVIRONMENT_IMPACT_CURRENT,
        CAST(bh.environmentalImpactPotential AS INT) AS ENVIRONMENTAL_IMPACT_POTENTIAL,
        CAST(bh.energyConsumptionCurrent AS INT) AS ENERGY_CONSUMPTION_CURRENT,
        CAST(bh.energyConsumtpionPotential AS INT) AS ENERGY_CONSUMTPION_POTENTIAL,
        CAST(bh.co2EmissionsCurrent AS DECIMAL(38,1)) AS CO2_EMISSIONS_CURRENT,
        CAST(bh.co2EmissionsPotential AS DECIMAL(38,1)) AS CO2_EMISSIONS_POTENTIAL,
        CAST(bh.lightingCostCurrent AS DECIMAL(38,2)) AS LIGHTING_COST_CURRENT,
        CAST(bh.lightingCostPotential AS DECIMAL(38,2)) AS LIGHTING_COST_POTENTIAL,
        CAST(bh.heatingCostCurrent AS DECIMAL(38,2)) AS HEATING_COST_CURRENT,
        CAST(bh.heatingCostPotential AS DECIMAL(38,2)) AS HEATING_COST_POTENTIAL,
        CAST(bh.hotWaterCostCurrent AS DECIMAL(38,2)) AS HOT_WATER_COST_CURRENT,
        CAST(bh.hotWaterCostPotential AS DECIMAL(38,2)) AS HOT_WATER_COST_POTENTIAL,
        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

        CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_int.epc_rating bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UPRN", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "hometrack_epc_recommendations",
    comment = "The data relating to the recommendations for the EPC (Energy Performance Certificate)"
)
@dlt.expect("UPRN is not null", "UPRN IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_epc_recommendations():

    df = spark.sql(f"""
    SELECT
        bh.reference AS REFERENCE,
        bh.UPRN AS UPRN,
        bh.PCA AS PCA,
        bh.PCD AS PCD,
        bh.Region AS REGION,
        bh.LMK_Key AS LMK_KEY,
        CAST(bh.ImprovementItem AS INT) AS IMPROVEMENT_ITEM,
        bh.ImprovementSummaryText AS IMPROVEMENT_SUMMARY_TEXT,
        bh.ImprovementDescriptiveText AS IMPROVEMENT_DESCRIPTIVE_TEXT,
        CAST(bh.ImprovementID AS BIGINT) AS IMPROVEMENT_ID,
        bh.ImprovementIDText AS IMPROVEMENT_ID_TEXT,
        bh.IndicativeCost AS INDICATIVE_COST,

        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

        CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_int.epc_recommendations bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UPRN", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "hometrack_flood_present",
    comment = "Flood data relating to the present"
)
@dlt.expect("UPRN is not null", "UPRN IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_flood_present():

    df = spark.sql(f"""
    SELECT
        bh.reference AS REFERENCE,
        bh.UPRN AS UPRN,
        bh.PCA AS PCA,
        bh.PCD AS PCD,
        bh.Region AS REGION,
        bh.timehorizon AS TIME_HORIZON,
        bh.RCP AS RCP,
        CAST(bh.undef_f30max AS INT) AS FLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_f75max AS INT) AS FLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_f100max AS INT) AS FLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_f250max AS INT) AS FLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_f500max AS INT) AS FLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_f1000max AS INT) AS FLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_p30max AS INT) AS PLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_p75max AS INT) AS PLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_p100max AS INT) AS PLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_p250max AS INT) AS PLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_p500max AS INT) AS PLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_p1000max AS INT) AS PLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_t30max AS INT) AS TIDAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_t75max AS INT) AS TIDAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_t100max AS INT) AS TIDAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_t250max AS INT) AS TIDAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_t500max AS INT) AS TIDAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_t1000max AS INT) AS TIDAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_f_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_FLUVIAL_FLOODING,
        CAST(bh.undef_p_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_PLUVIAL_FLOODING,
        CAST(bh.undef_t_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_TIDAL_FLOODING,
        CAST(bh.undef_c_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_COMBINED_FLOODING,
        CAST(bh.undef_f_rr AS INT) AS FLUVIAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_p_rr AS INT) AS PLUVIAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_t_rr AS INT) AS TIDAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_c_rr AS INT) AS COMBINED_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_f_score AS INT) AS FLUVIAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_p_score AS INT) AS PLUVIAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_t_score AS INT) AS TIDAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_c_score AS INT) AS COMBINED_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_f_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_FLUVIAL_FLOODING,
        CAST(bh.undef_p_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_PLUVIAL_FLOODING,
        CAST(bh.undef_t_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_TIDAL_FLOODING,
        CAST(bh.undef_c_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_COMBINED_FLOODING,
        CAST(bh.def_f30max AS INT) AS FLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f75max AS INT) AS FLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f100max AS INT) AS FLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f250max AS INT) AS FLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f500max AS INT) AS FLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f1000max AS INT) AS FLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p30max AS INT) AS PLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p75max AS INT) AS PLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p100max AS INT) AS PLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p250max AS INT) AS PLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p500max AS INT) AS PLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p1000max AS INT) AS PLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t30max AS INT) AS TIDAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t75max AS INT) AS TIDAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t100max AS INT) AS TIDAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t250max AS INT) AS TIDAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t500max AS INT) AS TIDAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t1000max AS INT) AS TIDAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_FLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_PLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_TIDAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_COMBINED_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_rr AS INT) AS FLUVIAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_rr AS INT) AS PLUVIAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_rr AS INT) AS TIDAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_rr AS INT) AS COMBINED_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_score AS INT) AS FLUVIAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_score AS INT) AS PLUVIAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_score AS INT) AS TIDAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_score AS INT) AS COMBINED_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_FLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_PLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_TIDAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_COMBINED_FLOODING_INCL_FLOOD_DEFENCE,

        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

        CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_int.floodpresent bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UPRN", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "hometrack_flood_future_20",
    comment = "Flood data relating to hazards in the 2020's"
)
@dlt.expect("UPRN is not null", "UPRN IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_flood_future_20():

    df = spark.sql(f"""
    SELECT
        bh.reference AS REFERENCE,
        bh.UPRN AS UPRN,
        bh.PCA AS PCA,
        bh.PCD AS PCD,
        bh.Region AS REGION,
        bh.timehorizon AS TIMEHORIZON,
        bh.RCP AS RCP,
        CAST(bh.undef_f30max AS INT) AS FLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_f75max AS INT) AS FLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_f100max AS INT) AS FLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_f250max AS INT) AS FLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_f500max AS INT) AS FLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_f1000max AS INT) AS FLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_p30max AS INT) AS PLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_p75max AS INT) AS PLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_p100max AS INT) AS PLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_p250max AS INT) AS PLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_p500max AS INT) AS PLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_p1000max AS INT) AS PLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_t30max AS INT) AS TIDAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_t75max AS INT) AS TIDAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_t100max AS INT) AS TIDAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_t250max AS INT) AS TIDAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_t500max AS INT) AS TIDAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_t1000max AS INT) AS TIDAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_f_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_FLUVIAL_FLOODING,
        CAST(bh.undef_p_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_PLUVIAL_FLOODING,
        CAST(bh.undef_t_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_TIDAL_FLOODING,
        CAST(bh.undef_c_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_COMBINED_FLOODING,
        CAST(bh.undef_f_rr AS INT) AS FLUVIAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_p_rr AS INT) AS PLUVIAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_t_rr AS INT) AS TIDAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_c_rr AS INT) AS COMBINED_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_f_score AS INT) AS FLUVIAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_p_score AS INT) AS PLUVIAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_t_score AS INT) AS TIDAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_c_score AS INT) AS COMBINED_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_f_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_FLUVIAL_FLOODING,
        CAST(bh.undef_p_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_PLUVIAL_FLOODING,
        CAST(bh.undef_t_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_TIDAL_FLOODING,
        CAST(bh.undef_c_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_COMBINED_FLOODING,
        CAST(bh.def_f30max AS INT) AS FLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f75max AS INT) AS FLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f100max AS INT) AS FLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f250max AS INT) AS FLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f500max AS INT) AS FLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f1000max AS INT) AS FLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p30max AS INT) AS PLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p75max AS INT) AS PLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p100max AS INT) AS PLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p250max AS INT) AS PLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p500max AS INT) AS PLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p1000max AS INT) AS PLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t30max AS INT) AS TIDAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t75max AS INT) AS TIDAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t100max AS INT) AS TIDAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t250max AS INT) AS TIDAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t500max AS INT) AS TIDAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t1000max AS INT) AS TIDAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_FLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_PLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_TIDAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_COMBINED_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_rr AS INT) AS FLUVIAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_rr AS INT) AS PLUVIAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_rr AS INT) AS TIDAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_rr AS INT) AS COMBINED_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_score AS INT) AS FLUVIAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_score AS INT) AS PLUVIAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_score AS INT) AS TIDAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_score AS INT) AS COMBINED_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_FLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_PLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_TIDAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_COMBINED_FLOODING_INCL_FLOOD_DEFENCE,
        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

        CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_int.floodfuture20 bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UPRN", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "hometrack_flood_future_50",
    comment = "Flood data relating to hazards in the 2050's"
)
@dlt.expect("UPRN is not null", "UPRN IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_flood_future_50():

    df = spark.sql(f"""
    SELECT
        bh.reference AS REFERENCE,
        CAST(bh.UPRN AS BIGINT) AS UPRN,
        bh.PCA AS PCA,
        bh.PCD AS PCD,
        bh.Region AS REGION,
        bh.timehorizon AS TIME_HORIZON,
        bh.RCP AS RCP,
        CAST(bh.undef_f30max AS INT) AS FLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_f75max AS INT) AS FLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_f100max AS INT) AS FLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_f250max AS INT) AS FLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_f500max AS INT) AS FLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_f1000max AS INT) AS FLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_p30max AS INT) AS PLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_p75max AS INT) AS PLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_p100max AS INT) AS PLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_p250max AS INT) AS PLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_p500max AS INT) AS PLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_p1000max AS INT) AS PLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_t30max AS INT) AS TIDAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_t75max AS INT) AS TIDAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_t100max AS INT) AS TIDAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_t250max AS INT) AS TIDAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_t500max AS INT) AS TIDAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_t1000max AS INT) AS TIDAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_f_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_FLUVIAL_FLOODING,
        CAST(bh.undef_p_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_PLUVIAL_FLOODING,
        CAST(bh.undef_t_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_TIDAL_FLOODING,
        CAST(bh.undef_c_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_COMBINED_FLOODING,
        CAST(bh.undef_f_rr AS INT) AS FLUVIAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_p_rr AS INT) AS PLUVIAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_t_rr AS INT) AS TIDAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_c_rr AS INT) AS COMBINED_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_f_score AS INT) AS FLUVIAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_p_score AS INT) AS PLUVIAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_t_score AS INT) AS TIDAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_c_score AS INT) AS COMBINED_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_f_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_FLUVIAL_FLOODING,
        CAST(bh.undef_p_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_PLUVIAL_FLOODING,
        CAST(bh.undef_t_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_TIDAL_FLOODING,
        CAST(bh.undef_c_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_COMBINED_FLOODING,
        CAST(bh.def_f30max AS INT) AS FLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f75max AS INT) AS FLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f100max AS INT) AS FLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f250max AS INT) AS FLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f500max AS INT) AS FLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f1000max AS INT) AS FLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p30max AS INT) AS PLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p75max AS INT) AS PLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p100max AS INT) AS PLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p250max AS INT) AS PLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p500max AS INT) AS PLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p1000max AS INT) AS PLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t30max AS INT) AS TIDAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t75max AS INT) AS TIDAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t100max AS INT) AS TIDAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t250max AS INT) AS TIDAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t500max AS INT) AS TIDAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t1000max AS INT) AS TIDAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_FLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_PLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_TIDAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_COMBINED_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_rr AS INT) AS FLUVIAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_rr AS INT) AS PLUVIAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_rr AS INT) AS TIDAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_rr AS INT) AS COMBINED_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_score AS INT) AS FLUVIAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_score AS INT) AS PLUVIAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_score AS INT) AS TIDAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_score AS INT) AS COMBINED_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_FLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_PLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_TIDAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_COMBINED_FLOODING_INCL_FLOOD_DEFENCE,
        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

      CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
      bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_int.floodfuture50 bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UPRN", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "hometrack_flood_future_80",
    comment = "Flood data relating to hazards in the 2080's"
)
@dlt.expect("UPRN is not null", "UPRN IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_flood_future_80():

    df = spark.sql(f"""
    SELECT
        bh.reference AS REFERENCE,
        bh.UPRN AS UPRN,
        bh.PCA AS PCA,
        bh.PCD AS PCD,
        bh.Region AS REGION,
        bh.timehorizon AS TIME_HORIZON,
        bh.RCP AS RCP,
        CAST(bh.undef_f30max AS INT) AS FLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_f75max AS INT) AS FLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_f100max AS INT) AS FLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_f250max AS INT) AS FLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_f500max AS INT) AS FLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_f1000max AS INT) AS FLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_p30max AS INT) AS PLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_p75max AS INT) AS PLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_p100max AS INT) AS PLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_p250max AS INT) AS PLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_p500max AS INT) AS PLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_p1000max AS INT) AS PLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_t30max AS INT) AS TIDAL_FLOOD_DEPTH_30_YEAR_RETURN,
        CAST(bh.undef_t75max AS INT) AS TIDAL_FLOOD_DEPTH_75_YEAR_RETURN,
        CAST(bh.undef_t100max AS INT) AS TIDAL_FLOOD_DEPTH_100_YEAR_RETURN,
        CAST(bh.undef_t250max AS INT) AS TIDAL_FLOOD_DEPTH_250_YEAR_RETURN,
        CAST(bh.undef_t500max AS INT) AS TIDAL_FLOOD_DEPTH_500_YEAR_RETURN,
        CAST(bh.undef_t1000max AS INT) AS TIDAL_FLOOD_DEPTH_1000_YEAR_RETURN,
        CAST(bh.undef_f_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_FLUVIAL_FLOODING,
        CAST(bh.undef_p_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_PLUVIAL_FLOODING,
        CAST(bh.undef_t_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_TIDAL_FLOODING,
        CAST(bh.undef_c_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_COMBINED_FLOODING,
        CAST(bh.undef_f_rr AS INT) AS FLUVIAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_p_rr AS INT) AS PLUVIAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_t_rr AS INT) AS TIDAL_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_c_rr AS INT) AS COMBINED_FLOOD_SCORE_RISK_RATING,
        CAST(bh.undef_f_score AS INT) AS FLUVIAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_p_score AS INT) AS PLUVIAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_t_score AS INT) AS TIDAL_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_c_score AS INT) AS COMBINED_FLOOD_SCORE_RATING_SUMMARISED,
        CAST(bh.undef_f_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_FLUVIAL_FLOODING,
        CAST(bh.undef_p_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_PLUVIAL_FLOODING,
        CAST(bh.undef_t_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_TIDAL_FLOODING,
        CAST(bh.undef_c_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_COMBINED_FLOODING,
        CAST(bh.def_f30max AS INT) AS FLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f75max AS INT) AS FLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f100max AS INT) AS FLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f250max AS INT) AS FLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f500max AS INT) AS FLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f1000max AS INT) AS FLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p30max AS INT) AS PLUVIAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p75max AS INT) AS PLUVIAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p100max AS INT) AS PLUVIAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p250max AS INT) AS PLUVIAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p500max AS INT) AS PLUVIAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p1000max AS INT) AS PLUVIAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t30max AS INT) AS TIDAL_FLOOD_DEPTH_30_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t75max AS INT) AS TIDAL_FLOOD_DEPTH_75_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t100max AS INT) AS TIDAL_FLOOD_DEPTH_100_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t250max AS INT) AS TIDAL_FLOOD_DEPTH_250_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t500max AS INT) AS TIDAL_FLOOD_DEPTH_500_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t1000max AS INT) AS TIDAL_FLOOD_DEPTH_1000_YEAR_RETURN_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_FLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_PLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_TIDAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_adr AS INT) AS ANNUAL_DAMAGE_RATIO_FROM_COMBINED_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_rr AS INT) AS FLUVIAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_rr AS INT) AS PLUVIAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_rr AS INT) AS TIDAL_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_rr AS INT) AS COMBINED_FLOOD_SCORE_RISK_RATING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_score AS INT) AS FLUVIAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_score AS INT) AS PLUVIAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_score AS INT) AS TIDAL_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_score AS INT) AS COMBINED_FLOOD_SCORE_RATING_SUMMARISED_INCL_FLOOD_DEFENCE,
        CAST(bh.def_f_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_FLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_p_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_PLUVIAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_t_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_TIDAL_FLOODING_INCL_FLOOD_DEFENCE,
        CAST(bh.def_c_aal AS INT) AS AVERAGE_ANNUAL_LOSS_FROM_COMBINED_FLOODING_INCL_FLOOD_DEFENCE,
        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

        CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_int.floodfuture80 bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UPRN", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "hometrack_ground",
    comment = "Environment data relating to the ground i.e. soil, mining and sinkhole hazards "
)
@dlt.expect("UPRN is not null", "UPRN IS NOT NULL")
@dlt.expect("MDP_FILE_NAME_DATE is recent", "MDP_FILE_NAME_DATE > '1900-01-01'")
def silver_hometrack_ground():

    # NB column names need some interpretation.
    df = spark.sql(f"""
    SELECT
        bh.reference AS REFERENCE,
        bh.UPRN AS UPRN,
        bh.PCA AS PCA,
        bh.PCD AS PCD,
        bh.Region AS REGION,
        CAST(bh.highest_bl AS INT) AS HIGHEST_BASELINE_SCORE,
        CAST(bh.highest20l AS INT) AS HIGHEST_FUTURE_SCORE_2020_LOW_EMMISIONS,
        CAST(bh.highest20m AS INT) AS HIGHEST_FUTURE_SCORE_2020_MED_EMMISIONS,
        CAST(bh.highest20h AS INT) AS HIGHEST_FUTURE_SCORE_2020_HIGH_EMMISIONS,
        CAST(bh.highest50l AS INT) AS HIGHEST_FUTURE_SCORE_2050_LOW_EMMISIONS,
        CAST(bh.highest50m AS INT) AS HIGHEST_FUTURE_SCORE_2050_MED_EMMISIONS,
        CAST(bh.highest50h AS INT) AS HIGHEST_FUTURE_SCORE_2050_HIGH_EMMISIONS,
        CAST(bh.highest80l AS INT) AS HIGHEST_FUTURE_SCORE_2080_LOW_EMMISIONS,
        CAST(bh.highest80m AS INT) AS HIGHEST_FUTURE_SCORE_2080_MEDIUM_EMMISIONS,
        CAST(bh.highest80h AS INT) AS HIGHEST_FUTURE_SCORE_2080_HIGH_EMMISIONS,
        CAST(bh.mining_bl AS INT) AS MINING_BASELINE_SCORE,
        CAST(bh.miningn_bl AS INT) AS MINING_BASELINE_SCORE_NON_COAL,
        CAST(bh.miningc_bl AS INT) AS MINING_BASELINE_SCORE_COAL,
        CAST(bh.miningb_bl AS INT) AS MINING_BASELINE_SCORE_CHESHIRE_BRINE,
        CAST(bh.dissol_bl AS INT) AS DISSOLUTION_BASELINE_SCORE,
        CAST(bh.snkhol_bl AS INT) AS SNKHOLE_BASELINE_SCORE,
        CAST(bh.misc_bl AS INT) AS MISCELLANEOUS_BASELINE_SCORE,
        CAST(bh.lslip_bl AS INT) AS LANDSLIP_BASELINE_SCORE,
        CAST(bh.ssubs_bl AS INT) AS SOIL_RELATED_SUBSIDENCE_BASELINE_SCORE,
        CAST(bh.ssubs_20l AS INT) AS SOIL_RELATED_SUBSIDENCE_2020_LOW_EMMISIONS,
        CAST(bh.ssubs_20m AS INT) AS SOIL_RELATED_SUBSIDENCE_2020_MED_EMMISIONS,
        CAST(bh.ssubs_20h AS INT) AS SOIL_RELATED_SUBSIDENCE_2020_HIGH_EMMISIONS,
        CAST(bh.ssubs_50l AS INT) AS SOIL_RELATED_SUBSIDENCE_2050_LOW_EMMISIONS,
        CAST(bh.ssubs_50m AS INT) AS SOIL_RELATED_SUBSIDENCE_2050_MED_EMMISIONS,
        CAST(bh.ssubs_50h AS INT) AS SOIL_RELATED_SUBSIDENCE_2050_HIGH_EMMISIONS,
        CAST(bh.ssubs_80l AS INT) AS SOIL_RELATED_SUBSIDENCE_2080_LOW_EMMISIONS,
        CAST(bh.ssubs_80m AS INT) AS SOIL_RELATED_SUBSIDENCE_2080_MED_EMMISIONS,
        CAST(bh.ssubs_80h AS INT) AS SOIL_RELATED_SUBSIDENCE_2080_HIGH_EMMISIONS,
        CAST(bh.subs_base AS INT) AS SOIL_HAZARD_BASELINE_SCORE,
        CAST(bh.subs_low20 AS INT) AS SOIL_HAZARD_SCORE_2020_LOW_EMMISIONS,
        CAST(bh.subs_low50 AS INT) AS SOIL_HAZARD_SCORE_2050_LOW_EMMISIONS,
        CAST(bh.subs_low80 AS INT) AS SOIL_HAZARD_SCORE_2080_LOW_EMMISIONS,
        CAST(bh.subs_med20 AS INT) AS SOIL_HAZARD_SCORE_2020_MED_EMMISIONS,
        CAST(bh.subs_med50 AS INT) AS SOIL_HAZARD_SCORE_2050_MED_EMMISIONS,
        CAST(bh.subs_med80 AS INT) AS SOIL_HAZARD_SCORE_2080_MED_EMMISIONS,
        CAST(bh.subs_hi20 AS INT) AS SOIL_HAZARD_SCORE_2020_HIGH_EMMISIONS,
        CAST(bh.subs_hi50 AS INT) AS SOIL_HAZARD_SCORE_2050_HIGH_EMMISIONS,
        CAST(bh.subs_hi80 AS INT) AS SOIL_HAZARD_SCORE_2080_HIGH_EMMISIONS,
        -- Helper for disambiguation of duplicates:
        to_date(
          coalesce(
            regexp_extract(
              bh._METADATA.file_name,
              '[1-9][0-9]{{3}}\.[0-9]{{2}}\.[0-9]{{2}}',
              0
            ),
            '1900.01.01'
          ),
          'yyyy.MM.dd'
        ) AS MDP_FILE_NAME_DATE,

        CAST(bh.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        bh.MDP_LOAD_DATETIME AS MDP_LOAD_DATETIME
    FROM
        {env_var}_catalog.bronze_hometrack_int.ground bh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UPRN", "MDP_FILE_NAME_DATE")

    # Add ROW_IS_CURRENT
    df = dlf.get_row_number(df, windowSpec)

    return df
