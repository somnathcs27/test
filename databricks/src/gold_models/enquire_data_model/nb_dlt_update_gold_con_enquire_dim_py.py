# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

import mdp_databricks_common.gold_layer_functions as dlf
 
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "dim_complaint_referred_to",
    comment = "who the complaint has been referred to"
)
def gold_dim_complaint_referred_to():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    COMPLAINT_REFERRED_TO_ID AS BK_COMPLAINT_REFERRED_TO,
    COALESCE(FULL_NAME, 'Unknown') AS COMPLAINT_REFERRED_TO_FULL_NAME,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.enquire_complaint_referred_to
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT_REFERRED_TO", monotonically_increasing_id() +1)

    #insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT_REFERRED_TO",
    "BK_COMPLAINT_REFERRED_TO",
    "COMPLAINT_REFERRED_TO_FULL_NAME",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
  name = "dim_complaint",
  comment = "Table relating to the details of the complaint"
)
def gold_dim_complaint():
    # Execute SQL query to select and transform data
    query = f"""
    WITH CTE_ACCOUNT_NUMBER_AGGREGATED AS (
    SELECT
    COMPLAINT_ID,
    COUNT(COMPLAINT_ID) AS ACCOUNT_NUMBER_AGGREGATED
    FROM {env_var}_catalog.silver_int.enquire_complaint_account
    GROUP BY COMPLAINT_ID
    ),

    CTE_MIN_SRC1_END_DATE (
        SELECT
        COMPLAINT_ID,
        MIN(EVENT_START_DATE) AS SRC1_END_DATE
        FROM {env_var}_catalog.silver_int.enquire_event e
        WHERE EVENT_TYPE_ID = '10039'
        GROUP BY COMPLAINT_ID
    ),

    CTE_MIN_SRC2_END_DATE (
        SELECT
        COMPLAINT_ID,
        MIN(EVENT_START_DATE) AS SRC2_END_DATE
        FROM {env_var}_catalog.silver_int.enquire_event e
        WHERE EVENT_TYPE_ID = '10061'
        GROUP BY COMPLAINT_ID
    ),

    CTE_MIN_FRL_END_DATE (
        SELECT
        COMPLAINT_ID,
        MIN(EVENT_START_DATE) AS FRL_END_DATE
        FROM {env_var}_catalog.silver_int.enquire_event e
        WHERE EVENT_TYPE_ID = '507'
        GROUP BY COMPLAINT_ID
    ),

    CTE_SRC_FRL (

        SELECT
        src1.COMPLAINT_ID,
        CASE
            WHEN (SRC1_END_DATE IS NOT NULL OR SRC2_END_DATE IS NOT NULL) THEN 'SRC'
            WHEN FRL_END_DATE IS NOT NULL THEN 'FRL'
            ELSE 'Unknown'
        END AS CLOSED_BY_SRC_OR_FRL
        FROM CTE_MIN_SRC1_END_DATE src1
        LEFT JOIN CTE_MIN_SRC2_END_DATE src2
        ON src1.COMPLAINT_ID = src2.COMPLAINT_ID
        LEFT JOIN CTE_MIN_FRL_END_DATE frl
        ON src1.COMPLAINT_ID = frl.COMPLAINT_ID
    )

SELECT
  CAST(c.COMPLAINT_ID AS BIGINT) AS BK_COMPLAINT,
  COALESCE(c.COMPLAINT_REFERENCE, 'Unknown') AS COMPLAINT_REFERENCE,
  COALESCE(c.COMPLAINANT_NAME ,'Unknown') AS COMPLAINANT_NAME,
  COALESCE(c.ROOT_CAUSE_EXTRA_INFO, 'Unknown') AS ROOT_CAUSE_EXTRA_INFO,
  COALESCE(c.FOS_POTENTIAL_IMPROVEMENTS, 'Unknown') AS FOS_POTENTIAL_IMPROVEMENTS,
  COALESCE(c.COLD_ACCOUNT_NUMBER, 'Unknown') AS COLD_ACCOUNT_NUMBER,
  COALESCE(c.FOS_REFERENCE_NUMBER, 'Unknown') AS FOS_REFERENCE_NUMBER,
  COALESCE(c.COMPLAINT_REASON, 'Unknown') AS COMPLAINT_REASON,
  COALESCE(c.COMPLAINT_ACTION_TAKEN, 'Unknown') AS COMPLAINT_ACTION_TAKEN,
  COALESCE(c.INVESTIGATION_NOTES, 'Unknown') AS INVESTIGATION_NOTES,
  COALESCE(c.IT_INCIDENT_REFERENCE, 'Unknown') AS IT_INCIDENT_REFERENCE,
  COALESCE(c.RISK_INCIDENT_REFERENCE, 'Unknown') AS RISK_INCIDENT_REFERENCE,
  CAST(COALESCE(c.FSA_REPORTING_CATEGORY, '-1') AS INT) AS REPORTING_CATEGORY,
  IFNULL(THIRD_PARTY_COMPLAINANT_TYPE_DESCRIPTION, 'Customer') AS COMPLAINT_ON_BEHALF_OF,
  COALESCE(CASE 
    WHEN op2.REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION = 'GDPR' THEN 'Regulatory-GDPR'
    WHEN op2.REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION = 'Account Amendment' THEN 'Regulatory-Account Amendment'
    WHEN op2.REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION = 'Products' THEN 'Regulatory-Products'
    WHEN op2.REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION = 'Product Transfer' THEN 'Regulatory-Product Transfer'
    ELSE NULL
  END, 'Unknown') AS COMPLAINT_REGULATORY_REASON,
  COALESCE(c.COMPLAINT_OR_FEEDBACK_INDICATOR, 'U') AS COMPLAINT_OR_FEEDBACK_INDICATOR,
  COALESCE(frl.CLOSED_BY_SRC_OR_FRL, 'Unknown') AS CLOSED_BY_SRC_OR_FRL,
  COALESCE(ca.ACCOUNT_NUMBER, 'Unknown') AS PRIMARY_ACCOUNT_NUMBER,
  CAST(COALESCE(acc.ACCOUNT_NUMBER_AGGREGATED, '-1') AS INT) AS ACCOUNT_NUMBER_AGGREGATED,
  'ENQUIRE' AS SOURCE_SYSTEM,
  c.ROW_START_DATETIME,
  c.ROW_END_DATETIME,
  c.ROW_IS_CURRENT
FROM {env_var}_catalog.silver_con.enquire_complaint c
LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_third_party_complainant_type beh
ON c.THIRD_PARTY_COMPLAINANT_TYPE_ID = beh.THIRD_PARTY_COMPLAINANT_TYPE_ID
LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_reason_for_complaint_option_2 op2
ON c.REASON_FOR_COMPLAINT_OPTION_2 = op2.REASON_FOR_COMPLAINT_OPTION_2_ID
LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_account  as ca
ON c.COMPLAINT_ID = ca.COMPLAINT_ID
AND PRIMARY_ACCOUNT_INDICATOR = 'Y'
LEFT JOIN CTE_ACCOUNT_NUMBER_AGGREGATED acc
ON c.COMPLAINT_ID = acc.COMPLAINT_ID
AND PRIMARY_ACCOUNT_INDICATOR = 'Y'
LEFT JOIN CTE_SRC_FRL frl
ON c.COMPLAINT_ID = frl.COMPLAINT_ID
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT", monotonically_increasing_id() +1)

    #insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT",
    "BK_COMPLAINT",
    "COMPLAINT_REFERENCE",
    "COMPLAINANT_NAME",
    "ROOT_CAUSE_EXTRA_INFO",
    "FOS_POTENTIAL_IMPROVEMENTS",
    "COLD_ACCOUNT_NUMBER",
    "FOS_REFERENCE_NUMBER",
    "COMPLAINT_REASON",
    "COMPLAINT_ACTION_TAKEN",
    "INVESTIGATION_NOTES",
    "IT_INCIDENT_REFERENCE",
    "RISK_INCIDENT_REFERENCE",
    "REPORTING_CATEGORY",
    "COMPLAINT_REGULATORY_REASON",
    "PRIMARY_ACCOUNT_NUMBER",
    "ACCOUNT_NUMBER_AGGREGATED",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_complaint_third_party",
    comment = "who the complaint has been referred to"
)
def gold_dim_complaint_third_party():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    CAST(THIRD_PARTY_ID AS BIGINT) AS BK_COMPLAINT_THIRD_PARTY,
    COALESCE(THRID_PARTY_DESCRIPTION, 'Unknown') AS THRID_PARTY_DESCRIPTION,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.enquire_complaint_third_party
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT_THIRD_PARTY", monotonically_increasing_id() +1)

    #insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT_THIRD_PARTY",
    "BK_COMPLAINT_THIRD_PARTY",
    "THRID_PARTY_DESCRIPTION",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df
