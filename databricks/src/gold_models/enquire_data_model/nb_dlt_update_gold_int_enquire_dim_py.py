# Databricks notebook source


# COMMAND ----------

import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

import mdp_databricks_common.gold_layer_functions as dlf
 
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "dim_complaint_reason",
    comment = "reasons for complaint"
)
def gold_dim_complaint_reason():
    # Execute SQL query to select and transform data
    query = f"""
    WITH CTE_DISTINCT_OPTIONS AS (
    SELECT DISTINCT
        c.COMPLAINT_REASON,
        op1.REASON_FOR_COMPLAINT_OPTION_1_ID,
        op2.REASON_FOR_COMPLAINT_OPTION_2_ID,
        op3.REASON_FOR_COMPLAINT_OPTION_3_ID,
        op1.REASON_FOR_COMPLAINT_OPTION_1_DESCRIPTION,
        op2.REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION,
        op3.REASON_FOR_COMPLAINT_OPTION_3_DESCRIPTION
    FROM {env_var}_catalog.silver_con.enquire_complaint c
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_reason_for_complaint_option_1 op1
        ON c.REASON_FOR_COMPLAINT_OPTION_1 = op1.REASON_FOR_COMPLAINT_OPTION_1_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_reason_for_complaint_option_2 op2
        ON c.REASON_FOR_COMPLAINT_OPTION_2 = op2.REASON_FOR_COMPLAINT_OPTION_2_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_reason_for_complaint_option_3 op3
        ON c.REASON_FOR_COMPLAINT_OPTION_3 = op3.REASON_FOR_COMPLAINT_OPTION_3_ID
    )

    SELECT
    xxhash64(CONCAT(
        COALESCE(ops.COMPLAINT_REASON, 'NULL'),
        COALESCE(CAST(ops.REASON_FOR_COMPLAINT_OPTION_1_ID AS STRING), 'NULL'),
        COALESCE(CAST(ops.REASON_FOR_COMPLAINT_OPTION_2_ID AS STRING), 'NULL'),
        COALESCE(CAST(ops.REASON_FOR_COMPLAINT_OPTION_3_ID AS STRING), 'NULL')
    )) AS BK_COMPLAINT_REASON,
    ops.COMPLAINT_REASON AS COMPLAINT_REASON_DESCRIPTION,
    ops.REASON_FOR_COMPLAINT_OPTION_1_DESCRIPTION,
    ops.REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION,
    ops.REASON_FOR_COMPLAINT_OPTION_3_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    1 AS ROW_IS_CURRENT
    FROM CTE_DISTINCT_OPTIONS ops
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT_REASON", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT_REASON",
    "BK_COMPLAINT_REASON",
    "COMPLAINT_REASON_DESCRIPTION",
    "REASON_FOR_COMPLAINT_OPTION_1_DESCRIPTION",
    "REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION",
    "REASON_FOR_COMPLAINT_OPTION_3_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_method_of_resolution",
    comment = "method of resolution relating to the complaint" #perhaps amend
)
def gold_dim_complaint_how_res_branch():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    METHOD_OF_RESOLUTION_ID AS BK_METHOD_OF_RESOLUTION,
    METHOD_OF_RESOLUTION_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_method_of_resolution
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_METHOD_OF_RESOLUTION", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_METHOD_OF_RESOLUTION",
    "BK_METHOD_OF_RESOLUTION",
    "METHOD_OF_RESOLUTION_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_payment_service_type",
    comment = "payment service type in regards to the complaint"
)
def gold_dim_complaint_payment_service_type():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    PAYMENT_SERVICE_TYPE_ID AS BK_PAYMENT_SERVICE_TYPE,
    PAYMENT_SERVICE_TYPE_DESCRIPTION,
    "ENQUIRE" AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_payment_service_type
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_PAYMENT_SERVICE_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_PAYMENT_SERVICE_TYPE",
    "BK_PAYMENT_SERVICE_TYPE",
    "PAYMENT_SERVICE_TYPE_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_complaint_received_method",
    comment = "The method which the complaint was received"
)
def gold_dim_complaint_std_complt_received_meth():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    COMPLAINT_RECEIVED_METHOD_ID AS BK_COMPLAINT_RECEIVED_METHOD,
    COMPLAINT_RECEIVED_METHOD_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_received_method
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT_RECEIVED_METHOD", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT_RECEIVED_METHOD",
    "BK_COMPLAINT_RECEIVED_METHOD",
    "COMPLAINT_RECEIVED_METHOD_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_justified",
    comment = "If the complaint was justified" #perhaps amend
)
def gold_dim_justified_flag():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    JUSTIFIED_FLAG_ID AS BK_JUSTIFIED,
    JUSTIFIED_FLAG_DECRIPTION AS JUSTIFIED_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_justified_flag
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_JUSTIFIED", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_JUSTIFIED",
    "BK_JUSTIFIED",
    "JUSTIFIED_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_complaint_type",
    comment = "THe type of complaint received" #perhaps amend
)
def gold_dim_complaint_std_complaint_type():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    COMPLAINT_TYPE_ID AS BK_COMPLAINT_TYPE,
    COMPLAINT_TYPE_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_type
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT_TYPE",
    "BK_COMPLAINT_TYPE",
    "COMPLAINT_TYPE_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_complaint_status",
    comment = "The status of the complaint" #perhaps amend
)
def gold_dim_complaint_status():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    COMPLAINT_STATUS_ID AS BK_COMPLAINT_STATUS,
    COMPLAINT_STATUS_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_status
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT_STATUS", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT_STATUS",
    "BK_COMPLAINT_STATUS",
    "COMPLAINT_STATUS_DESCRIPTION",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_complaint_complainant_type",
    comment = "The type of complainant" #perhaps amend
)
def gold_dim_complaint_complainant_type():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    COMPLAINANT_TYPE_ID AS BK_COMPLAINANT_TYPE,
    COMPLAINANT_TYPE_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_complainant_type
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINANT_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINANT_TYPE",
    "BK_COMPLAINANT_TYPE",
    "COMPLAINANT_TYPE_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_non_reportable",
    comment = "" #perhaps amend
)
def gold_dim_complaint_non_report_complaint():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    NON_REPORT_COMPLAINT_ID AS BK_NON_REPORTABLE,
    NON_REPORT_COMPLAINT_DESCRIPTION AS NON_REPORTABLE_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_non_report_complaint
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_NON_REPORTABLE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_NON_REPORTABLE",
    "BK_NON_REPORTABLE",
    "NON_REPORTABLE_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
  name = "dim_complaint_event_type",
  comment = "the type of event that has taken place against the complaint"
)
def gold_dim_complaint_event_type ():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    EVENT_TYPE_ID AS BK_COMPLAINT_EVENT_TYPE,
    EVENT_TYPE_DESCRIPTION AS EVENT_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_event_type
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT_EVENT_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT_EVENT_TYPE",
    "BK_COMPLAINT_EVENT_TYPE",
    "EVENT_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_fos_category_resolution",
    comment = "resolution from financial ombudsman service"
)
def gold_dim_complaint_std_fos_resolution():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    FOS_RESOLUTION_ID AS BK_FOS_CATEGORY_RESOLUTION,
    FOS_RESOLUTION_DESCRIPTION AS FOS_OUTCOME_CATEGORY_RESOLUTION,
    'ENQUIRE' AS SOURCE_SYSTEM, 
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_fos_resolution
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_FOS_CATEGORY_RESOLUTION", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_FOS_CATEGORY_RESOLUTION",
    "BK_FOS_CATEGORY_RESOLUTION",
    "FOS_OUTCOME_CATEGORY_RESOLUTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_cost_category",
    comment = "cost categories relating to the complaint"
)
def gold_dim_complaint_cost_category():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    COST_CATEGORY_ID AS BK_COST_CATEGORY,
    COST_CATEGORY_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM, 
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_int.enquire_complaint_cost_category
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COST_CATEGORY", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COST_CATEGORY",
    "BK_COST_CATEGORY",
    "COST_CATEGORY_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_root_cause_analysis",
    comment = "root cause analysis of the complaint"
)
def gold_dim_complaint_root_cause_analysis():
    # Execute SQL query to select and transform data
    query = f"""
WITH CTE_DISTINCT_OPTIONS AS (
    SELECT DISTINCT
        crc.ROOT_CAUSE_ID,
        crc.ROOT_CAUSE_DESCRIPTION,
        rca.ROOT_CAUSE_ANALYSIS_ID,
        rca.ROOT_CAUSE_ANALYSIS_DESCRIPTION,
        rtac.ROOT_CAUSE_ANALYSIS_CATEGORY_ID,
        rtac.ROOT_CAUSE_ANALYSIS_CATEGORY_DESCRIPTION,
        rta.ROOT_CAUSE_AREA_ID,
        rta.ROOT_CAUSE_AREA_DESCRIPTION,
        rcc.ROOT_CAUSE_CHANNEL_ID,
        rcc.ROOT_CAUSE_CHANNEL_DESCRIPTION,
        rcj.ROOT_CAUSE_JOURNEY_ID,
        rcj.ROOT_CAUSE_JOURNEY_DESCRIPTION
    FROM {env_var}_catalog.silver_con.enquire_complaint c
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_root_cause crc
        ON c.ROOT_CAUSE = crc.ROOT_CAUSE_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_root_cause_analysis rca
        ON c.ROOT_CAUSE_ANALYSIS_ID = rca.ROOT_CAUSE_ANALYSIS_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_root_cause_analysis_category rtac
        ON c.ROOT_CAUSE_ANALYSIS_CATEGORY_ID = rtac.ROOT_CAUSE_ANALYSIS_CATEGORY_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_root_cause_area rta
        ON c.ROOT_CAUSE_AREA_ID = rta.ROOT_CAUSE_AREA_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_root_cause_channel rcc
        ON c.ROOT_CAUSE_CHANNEL_ID = rcc.ROOT_CAUSE_CHANNEL_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_root_cause_journey rcj
        ON c.ROOT_CAUSE_JOURNEY_ID = rcj.ROOT_CAUSE_JOURNEY_ID
)

SELECT
xxhash64(CONCAT(
    COALESCE(CAST(ops.ROOT_CAUSE_ID AS STRING), 'NULL'),
    COALESCE(CAST(ops.ROOT_CAUSE_ANALYSIS_ID AS STRING), 'NULL'),
    COALESCE(CAST(ops.ROOT_CAUSE_ANALYSIS_CATEGORY_ID AS STRING), 'NULL'),
    COALESCE(CAST(ops.ROOT_CAUSE_AREA_ID AS STRING), 'NULL'),
    COALESCE(CAST(ops.ROOT_CAUSE_CHANNEL_ID AS STRING), 'NULL'),
    COALESCE(CAST(ops.ROOT_CAUSE_JOURNEY_ID AS STRING), 'NULL')
)) AS BK_ROOT_CAUSE_ANALYSIS,
ops.ROOT_CAUSE_DESCRIPTION,
ops.ROOT_CAUSE_ANALYSIS_DESCRIPTION,
ops.ROOT_CAUSE_ANALYSIS_CATEGORY_DESCRIPTION,
ops.ROOT_CAUSE_AREA_DESCRIPTION,
ops.ROOT_CAUSE_CHANNEL_DESCRIPTION,
ops.ROOT_CAUSE_JOURNEY_DESCRIPTION,
'ENQUIRE' AS SOURCE_SYSTEM,
1 AS ROW_IS_CURRENT
FROM CTE_DISTINCT_OPTIONS ops
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_ROOT_CAUSE_ANALYSIS", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_ROOT_CAUSE_ANALYSIS",
    "BK_ROOT_CAUSE_ANALYSIS",
    "ROOT_CAUSE_DESCRIPTION",
    "ROOT_CAUSE_ANALYSIS_DESCRIPTION",
    "ROOT_CAUSE_ANALYSIS_CATEGORY_DESCRIPTION",
    "ROOT_CAUSE_AREA_DESCRIPTION",
    "ROOT_CAUSE_CHANNEL_DESCRIPTION",
    "ROOT_CAUSE_JOURNEY_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_FSA_product_type",
    comment = "FSA product type"
)
def gold_dim_complaint_FSA_product_type():
    # Execute SQL query to select and transform data
    query = f"""
    WITH CTE_PRODUCT_TYPE AS(
    SELECT DISTINCT
    apt.FSA_ACTUAL_PRODUCT_TYPE_ID,
    apt.FSA_ACTUAL_PRODUCT_TYPE_DESCRIPTION,
    gpt.FSA_GENERIC_PRODUCT_TYPE_ID,
    gpt.FSA_GENERIC_PRODUCT_TYPE_DESCRIPTION
    FROM {env_var}_catalog.silver_con.enquire_complaint c  
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_fsa_actual_product_type apt
    ON c.FSA_ACTUAL_PRODUCT_TYPE = apt.FSA_ACTUAL_PRODUCT_TYPE_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_fsa_generic_product_type gpt
    ON c.FSA_PRODUCT_TYPE = gpt.FSA_GENERIC_PRODUCT_TYPE_ID
    )

    SELECT
    xxhash64(CONCAT(
        COALESCE(CAST(FSA_ACTUAL_PRODUCT_TYPE_ID AS STRING), 'NULL'),
        COALESCE(CAST(FSA_GENERIC_PRODUCT_TYPE_ID AS STRING), 'NULL')
    )) AS BK_FSA_PRODUCT_TYPE,
    FSA_ACTUAL_PRODUCT_TYPE_DESCRIPTION,
    FSA_GENERIC_PRODUCT_TYPE_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    1 AS ROW_IS_CURRENT
    FROM CTE_PRODUCT_TYPE
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_FSA_PRODUCT_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_FSA_PRODUCT_TYPE",
    "BK_FSA_PRODUCT_TYPE",
    "FSA_ACTUAL_PRODUCT_TYPE_DESCRIPTION",
    "FSA_GENERIC_PRODUCT_TYPE_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_FSA_reporting_category",
    comment = "FSA reporting category"
)
def gold_dim_complaint_FSA_reporting_category():
    # Execute SQL query to select and transform data
    query = f"""
    WITH CTE_FSA_REPORTING AS(
    SELECT DISTINCT
    rc.FSA_REPORT_CATEGORY_ID,
    rc.FSA_REPORT_CATEGORY_DESCRIPTION,
    rsc.FSA_REPORT_SUB_CATEGORY_ID,
    rsc.FSA_REPORT_SUB_CATEGORY_DESCRIPTION
    FROM {env_var}_catalog.silver_con.enquire_complaint c  
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_fsa_report_category rc
    ON c.FSA_REPORTING_CATEGORY = rc.FSA_REPORT_CATEGORY_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_fsa_report_sub_category rsc
    ON c.FSA_REPORTING_SUB_CATEGORY = rsc.FSA_REPORT_SUB_CATEGORY_ID
    )

    SELECT
    xxhash64(CONCAT(
        COALESCE(CAST(FSA_REPORT_CATEGORY_ID AS STRING), 'NULL'),
        COALESCE(CAST(FSA_REPORT_SUB_CATEGORY_ID AS STRING), 'NULL')
    )) AS BK_FSA_REPORTING_CATEGORY,
    FSA_REPORT_CATEGORY_DESCRIPTION,
    FSA_REPORT_SUB_CATEGORY_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    1 AS ROW_IS_CURRENT
    FROM CTE_FSA_REPORTING
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_FSA_REPORTING_CATEGORY", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_FSA_REPORTING_CATEGORY",
    "BK_FSA_REPORTING_CATEGORY",
    "FSA_REPORT_CATEGORY_DESCRIPTION",
    "FSA_REPORT_SUB_CATEGORY_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_consumer_duty_outcome",
    comment = "Consumer Duty Outcome"
)
def gold_dim_consumer_duty_outcome():
    # Execute SQL query to select and transform data
    query = f"""
    WITH CTE_CONSUMER_DUTY AS(
    SELECT DISTINCT
    cdo.CONSUMER_DUTY_OUTCOME_ID,
    cdo.CONSUMER_DUTY_OUTCOME_DESCRIPTION,
    cdso.CONSUMER_DUTY_SUB_OUTCOME_ID,
    cdso.CONSUMER_DUTY_SUB_OUTCOME_DESCRIPTION
    FROM {env_var}_catalog.silver_con.enquire_complaint c  
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_consumer_duty_outcome cdo
    ON c.CONSUMER_DUTY_OUTCOME_ID = cdo.CONSUMER_DUTY_OUTCOME_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_consumer_duty_sub_outcome cdso
    ON c.CONSUMER_DUTY_SUB_OUTCOME_ID = cdso.CONSUMER_DUTY_SUB_OUTCOME_ID
    )

    SELECT
    xxhash64(CONCAT(
        COALESCE(CAST(CONSUMER_DUTY_OUTCOME_ID AS STRING), 'NULL'),
        COALESCE(CAST(CONSUMER_DUTY_SUB_OUTCOME_ID AS STRING), 'NULL')
    )) AS BK_CONSUMER_DUTY_OUTCOME,
    CONSUMER_DUTY_OUTCOME_DESCRIPTION,
    CONSUMER_DUTY_SUB_OUTCOME_DESCRIPTION,
    'ENQUIRE' AS SOURCE_SYSTEM,
    1 AS ROW_IS_CURRENT
    FROM CTE_CONSUMER_DUTY
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_CONSUMER_DUTY_OUTCOME", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_CONSUMER_DUTY_OUTCOME",
    "BK_CONSUMER_DUTY_OUTCOME",
    "CONSUMER_DUTY_OUTCOME_DESCRIPTION",
    "CONSUMER_DUTY_SUB_OUTCOME_DESCRIPTION",
    "SOURCE_SYSTEM",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
    name = "dim_complaint_flag",
    comment = "flag columns relating to the complai"
)
def gold_dim_consumer_duty_outcome():
    # Execute SQL query to select and transform data
    query = f"""
    WITH CTE_EVENT_RECEIVED_DATE AS (
    SELECT
        COMPLAINT_ID,
        CAST(MIN(EVENT_START_DATE) AS DATE) AS COMPLAINT_RECEIVED_DATE
    FROM {env_var}_catalog.silver_int.enquire_event
    WHERE EVENT_TYPE_ID = 103
    GROUP BY COMPLAINT_ID
    ),
    CTE_EVENT_CLOSED_DATE AS (
    SELECT
        COMPLAINT_ID,
        CAST(MIN(EVENT_START_DATE) AS DATE) AS COMPLAINT_CLOSED_DATE
    FROM {env_var}_catalog.silver_int.enquire_event
    WHERE EVENT_TYPE_ID = 190
    GROUP BY COMPLAINT_ID
    ),

    WORKING_DAYS_BETWEEN_RECEIVED_AND_CLOSED AS (
    SELECT
        c.COMPLAINT_ID,
        COUNT(*) AS WORKING_DAYS_BETWEEN_RECEIVED_AND_CLOSED
    FROM {env_var}_catalog.silver_con.enquire_complaint c
    LEFT JOIN CTE_EVENT_RECEIVED_DATE ref
    ON c.COMPLAINT_ID = ref.COMPLAINT_ID
    LEFT JOIN CTE_EVENT_CLOSED_DATE res 
    ON c.COMPLAINT_ID = res.COMPLAINT_ID
    JOIN {env_var}_catalog.gold_int.dim_calendar cal
    ON ref.COMPLAINT_RECEIVED_DATE IS NOT NULL
    AND res.COMPLAINT_CLOSED_DATE IS NOT NULL
    AND cal.PK_DATE BETWEEN ref.COMPLAINT_RECEIVED_DATE AND res.COMPLAINT_CLOSED_DATE
    AND cal.FIVE_WORK_DAY_FLAG = 1
    GROUP BY c.COMPLAINT_ID
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
    c.COMPLAINT_ID AS BK_COMPLAINT,
    COALESCE(c.LBS_CUSTOMER_INDICATOR, 'U') AS IS_LBS_CUSTOMER,
    COALESCE(c.FSA_INDICATOR, 'U') AS HAS_BEEN_REFERRED_TO_FSA,
    COALESCE(c.FOS_INDICATOR, 'U') AS HAS_BEEN_REFERRED_TO_FOS,
    COALESCE(c.FOS_OVERTURNED_INDICATOR, 'U') AS HAS_BEEN_OVERTURNED_BY_FOS,
    COALESCE(c.CUSTOMER_SATISFACTION_SURVEY_INDICATOR, 'U') AS HAS_CUSTOMER_SATISFACTION_SURVEY_BEEN_SENT,
    COALESCE(c.FOS_OVERTURNED_COMPLAINT_INDICATOR, 'U') AS HAS_BEEN_OVERTURNED_BY_FOS_COMPLAINCE,
    COALESCE(c.CUSTOMER_ACCEPTED_RESOLUTION, 'U') AS IS_CUSTOMER_ACCEPTED_RESOLUTION,
    COALESCE(c.AFTER_REOPEN_HAS_RESOLUTION_CHANGED, 'U') AS HAS_RESOLUTION_CHANGED_AFTER_REOPEN,
    COALESCE(c.CLAIMS_RELATED_INDICATOR, 'U') AS IS_CLAIMS_RELATED,
    COALESCE(c.PLEVIN_COMPLAINT_INDICATOR, 'U') AS IS_PLEVIN_COMPLAINT,
    COALESCE(c.COLD_ACCOUNT_NUMBER_INDICATOR, 'U') AS IS_COLD_ACCOUNT_NUMBER,
    COALESCE(c.PAYMENT_SERVICES_RELATED_INDICATOR, 'U') AS IS_PAYMENT_SERVICES_RELATED,
    CASE
        WHEN wd1.WORKING_DAYS_BETWEEN_RECEIVED_AND_CLOSED <= 3 AND src.CLOSED_BY_SRC_OR_FRL = 'FRL' THEN 'Y'
        ELSE 'N'
    END AS IS_3_DAY_FINAL_RESPONSE_LETTER,
    CASE
        WHEN wd1.WORKING_DAYS_BETWEEN_RECEIVED_AND_CLOSED <= 3 AND src.CLOSED_BY_SRC_OR_FRL = 'FRL' AND j.JUSTIFIED_FLAG_DECRIPTION = 'Justified' THEN 'Y'
        ELSE 'N'
    END AS IS_3_DAY_FINAL_RESPONSE_LETTER_JUSTIFIED,
    CASE WHEN mor.METHOD_OF_RESOLUTION_ID = 1 THEN 'Y' ELSE 'N' END AS HAS_SPOKEN_TO_RESOLUTION,
    CASE WHEN mor.METHOD_OF_RESOLUTION_ID = 2 THEN 'Y' ELSE 'N' END AS HAS_NOT_SPOKEN_TO_RESOLUTION,
    CASE WHEN mor.METHOD_OF_RESOLUTION_ID = 4 THEN 'Y' ELSE 'N' END AS IS_UNABLE_TO_RESOLVE_RESOLUTION,
    CASE WHEN mor.METHOD_OF_RESOLUTION_ID NOT IN(1, 2, 4) THEN 'Y' ELSE 'N' END AS IS_UNKNOWN_RESOLUTION,
    CASE WHEN src.CLOSED_BY_SRC_OR_FRL = 'SRC' AND j.JUSTIFIED_FLAG_DECRIPTION = 'Justified' THEN 'Y' ELSE 'N' END AS IS_SUMMARY_RESOLUTION_COMMUNICATION_JUSTIFIED,
    -- Add IS_COMPLAINT_AGAINST_BRANCH
    -- Add WAS_COMPLAINT_LOGGED_AT_BRANCH
    CASE WHEN (c.COMPLAINT_REASON = 'website' OR op2.REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION IN('Online/OMNI', '(1) Online', '(2) Online')) THEN 'Y' ELSE 'N' END AS IS_WEBSITE_COMPLAINT,
    CASE WHEN cd.COMPLAINT_CLOSED_DATE IS NOT NULL THEN 'Y' ELSE 'N' END AS IS_COMPLAINT_CLOSED,
    CASE WHEN (cs.COMPLAINT_STATUS_DESCRIPTION = 'Active' AND cd.COMPLAINT_CLOSED_DATE IS NULL) THEN 'Y' ELSE 'N' END AS IS_COMPLAINT_OUTSTANDING,
    CASE WHEN (c.REASON_FOR_FURTHER_COMPLAINT IS NOT NULL AND cd.COMPLAINT_CLOSED_DATE IS NOT NULL) THEN 'Y' ELSE 'N' END AS IS_FURTHER_COMPLAINT_CLOSED,
    CASE WHEN (cs.COMPLAINT_STATUS_DESCRIPTION = 'Active' AND cd.COMPLAINT_CLOSED_DATE IS NULL AND c.REASON_FOR_FURTHER_COMPLAINT IS NOT NULL) THEN 'Y' ELSE 'N' END AS IS_FURTHER_COMPLAINT_OUTSTANDING,
    CASE WHEN(COMPLAINT_STATUS_DESCRIPTION <> 'Active' AND COMPLAINT_STATUS_DESCRIPTION <> 'Closed And Resolved') THEN 'Y' ELSE 'N' END AS IS_COMPLAINT_UNASSIGNED,
    CASE WHEN(COMPLAINT_STATUS_DESCRIPTION <> 'Active' AND COMPLAINT_STATUS_DESCRIPTION <> 'Closed And Resolved' AND c.REASON_FOR_FURTHER_COMPLAINT IS NOT NULL) THEN 'Y' ELSE 'N' END AS IS_FURTHER_COMPLAINT_UNASSIGNED,
    'ENQUIRE' AS SOURCE_SYSTEM,
    c.ROW_START_DATETIME,
    c.ROW_END_DATETIME,
    c.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.enquire_complaint c
    LEFT JOIN WORKING_DAYS_BETWEEN_RECEIVED_AND_CLOSED wd1
    ON c.COMPLAINT_ID = wd1.COMPLAINT_ID
    LEFT JOIN CTE_SRC_FRL src
    ON c.COMPLAINT_ID = src.COMPLAINT_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_justified_flag j
    ON c.Justified = j.JUSTIFIED_FLAG_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_method_of_resolution mor
    ON c.24HR_COMPLAINT_HOW_RESOLVED_CODE = mor.METHOD_OF_RESOLUTION_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_reason_for_complaint_option_2 op2
    ON c.REASON_FOR_COMPLAINT_OPTION_2 = op2.REASON_FOR_COMPLAINT_OPTION_2_ID
    LEFT JOIN CTE_EVENT_CLOSED_DATE cd 
    ON c.COMPLAINT_ID = cd.COMPLAINT_ID
    LEFT JOIN {env_var}_catalog.silver_int.enquire_complaint_status cs  
    ON c.COMPLAINT_STATUS = cs.COMPLAINT_STATUS_ID
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_COMPLAINT_FLAG", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_COMPLAINT_FLAG",
    "BK_COMPLAINT",
    "IS_LBS_CUSTOMER",
    "HAS_BEEN_REFERRED_TO_FSA",
    "HAS_BEEN_REFERRED_TO_FOS",
    "HAS_BEEN_OVERTURNED_BY_FOS",
    "HAS_CUSTOMER_SATISFACTION_SURVEY_BEEN_SENT",
    "HAS_BEEN_OVERTURNED_BY_FOS_COMPLAINCE",
    "IS_CUSTOMER_ACCEPTED_RESOLUTION",
    "HAS_RESOLUTION_CHANGED_AFTER_REOPEN",
    "IS_CLAIMS_RELATED",
    "IS_PLEVIN_COMPLAINT",
    "IS_COLD_ACCOUNT_NUMBER",
    "IS_PAYMENT_SERVICES_RELATED",
    "IS_3_DAY_FINAL_RESPONSE_LETTER",
    "IS_3_DAY_FINAL_RESPONSE_LETTER_JUSTIFIED",
    "HAS_SPOKEN_TO_RESOLUTION",
    "HAS_NOT_SPOKEN_TO_RESOLUTION",
    "IS_UNABLE_TO_RESOLVE_RESOLUTION",
    "IS_UNKNOWN_RESOLUTION",
    "IS_SUMMARY_RESOLUTION_COMMUNICATION_JUSTIFIED",
    "IS_WEBSITE_COMPLAINT",
    "IS_COMPLAINT_CLOSED",
    "IS_COMPLAINT_OUTSTANDING",
    "IS_FURTHER_COMPLAINT_CLOSED",
    "IS_FURTHER_COMPLAINT_OUTSTANDING",
    "IS_COMPLAINT_UNASSIGNED",
    "IS_FURTHER_COMPLAINT_UNASSIGNED",
    "SOURCE_SYSTEM",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df
