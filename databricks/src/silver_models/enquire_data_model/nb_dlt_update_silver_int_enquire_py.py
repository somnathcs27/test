# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window
import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_business_area", 
    comment="the business area within the organisation"
)
@dlt.expect("BUSINESS_AREA_ID is not null", "BUSINESS_AREA_ID IS NOT NULL")
def silver_enquire_business_area():

    df = spark.sql(
        f"""
    SELECT  
    CAST(BUSINESS_AREA_ID AS BIGINT) AS BUSINESS_AREA_ID,
    BA_NAME AS BUSINESS_AREA_NAME,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.business_area
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("BUSINESS_AREA_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_event",
    comment = "logs of events that occur on the enquire system, can be related to complaints or subject access requests"
)
@dlt.expect("EVENT_ID is not null", "EVENT_ID IS NOT NULL")
def silver_enquire_event():

    df = spark.sql(f"""
    SELECT
    CAST(EVENT_ID AS BIGINT) AS EVENT_ID,
    CAST(EVENT_TYPE_ID AS BIGINT) AS EVENT_TYPE_ID,
    CAST(COMPLAINT_ID AS BIGINT) AS COMPLAINT_ID,
    START_DATE AS EVENT_START_DATE,
    END_DATE AS EVENT_END_DATE,
    CREATED_BY AS EVENT_CREATED_BY,
    CREATED_DATE AS EVENT_CREATED_DATE,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.event
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("EVENT_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_event_type",
    comment = "types of events that occur on the enquire system, can be related to complaints or subject access requests"
)
@dlt.expect("EVENT_TYPE_ID is not null", "EVENT_TYPE_ID IS NOT NULL")
def silver_enquire_event_type():

    df = spark.sql(f"""
    SELECT  
    CAST(EVENT_TYPE_ID AS BIGINT) AS EVENT_TYPE_ID,
    EVENT_DESCRIPTION AS EVENT_TYPE_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.event_type
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("EVENT_TYPE_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_root_cause",
    comment = "the root cause that is linked to the complaint"
)
@dlt.expect("ROOT_CAUSE_ID is not null", "ROOT_CAUSE_ID IS NOT NULL")
def silver_enquire_std_root_cause():

    df = spark.sql(f"""
    SELECT
    CAST(STD_ROOT_CAUSE_ID AS BIGINT) AS ROOT_CAUSE_ID,
    DESCRIPTION AS ROOT_CAUSE_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_root_cause
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("ROOT_CAUSE_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_bus_area",
    comment = "the business area that is linked to the complaint"
)
@dlt.expect("COMPLAINT_BUSINESS_AREA_ID is not null", "COMPLAINT_BUSINESS_AREA_ID IS NOT NULL")
def silver_enquire_complaint_business_area():

    df = spark.sql(f"""
    SELECT
    CAST(COMPLAINT_BUS_AREA_ID AS BIGINT) AS COMPLAINT_BUSINESS_AREA_ID,
    CAST(COMPLAINT_ID AS INT) AS COMPLAINT_ID,
    CAST(BUSINESS_AREA_ID AS INT) AS BUSINESS_AREA_ID,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.complaint_bus_area
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("COMPLAINT_BUSINESS_AREA_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_reason_for_complaint_option_1",
    comment = "reasons for complaints options 1"
)
@dlt.expect("REASON_FOR_COMPLAINT_OPTION_1_ID is not null", "REASON_FOR_COMPLAINT_OPTION_1_ID IS NOT NULL")
def silver_enquire_reason_for_compl_opt1():

    df = spark.sql(f"""
    SELECT
    CAST(STD_REASON_FOR_COMPL_OPT1_ID AS BIGINT) AS REASON_FOR_COMPLAINT_OPTION_1_ID,
    DESCRIPTION AS REASON_FOR_COMPLAINT_OPTION_1_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_reason_for_compl_opt1
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("REASON_FOR_COMPLAINT_OPTION_1_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_reason_for_complaint_option_2",
    comment = "reasons for complaints options 2"
)
@dlt.expect("REASON_FOR_COMPLAINT_OPTION_2_ID is not null", "REASON_FOR_COMPLAINT_OPTION_2_ID IS NOT NULL")
def silver_enquire_reason_for_compl_opt2():

    df = spark.sql(f"""
    SELECT
    CAST(STD_REASON_FOR_COMPL_OPT2_ID AS BIGINT) AS REASON_FOR_COMPLAINT_OPTION_2_ID,
    DESCRIPTION AS REASON_FOR_COMPLAINT_OPTION_2_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_reason_for_compl_opt2
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("REASON_FOR_COMPLAINT_OPTION_2_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_reason_for_complaint_option_3",
    comment = "reasons for complaints options 3"
)
@dlt.expect("REASON_FOR_COMPLAINT_OPTION_3_ID is not null", "REASON_FOR_COMPLAINT_OPTION_3_ID IS NOT NULL")
def silver_enquire_reason_for_compl_opt3():

    df = spark.sql(f"""
    SELECT
    CAST(STD_REASON_FOR_COMPL_OPT3_ID AS BIGINT) AS REASON_FOR_COMPLAINT_OPTION_3_ID,
    DESCRIPTION AS REASON_FOR_COMPLAINT_OPTION_3_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_reason_for_compl_opt3
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("REASON_FOR_COMPLAINT_OPTION_3_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_reason_for_complaint",
    comment = "reasons for complaint"
)
@dlt.expect("REASON_FOR_COMPLAINT_ID is not null", "REASON_FOR_COMPLAINT_ID IS NOT NULL")
def silver_enquire_reason_for_compl():

    df = spark.sql(f"""
    SELECT
    CAST(STD_REASON_FOR_COMPL_ID AS BIGINT) AS REASON_FOR_COMPLAINT_ID,
    DESCRIPTION AS REASON_FOR_COMPLAINT_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_reason_for_compl
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("REASON_FOR_COMPLAINT_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_fos_category",
    comment = "financial ombudsman service categories"
)
@dlt.expect("FOS_CATEGORY_ID is not null", "FOS_CATEGORY_ID IS NOT NULL")
def silver_enquire_std_fos_category():

    df = spark.sql(f"""
    SELECT
    CAST(STD_FOS_CATEGORY_ID AS BIGINT) AS FOS_CATEGORY_ID,
    DESCRIPTION AS FOS_CATEGORY_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_fos_category
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("FOS_CATEGORY_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_fos_outcome_category",
    comment = "financial ombudsman service outcome categories"
)
@dlt.expect("FOS_OUTCOME_CATEGORY_ID is not null", "FOS_OUTCOME_CATEGORY_ID IS NOT NULL")
def silver_enquire_std_fos_outcome_category():

    df = spark.sql(f"""
    SELECT
    CAST(STD_FOS_OUTCOME_CATEGORY_ID AS BIGINT) AS FOS_OUTCOME_CATEGORY_ID,
    DESCRIPTION AS FOS_OUTCOME_CATEGORY_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_fos_outcome_category
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("FOS_OUTCOME_CATEGORY_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_fos_resolution",
    comment = "resolution from financial ombudsman service"
)
@dlt.expect("FOS_RESOLUTION_ID is not null", "FOS_RESOLUTION_ID IS NOT NULL")
def silver_enquire_std_fos_resolution():

    df = spark.sql(f"""
    SELECT
    CAST(STD_FOS_RESOLUTION_ID AS BIGINT) AS FOS_RESOLUTION_ID,
    DESCRIPTION AS FOS_RESOLUTION_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_fos_resolution
        """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("FOS_RESOLUTION_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_fsa_actual_product_type",
    comment = "the actual fsa product type relating to the complaint"
)
@dlt.expect("FSA_ACTUAL_PRODUCT_TYPE_ID is not null", "FSA_ACTUAL_PRODUCT_TYPE_ID IS NOT NULL")
def silver_enquire_std_fsa_act_prod_type():

    df = spark.sql(f"""
    SELECT
    CAST(STD_FSA_ACT_PROD_TYPE_ID AS BIGINT) AS FSA_ACTUAL_PRODUCT_TYPE_ID,
    DESCRIPTION AS FSA_ACTUAL_PRODUCT_TYPE_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_fsa_act_prod_type
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("FSA_ACTUAL_PRODUCT_TYPE_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_fsa_generic_product_type",
    comment = "the fsa generic product type relating to the compaint"
)
@dlt.expect("FSA_GENERIC_PRODUCT_TYPE_ID is not null", "FSA_GENERIC_PRODUCT_TYPE_ID IS NOT NULL")
def silver_enquire_std_fsa_gen_prod_type():

    df = spark.sql(f"""
    SELECT
    CAST(STD_FSA_GEN_PROD_TYPE_ID AS BIGINT) AS FSA_GENERIC_PRODUCT_TYPE_ID,
    DESCRIPTION AS FSA_GENERIC_PRODUCT_TYPE_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_fsa_gen_prod_type
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("FSA_GENERIC_PRODUCT_TYPE_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_fsa_report_category",
    comment = "fsa report categories held against a complaint"
)
@dlt.expect("FSA_REPORT_CATEGORY_ID is not null", "FSA_REPORT_CATEGORY_ID IS NOT NULL")
def silver_enquire_std_fsa_rep_cat():

    df = spark.sql(f"""
    SELECT
    CAST(STD_FSA_REP_CAT_ID AS BIGINT) AS FSA_REPORT_CATEGORY_ID,
    DESCRIPTION AS FSA_REPORT_CATEGORY_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_fsa_rep_cat
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("FSA_REPORT_CATEGORY_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_fsa_report_sub_category",
    comment = "fsa report sub categories held against a complaint"
)
@dlt.expect("FSA_REPORT_SUB_CATEGORY_ID is not null", "FSA_REPORT_SUB_CATEGORY_ID IS NOT NULL")
def silver_enquire_std_fsa_rep_sub_cat():

    df = spark.sql(f"""
    SELECT
    CAST(STD_FSA_REP_CAT_ID AS INT) AS FSA_REPORT_SUB_CATEGORY_ID,
    DESCRIPTION AS FSA_REPORT_SUB_CATEGORY_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_fsa_rep_sub_cat
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("FSA_REPORT_SUB_CATEGORY_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_third_party_complainant_type",
    comment = "what the third party complainant type is i.e. customer, themselves"
)
@dlt.expect("THIRD_PARTY_COMPLAINANT_TYPE_ID is not null", "THIRD_PARTY_COMPLAINANT_TYPE_ID IS NOT NULL")
def silver_enquire_std_complnant_type_beh():

    df = spark.sql(f"""
    SELECT
    CAST(STD_COMPLNANT_TYPE_BEH_ID AS BIGINT) AS THIRD_PARTY_COMPLAINANT_TYPE_ID,
    DESCRIPTION AS THIRD_PARTY_COMPLAINANT_TYPE_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_complnant_type_beh
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("THIRD_PARTY_COMPLAINANT_TYPE_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_method_of_resolution",
    comment = "the type of resolution at the end of the complaint"
)
@dlt.expect("METHOD_OF_RESOLUTION_ID is not null", "METHOD_OF_RESOLUTION_ID IS NOT NULL")
def silver_enquire_std_how_res_branch():

    df = spark.sql(f"""
    SELECT
    CAST(STD_HOW_RES_BRANCH_ID AS BIGINT) AS METHOD_OF_RESOLUTION_ID,
    DESCRIPTION AS METHOD_OF_RESOLUTION_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_how_res_branch
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("METHOD_OF_RESOLUTION_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_non_report_complaint",
    comment = "types of non-reportable complaints i.e not society customer, third party complaint etc."
)
@dlt.expect("NON_REPORT_COMPLAINT_ID is not null", "NON_REPORT_COMPLAINT_ID IS NOT NULL")
def silver_enquire_std_non_report_comp():

    df = spark.sql(f"""
    SELECT
    CAST(STD_NON_REPORT_COMP_ID AS BIGINT) AS NON_REPORT_COMPLAINT_ID,
    DESCRIPTION AS NON_REPORT_COMPLAINT_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_non_report_comp
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("NON_REPORT_COMPLAINT_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_payment_service_type",
    comment = "the payment servce type that the complaint refers to"
)
@dlt.expect("PAYMENT_SERVICE_TYPE_ID is not null", "PAYMENT_SERVICE_TYPE_ID IS NOT NULL")
def silver_enquire_std_paym_serv_type():

    df = spark.sql(f"""
    SELECT
    CAST(STD_PAYM_SERV_TYPE_ID AS BIGINT) AS PAYMENT_SERVICE_TYPE_ID,
    DESCRIPTION AS PAYMENT_SERVICE_TYPE_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_paym_serv_type
    """)
 
    # Define the window specification
    windowSpec = dlf.get_window_spec("PAYMENT_SERVICE_TYPE_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_received_method",
    comment = "the method the received was received i.e. email, phone etc."
)
@dlt.expect("COMPLAINT_RECEIVED_METHOD_ID is not null", "COMPLAINT_RECEIVED_METHOD_ID IS NOT NULL")
def silver_enquire_std_complt_received_meth():

    df = spark.sql(f"""
    SELECT
    CAST(STD_COMPLT_RECEIVED_METH_ID AS BIGINT) AS COMPLAINT_RECEIVED_METHOD_ID,
    DESCRIPTION AS COMPLAINT_RECEIVED_METHOD_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_complt_received_meth
    """)
 
    # Define the window specification
    windowSpec = dlf.get_window_spec("COMPLAINT_RECEIVED_METHOD_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_complainant_type",
    comment = "the type of person complaining i.e. customer, broker, third party etc."
)
@dlt.expect("COMPLAINANT_TYPE_ID is not null", "COMPLAINANT_TYPE_ID IS NOT NULL")
def silver_enquire_std_complainant_type():

    df = spark.sql(f"""
    SELECT
    CAST(STD_COMPLAINANT_TYPE_ID AS BIGINT) AS COMPLAINANT_TYPE_ID,
    DESCRIPTION AS COMPLAINANT_TYPE_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_complainant_type
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("COMPLAINANT_TYPE_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_justified_flag",
    comment = "whether the complaint was justified or not"
)
@dlt.expect("JUSTIFIED_FLAG_ID is not null", "JUSTIFIED_FLAG_ID IS NOT NULL")
def silver_enquire_std_justified_flag():

    df = spark.sql(f"""
    SELECT
    CAST(STD_JUSTIFIED_FLAG_ID AS BIGINT) AS JUSTIFIED_FLAG_ID,
    DESCRIPTION AS JUSTIFIED_FLAG_DECRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_justified_flag
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("JUSTIFIED_FLAG_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_type",
    comment = "the type of complaint made against the society"
)
@dlt.expect("COMPLAINT_TYPE_ID is not null", "COMPLAINT_TYPE_ID IS NOT NULL")
def silver_enquire_std_complaint_type():

    df = spark.sql(f"""
    SELECT
    CAST(STD_COMPLAINT_TYPE_ID AS BIGINT) AS COMPLAINT_TYPE_ID,
    DESCRIPTION AS COMPLAINT_TYPE_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_complaint_type
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("COMPLAINT_TYPE_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_status",
    comment = "the status of the complaint and whether it is active, closed or cancelled"
)
@dlt.expect("COMPLAINT_STATUS_ID is not null", "COMPLAINT_STATUS_ID IS NOT NULL")
def silver_enquire_std_complaint_status():

    df = spark.sql(f"""
    SELECT
    CAST(STD_COMPLAINT_STATUS_ID AS BIGINT) AS COMPLAINT_STATUS_ID,
    DESCRIPTION AS COMPLAINT_STATUS_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_complaint_status
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("COMPLAINT_STATUS_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_cost", 
    comment="costs associated with the complaint"
)
@dlt.expect("COMPLAINT_COST_ID is not null", "COMPLAINT_COST_ID IS NOT NULL")
def silver_enquire_complaint_cost():

    df = spark.sql(
        f"""
    SELECT  
    CAST(COMPLAINT_COST_ID AS BIGINT) AS COMPLAINT_COST_ID,
    CAST(COMPLAINT_ID AS INT) AS COMPLAINT_ID,
    CAST(CATEGORY_CODE AS INT) AS COST_CATEGORY_CODE,
    CAST(CURRENCY AS DECIMAL(38,2)) AS COST_CURRENCY,
    CAST(AMOUNT AS DECIMAL(38,2)) AS COST_AMOUNT,
    DATE_COST_PAID AS COST_PAID_DATE,
    GL_ACCOUNT,
    CAST(DATE_CHEQUE_ISSUED AS DATE) AS DATE_CHEQUE_ISSUED,
    CHEQUE_NUMBER,
    FOS_INVOICE_REF_NUMBER AS FOS_INVOICE_REFERENCE_NUMBER,
    CAST(FOS_INVOICE_DATE AS DATE) AS FOS_INVOICE_DATE,
    START_DATE AS COST_START_DATE,
    END_DATE AS COST_END_DATE,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.complaint_cost
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("COMPLAINT_COST_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_consumer_duty_outcome", 
    comment="the types of consumer duty outcomes held against a complaint i.e. customer support, price & value etc."
)
@dlt.expect("CONSUMER_DUTY_OUTCOME_ID is not null", "CONSUMER_DUTY_OUTCOME_ID IS NOT NULL")
def silver_enquire_std_cons_duty_outc():

    df = spark.sql(
        f"""
    SELECT  
    CAST(STD_CONS_DUTY_OUTC_ID AS BIGINT) AS CONSUMER_DUTY_OUTCOME_ID,
    DESCRIPTION AS CONSUMER_DUTY_OUTCOME_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_cons_duty_outc
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("CONSUMER_DUTY_OUTCOME_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_consumer_duty_sub_outcome", 
    comment="the types of consumer duty sub-outcomes held against a complaint"
)
@dlt.expect("CONSUMER_DUTY_SUB_OUTCOME_ID is not null", "CONSUMER_DUTY_SUB_OUTCOME_ID IS NOT NULL")
def silver_enquire_std_cons_duty_subo():

    df = spark.sql(
        f"""
    SELECT  
    CAST(STD_CONS_DUTY_SUBO_ID AS BIGINT) AS CONSUMER_DUTY_SUB_OUTCOME_ID,
    DESCRIPTION AS CONSUMER_DUTY_SUB_OUTCOME_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_cons_duty_subo
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("CONSUMER_DUTY_SUB_OUTCOME_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_cost_category", 
    comment="the types of cost categories held i.e. compensation, waived fees, refund of premiums etc."
)
@dlt.expect("COST_CATEGORY_ID is not null", "COST_CATEGORY_ID IS NOT NULL")
def silver_enquire_std_cost_category():

    df = spark.sql(
        f"""
    SELECT  
    CAST(STD_COST_CATEGORY_ID AS BIGINT) AS COST_CATEGORY_ID,
    DESCRIPTION AS COST_CATEGORY_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_cost_category
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("COST_CATEGORY_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_root_cause_analysis", 
    comment="the types of root cause analysis held against a complaint i.e. system, people/colleague error, process etc."
)
@dlt.expect("ROOT_CAUSE_ANALYSIS_ID is not null", "ROOT_CAUSE_ANALYSIS_ID IS NOT NULL")
def silver_enquire_std_rc_analysis():

    df = spark.sql(
        f"""
    SELECT  
    CAST(STD_RC_ANALYSIS_ID AS BIGINT) AS ROOT_CAUSE_ANALYSIS_ID,
    DESCRIPTION AS ROOT_CAUSE_ANALYSIS_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_rc_analysis
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ROOT_CAUSE_ANALYSIS_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_root_cause_analysis_category", 
    comment="the types of root cause analysis category held against a complaint i.e. Avaya, BTL, CHAPS etc."
)
@dlt.expect("ROOT_CAUSE_ANALYSIS_CATEGORY_ID is not null", "ROOT_CAUSE_ANALYSIS_CATEGORY_ID IS NOT NULL")
def silver_enquire_std_rc_analysis_cat():

    df = spark.sql(
        f"""
    SELECT  
    CAST(STD_RC_ANALYSIS_CAT_ID AS BIGINT) AS ROOT_CAUSE_ANALYSIS_CATEGORY_ID,
    DESCRIPTION AS ROOT_CAUSE_ANALYSIS_CATEGORY_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_rc_analysis_cat
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ROOT_CAUSE_ANALYSIS_CATEGORY_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_root_cause_channel", 
    comment="the types of root cause channels held against a complaint i.e. online, phone, email etc."
)
@dlt.expect("ROOT_CAUSE_CHANNEL_ID is not null", "ROOT_CAUSE_CHANNEL_ID IS NOT NULL")
def silver_enquire_std_rc_channel():

    df = spark.sql(
        f"""
    SELECT  
    CAST(STD_RC_CHANNEL_ID AS BIGINT) AS ROOT_CAUSE_CHANNEL_ID,
    DESCRIPTION AS ROOT_CAUSE_CHANNEL_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_rc_channel
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ROOT_CAUSE_CHANNEL_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_root_cause_journey", 
    comment="the types of root cause journeys held against a complaint i.e. mortgage application, savings account closure, isa transfer in etc."
)
@dlt.expect("ROOT_CAUSE_JOURNEY_ID is not null", "ROOT_CAUSE_JOURNEY_ID IS NOT NULL")
def silver_enquire_std_rc_journey():

    df = spark.sql(
        f"""
    SELECT  
    CAST(STD_RC_JOURNEY_ID AS BIGINT) AS ROOT_CAUSE_JOURNEY_ID,
    DESCRIPTION AS ROOT_CAUSE_JOURNEY_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_rc_journey
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ROOT_CAUSE_JOURNEY_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="enquire_complaint_root_cause_area", 
    comment="the types of root cause areas of a complaint i.e. savings, mortgages, insurance etc."
)
@dlt.expect("ROOT_CAUSE_AREA_ID is not null", "ROOT_CAUSE_AREA_ID IS NOT NULL")
def silver_enquire_std_rc_area():

    df = spark.sql(
        f"""
    SELECT  
    CAST(STD_RC_AREA_ID AS BIGINT) AS ROOT_CAUSE_AREA_ID,
    DESCRIPTION AS ROOT_CAUSE_AREA_DESCRIPTION,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.std_rc_area
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ROOT_CAUSE_AREA_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "enquire_complaint_account",
    comment = "the customer account that is linked to the complaint"
)
@dlt.expect("COMPLAINT_ACCOUNT_ID is not null", "COMPLAINT_ACCOUNT_ID IS NOT NULL")
def silver_enquire_complaint_account():

    df = spark.sql(f"""
    SELECT  
    CAST(COMPLAINT_ACCOUNT_ID AS BIGINT) AS COMPLAINT_ACCOUNT_ID,
    CAST(COMPLAINT_ID AS INT) AS COMPLAINT_ID,
    ACCOUNT_NO AS ACCOUNT_NUMBER,
    PRIMARY_ACCOUNT_IND AS PRIMARY_ACCOUNT_INDICATOR,
    CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
    CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
    __START_AT AS ROW_START_DATETIME,
    __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_enquire_int.complaint_account
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("COMPLAINT_ACCOUNT_ID", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df
