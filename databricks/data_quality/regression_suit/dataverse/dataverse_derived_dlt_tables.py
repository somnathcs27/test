# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id, md5, xxhash64

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')


# COMMAND ----------

@dlt.table(
    name="dim_customer_test_data",
    comment="Customer master table which has customer demographic and other information"
)
def gold_dim_customer_contact_test_data():
    query = f"""
        SELECT 
            CUSTOMER_ID AS BK_CUSTOMER,
            CUSTOMER_NUMBER,
            DOGM.LOCALIZED_LABEL AS TITLE,
            FORENAMES,
            INITIALS,
            SURNAME,
            DOM.LOCALIZED_LABEL AS GENDER,
            CAST(DATE_OF_BIRTH AS DATE) AS DATE_OF_BIRTH,
            AGE,
            NI_NUMBER,
            BUILDING_NAME,
            BUILDING_NUMBER,
            STREET_NAME,
            CITY,
            COUNTY,
            DC.COUNTRY_NAME AS COUNTRY,
            REGION,
            POSTCODE,
            EMAIL_ADDRESS,
            PCM.LOCALIZED_LABEL AS PREFERRED_CONTACT_METHOD,
            TELEPHONE_HOME,
            TELEPHONE_MOBILE,
            TELEPHONE_WORK,
            CUSTOMER_TYPE_CODE,
            CUSTOMER_START_DATE,
            CUSTOMER_END_DATE,
            DVC.LOAD_ID AS LOAD_ID,
            DVC.LOAD_DATETIME AS LOAD_DATETIME,
            DVC.ROW_START_DATETIME AS ROW_START_DATETIME,
            DVC.ROW_END_DATETIME AS ROW_END_DATETIME,
            DVC.ROW_IS_CURRENT AS ROW_IS_CURRENT 
        FROM {env_var}_catalog.data_quality.dataverse_contact_test_data DVC
    
        LEFT JOIN  {env_var}_catalog.data_quality.dataverse_option_set_metadata_test_data DOM
        ON DOM.ENTITY_NAME = 'contact' AND DOM.option_set_name = 'gendercode' AND DVC.GENDER = DOM.OPTION

        LEFT JOIN  {env_var}_catalog.data_quality.dataverse_option_set_metadata_test_data PCM
        ON PCM.ENTITY_NAME = 'contact' and PCM.option_set_name = 'preferredcontactmethodcode' AND DVC.PREFERRED_CONTACT_METHOD = PCM.OPTION

        LEFT JOIN {env_var}_catalog.data_quality.dataverse_global_option_set_metadata_test_data DOGM
        ON DOGM.ENTITY_NAME = 'contact'  AND GLOBAL_OPTION_SET_NAME = 'lbs_title' AND DOGM.OPTION = DVC.TITLE

        LEFT JOIN {env_var}_catalog.data_quality.dataverse_country_test_data DC
        ON DC.COUNTRY_ID = DVC.COUNTRY AND DC.ROW_END_DATETIME IS NULL

    """

    df = spark.sql(query)
    df = df.withColumn("PK_CUSTOMER", monotonically_increasing_id() + 1)

    df = df.select(
            "PK_CUSTOMER",
            "BK_CUSTOMER",
            "CUSTOMER_NUMBER",
            "TITLE",
            "FORENAMES",
            "INITIALS",
            "SURNAME",
            "GENDER",
            "DATE_OF_BIRTH",
            "AGE",
            "NI_NUMBER",
            "BUILDING_NAME",
            "BUILDING_NUMBER",
            "STREET_NAME",
            "CITY",
            "COUNTY",
            "COUNTRY",
            "REGION",
            "POSTCODE",
            "EMAIL_ADDRESS",
            "PREFERRED_CONTACT_METHOD",
            "TELEPHONE_HOME",
            "TELEPHONE_MOBILE",
            "TELEPHONE_WORK",
            "CUSTOMER_TYPE_CODE",
            "CUSTOMER_START_DATE",
            "CUSTOMER_END_DATE",
            "LOAD_ID",
            "LOAD_DATETIME",
            "ROW_START_DATETIME",
            "ROW_END_DATETIME",
            "ROW_IS_CURRENT"
    )

    return df
