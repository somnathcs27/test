# Databricks notebook source
import dlt
import pyspark.sql.functions as F
import pyspark.sql.types as T
import json
import hashlib
import mdp_databricks_common.silver_layer_functions as dlf
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, expr
from datetime import datetime

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

def str_to_datetime(value: str, try_formats: list):
    for format in try_formats:
        try:
            return datetime.strptime(value, format)
        except:
            pass
    return None

def xsi_nil(value: any):
    if isinstance(value, dict):
        return None
    elif isinstance(value, str) and value == '{"xsi:@nil":true}':
        return None
    else:
        return value



# COMMAND ----------

# MAGIC %md
# MAGIC # Base Rates

# COMMAND ----------

@dlt.table(
    name="mso_xml_base_rates", 
    comment="Data from BaseRates.xml from MSO"
)
def get_base_rates_data():
    """Unpack BaseRates.xml into a table. Grain is one row per BaseRateHistory.
    """
    df = spark.sql(f"SELECT * FROM {env_var}_catalog.bronze_mso_xml_int.base_rates")

    # Assuming your DataFrame is named `df`
    # Convert the entire root column structure into a JSON string
    df_with_json = df.select(
        F.to_json(F.col('root')).alias('root_json'),
        'MDP_LOAD_TYPE',
        'MDP_LOAD_ID',
        'MDP_LOAD_DATETIME',
        'MDP_BRONZE_LAYER_PROCESSED_DATETIME'
    )

    root_json = df_with_json.first().root_json
    base_rates_json = json.loads(root_json)

    mdp_load_id = df_with_json.first().MDP_LOAD_ID
    mdp_load_type = df_with_json.first().MDP_LOAD_TYPE
    mdp_load_datetime = df_with_json.first().MDP_LOAD_DATETIME
    base_rates_version = base_rates_json.get("BaseRatesVersion")
    base_rates_sub_version = base_rates_json.get("BaseRatesSubVersion")

    base_rate_rows = []

    for base_rate in base_rates_json.get("BaseRateTypes", {}).get("BaseRateType"):
        join_key = hashlib.md5(str(base_rate).encode()).hexdigest()
        base_rate_history_json = json.loads(base_rate.get("BaseRateHistory"))
        base_rate_history = base_rate_history_json if isinstance(base_rate_history_json, list) else [base_rate_history_json]
        
        for brh in base_rate_history:
            base_rate_rows.append(dict(
                base_rate_type_code=base_rate.get("BaseRateTypeCode"),
                base_rate_type_description=base_rate.get("BaseRateTypeDescription"),
                base_rate_type_name=base_rate.get("BaseRateTypeName"),
                is_approved=base_rate.get("IsApproved"),
                is_externally_managed=base_rate.get("IsExternallyManaged"),
                interest_rate=brh.get("InterestRate"),
                effective_from=datetime.strptime(brh.get("EffectiveFrom", "1900-01-01T00:00:00"), "%Y-%m-%dT%H:%M:%S"),
                effective_to=datetime.strptime(brh.get("EffectiveTo", "1900-01-01T00:00:00"), "%Y-%m-%dT%H:%M:%S"),
                history_is_approved=brh.get("IsApproved"),
                MDP_LOAD_ID=int(mdp_load_id),
                MDP_LOAD_TYPE=mdp_load_type,
                MDP_LOAD_DATETIME=mdp_load_datetime,
            ))

    schema = T.StructType([
        T.StructField("base_rate_type_code", T.StringType(), False),
        T.StructField("base_rate_type_description", T.StringType(), False),
        T.StructField("base_rate_type_name", T.StringType(), False),
        T.StructField("is_approved", T.BooleanType(), False),
        T.StructField("is_externally_managed", T.BooleanType(), False),
        T.StructField("interest_rate", T.DoubleType(), False),
        T.StructField("effective_from", T.TimestampType(), False),
        T.StructField("effective_to", T.TimestampType(), False),
        T.StructField("history_is_approved", T.BooleanType(), False),
        T.StructField("MDP_LOAD_ID", T.LongType(), False),
        T.StructField("MDP_LOAD_TYPE", T.StringType(), False),
        T.StructField("MDP_LOAD_DATETIME", T.TimestampType(), False),
    ])

    return spark.createDataFrame(base_rate_rows, schema)


# COMMAND ----------

# MAGIC %md
# MAGIC # Products

# COMMAND ----------

@dlt.table(
    name="mso_xml_product", 
    comment="Data from Product.xml from MSO"
)
def get_product_data():
    df = spark.sql(f"SELECT * FROM {env_var}_catalog.bronze_mso_xml_int.products")

    # Convert the entire root column structure into a JSON string
    df_with_json = df.select(
        F.to_json(F.col('root')).alias('root_json'),
        'MDP_LOAD_TYPE',
        'MDP_LOAD_ID',
        'MDP_LOAD_DATETIME',
        'MDP_BRONZE_LAYER_PROCESSED_DATETIME'
    )

    root_json = df_with_json.first().root_json
    product_json = json.loads(root_json)
    
    mdp_load_id = df_with_json.first().MDP_LOAD_ID
    mdp_load_type = df_with_json.first().MDP_LOAD_TYPE
    mdp_load_datetime = df_with_json.first().MDP_LOAD_DATETIME
    product_version = product_json.get("ProductVersion")
    product_sub_version = product_json.get("ProductSubVersion")

    product_rows = []

    for product in product_json.get("Products", {}).get("Product"):
        product_applicability_str = product.get("ProductApplicability")
        product_applicability = json.loads(product_applicability_str) if isinstance(json.loads(product_applicability_str), list) else [json.loads(product_applicability_str)]

        approved_by = product.get("ApprovedBy")
        approved_date = str_to_datetime(product.get("ApprovedDate", "1900-01-01T00:00:00.0"), ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"])
        is_approved = product.get("IsApproved")
        product_beneficial_rate_period = product.get("ProductBeneficialRatePeriod")
        product_code = product.get("ProductCode")
        product_description = product.get("ProductDescription")
        product_erc_profile_code = xsi_nil(product.get("ProductERCProfileCode"))
        product_eligibility_policy_code = xsi_nil(product.get("ProductEligibilityPolicyCode"))
        product_external_code = xsi_nil(product.get("ProductExternalCode"))
        product_feature_profile_code = xsi_nil(product.get("ProductFeatureProfileCode"))
        product_fee_profile_code = xsi_nil(product.get("ProductFeeProfileCode"))
        product_name = product.get("ProductName")
        product_rate_profile_code = xsi_nil(product.get("ProductRateProfileCode"))
        product_set = xsi_nil(product.get("ProductSet"))
        product_type = product.get("ProductType")

        for applicability in product_applicability:
            product_rows.append(dict(
                approved_by=approved_by,
                approved_date=approved_date,
                is_approved=is_approved,
                product_beneficial_rate_period=product_beneficial_rate_period,
                product_code=product_code,
                product_description=product_description,
                product_erc_profile_code=product_erc_profile_code,
                product_eligibility_policy_code=product_eligibility_policy_code,
                product_external_code=product_external_code,
                product_feature_profile_code=product_feature_profile_code,
                product_fee_profile_code=product_fee_profile_code,
                product_name=product_name,
                product_rate_profile_code=product_rate_profile_code,
                product_set=product_set,
                product_type=product_type,
                product_applicability_channel_code=applicability.get("ChannelCode"),
                product_applicability_effective_from=str_to_datetime(applicability.get("EffectiveFrom", "1900-01-01T00:00:00"), ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S"]),
                product_applicability_effective_to=str_to_datetime(applicability.get("EffectiveTo", "1900-01-01T00:00:00"), ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S"]),
                product_applicability_is_approved=applicability.get("IsApproved"),
                product_applicability_brand=xsi_nil(applicability.get("Brand")),
                MDP_LOAD_ID=mdp_load_id,
                MDP_LOAD_TYPE=mdp_load_type,
                MDP_LOAD_DATETIME=mdp_load_datetime
            ))
    
    schema = T.StructType([
        T.StructField("approved_by", T.StringType(), False),
        T.StructField("approved_date", T.TimestampType(), False),
        T.StructField("is_approved", T.BooleanType(), False),
        T.StructField("product_beneficial_rate_period", T.StringType(), True),
        T.StructField("product_code", T.StringType(), False),
        T.StructField("product_description", T.StringType(), False),
        T.StructField("product_erc_profile_code", T.StringType(), True),
        T.StructField("product_eligibility_policy_code", T.StringType(), True),
        T.StructField("product_external_code", T.StringType(), True),
        T.StructField("product_feature_profile_code", T.StringType(), True),
        T.StructField("product_fee_profile_code", T.StringType(), True),
        T.StructField("product_name", T.StringType(), False),
        T.StructField("product_rate_profile_code", T.StringType(), True),
        T.StructField("product_set", T.StringType(), True),
        T.StructField("product_type", T.StringType(), False),
        T.StructField("product_applicability_channel_code", T.StringType(), False),
        T.StructField("product_applicability_effective_from", T.TimestampType(), False),
        T.StructField("product_applicability_effective_to", T.TimestampType(), True),
        T.StructField("product_applicability_is_approved", T.BooleanType(), False),
        T.StructField("product_applicability_brand", T.StringType(), True),
        T.StructField("MDP_LOAD_ID", T.StringType(), False),
        T.StructField("MDP_LOAD_TYPE", T.StringType(), False),
        T.StructField("MDP_LOAD_DATETIME", T.TimestampType(), False)
    ])

    return spark.createDataFrame(product_rows, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rate Profiles

# COMMAND ----------

@dlt.table(
    name="mso_xml_rate_profile", 
    comment="Data from Product.xml from MSO"
)
def get_rate_profile_data():
    df = spark.sql(f"SELECT * FROM {env_var}_catalog.bronze_mso_xml_int.rate_profiles")

    # Convert the entire root column structure into a JSON string
    df_with_json = df.select(
        F.to_json(F.col('root')).alias('root_json'),
        'MDP_LOAD_TYPE',
        'MDP_LOAD_ID',
        'MDP_LOAD_DATETIME',
        'MDP_BRONZE_LAYER_PROCESSED_DATETIME'
    )
    root_json = df_with_json.first().root_json
    rate_profile_json = json.loads(root_json)

    mdp_load_id = df_with_json.first().MDP_LOAD_ID
    mdp_load_type = df_with_json.first().MDP_LOAD_TYPE
    mdp_load_datetime = df_with_json.first().MDP_LOAD_DATETIME
    rate_profile_version = rate_profile_json.get("RateProfileVersion")
    rate_profile_sub_version = rate_profile_json.get("RateProfileSubVersion")
    
    rate_rows = []

    for rate_profile in rate_profile_json.get("RateProfiles", {}).get("RateProfile"):
        rates = json.loads(rate_profile.get("Rate"))
        if isinstance(rates, dict):
            rates = [rates]

        interest_charging_basis = rate_profile.get("InterestChargingBasis")
        is_approved = rate_profile.get("IsApproved")
        rate_profile_code = rate_profile.get("RateProfileCode")
        rate_profile_name = rate_profile.get("RateProfileName")

        for rate in rates:
            rate_rows.append(dict(
                interest_charging_basis=xsi_nil(interest_charging_basis),
                is_approved=xsi_nil(is_approved),
                rate_profile_code=xsi_nil(rate_profile_code),
                rate_profile_name=xsi_nil(rate_profile_name),
                rate_fixed_end_date=datetime.strptime(xsi_nil(rate.get("RateFixedEndDate", "1900-01-01T00:00:00")), "%Y-%m-%dT%H:%M:%S"),
                rate_sequence=xsi_nil(rate.get("RateSequence")),
                rate_adjustment=xsi_nil(rate.get("RateAdjustment")),
                base_rate_type_code=xsi_nil(rate.get("BaseRateTypeCode")),
                rate_is_approved=xsi_nil(rate.get("IsApproved")),
                collared_rate=xsi_nil(rate.get("CollaredRate")),
                capped_rate=xsi_nil(rate.get("CappedRate")),
                rate_duration=xsi_nil(rate.get("RateDuration")),
                rate_profile_version=rate_profile_version,
                rate_profile_sub_version=rate_profile_sub_version,
                MDP_LOAD_TYPE=mdp_load_type,
                MDP_LOAD_ID=int(mdp_load_id),
                MDP_LOAD_DATETIME=mdp_load_datetime
            ))
    schema = T.StructType([
        T.StructField("interest_charging_basis", T.StringType(), False),
        T.StructField("is_approved", T.BooleanType(), False),
        T.StructField("rate_profile_code", T.StringType(), False),
        T.StructField("rate_profile_name", T.StringType(), False),
        T.StructField("rate_fixed_end_date", T.TimestampType(), False),
        T.StructField("rate_sequence", T.IntegerType(), False),
        T.StructField("rate_adjustment", T.DoubleType(), False),
        T.StructField("base_rate_type_code", T.StringType(), False),
        T.StructField("rate_is_approved", T.BooleanType(), False),
        T.StructField("collared_rate", T.DoubleType(), True),
        T.StructField("capped_rate", T.DoubleType(), True),
        T.StructField("rate_duration", T.IntegerType(), True),
        T.StructField("rate_profile_version", T.IntegerType(), False),
        T.StructField("rate_profile_sub_version", T.IntegerType(), False),
        T.StructField("MDP_LOAD_TYPE", T.StringType(), False),
        T.StructField("MDP_LOAD_ID", T.IntegerType(), False),
        T.StructField("MDP_LOAD_DATETIME", T.TimestampType(), False)
    ])
    return spark.createDataFrame(rate_rows, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Fee Profiles

# COMMAND ----------


@dlt.table(
    name="mso_xml_fee_profile", 
    comment="Data from FeeProfile.xml from MSO"
)
def get_fee_profiles_data():
    df = spark.sql(f"SELECT * FROM {env_var}_catalog.bronze_mso_xml_int.fee_profiles")

    # Assuming your DataFrame is named `df`
    # Convert the entire root column structure into a JSON string
    df_with_json = df.select(
        F.to_json(F.col('root')).alias('root_json'),
        'MDP_LOAD_TYPE',
        'MDP_LOAD_ID',
        'MDP_LOAD_DATETIME',
        'MDP_BRONZE_LAYER_PROCESSED_DATETIME'
    )

    root_json = df_with_json.first().root_json
    fee_profiles_json = json.loads(root_json)

    mdp_load_id = df_with_json.first().MDP_LOAD_ID
    mdp_load_type = df_with_json.first().MDP_LOAD_TYPE
    mdp_load_datetime = df_with_json.first().MDP_LOAD_DATETIME
    fee_profiles_version = fee_profiles_json.get("FeeProfileVersion")
    fee_profiles_sub_version = fee_profiles_json.get("FeeProfileSubVersion")

    fee_rows = []

    for fee_profile in fee_profiles_json.get("FeeProfiles", {}).get("FeeProfile"):
        fee_profile_code = fee_profile.get("FeeProfileCode")
        fee_profile_name = fee_profile.get("FeeProfileName")
        fee_profile_type = fee_profile.get("FeeProfileType")
        is_approved = fee_profile.get("IsApproved")
        
        fees = fee_profile.get("Fees")
        for fee in [json.loads(fees.get("Fee"))]:
            if not isinstance(fee, list):
                fee = [fee]
            
            for f in fee:
                fee_rows.append(dict(
                    FEE_PROFILE_VERSION=fee_profiles_version,
                    FEE_PROFILE_SUB_VERSION=fee_profiles_sub_version,
                    FEE_PROFILE_CODE=fee_profile_code,
                    FEE_PROFILE_NAME=fee_profile_name,
                    FEE_PROFILE_TYPE=fee_profile_type,
                    FEE_CODE=f.get("FeeCode"),
                    ELIGIBILITY_POLICY_CODE=xsi_nil(f.get("EligibilityPolicyCode")),
                    IS_APPROVED=is_approved,
                    MDP_LOAD_ID=int(mdp_load_id),
                    MDP_LOAD_TYPE=mdp_load_type,
                    MDP_LOAD_DATETIME=mdp_load_datetime,
                    ROW_START_DATETIME=mdp_load_datetime,
                    ROW_END_DATETIME=None
                ))

        schema = T.StructType([
            T.StructField("FEE_PROFILE_VERSION", T.IntegerType(), False),
            T.StructField("FEE_PROFILE_SUB_VERSION", T.IntegerType(), False),
            T.StructField("FEE_PROFILE_CODE", T.StringType(), True),
            T.StructField("FEE_PROFILE_NAME", T.StringType(), True),
            T.StructField("FEE_PROFILE_TYPE", T.StringType(), True),
            T.StructField("FEE_CODE", T.StringType(), True),
            T.StructField("ELIGIBILITY_POLICY_CODE", T.StringType(), True),
            T.StructField("IS_APPROVED", T.BooleanType(), True),
            T.StructField("MDP_LOAD_ID", T.IntegerType(), False),
            T.StructField("MDP_LOAD_TYPE", T.StringType(), False),
            T.StructField("MDP_LOAD_DATETIME", T.TimestampType(), False),
            T.StructField("ROW_START_DATETIME", T.TimestampType(), False),
            T.StructField("ROW_END_DATETIME", T.TimestampType(), True)
        ])

    df = spark.createDataFrame(fee_rows, schema)

    # Define the window specification
    windowSpec = dlf.get_window_spec(["FEE_PROFILE_CODE", "FEE_CODE"], "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)


    return df
      
