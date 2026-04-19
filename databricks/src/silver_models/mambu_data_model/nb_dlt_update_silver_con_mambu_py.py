# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window
import mdp_databricks_common.silver_layer_functions as dlf
from mdp_databricks_common.feature_toggle import feature_toggle as ft

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

from mdp_databricks_common.utils.time_utils import get_run_date

# COMMAND ----------

#get the run date from common libs
RUN_DATE = get_run_date()

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC # Mambu Transition-State-1 (Baseline Savings -- deployed to Production in December 2024)

# COMMAND ----------

@dlt.table(
    name="mambu_client", 
    comment="Captures information about individual clients of the mfi. Clients may have their own accounts or belong to groups and are assigned to users and branches. Custom fields, identification documents & addresses are stored in separate tables."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_client():

    df = spark.sql(
        f"""
    SELECT   
        ENCODEDKEY AS ENCODED_KEY,
        ASSIGNEDBRANCHKEY AS ASSIGNED_BRANCH_KEY,
        ASSIGNEDCENTREKEY AS ASSIGNED_CENTRE_KEY,
        ASSIGNEDUSERKEY AS ASSIGNED_USER_KEY,
        CLIENTROLEKEY AS CLIENT_ROLE_KEY,
        PROFILEPICTUREKEY AS PROFILE_PICTURE_KEY,
        PROFILESIGNATUREKEY AS PROFILE_SIGNATURE_KEY,
        PORTALPREFERENCESKEY AS PORTAL_PREFERENCES_KEY,
        ID,
        FIRSTNAME AS FIRST_NAME,
        MIDDLENAME AS MIDDLE_NAME,
        LASTNAME AS LAST_NAME,
        APPROVEDDATE AS APPROVED_DATE,
        ACTIVATIONDATE AS ACTIVATION_DATE,
        CLOSEDDATE AS CLOSED_DATE,
        CAST(BIRTHDATE AS DATE) AS BIRTH_DATE,
        GENDER,
        STATE,
        LOANCYCLE AS LOAN_CYCLE,
        GROUPLOANCYCLE AS GROUP_LOAN_CYCLE,
        MOBILEPHONE1 AS MOBILE_PHONE_1,
        MOBILEPHONE2 AS MOBILE_PHONE_2,
        HOMEPHONE AS HOME_PHONE,
        EMAILADDRESS AS EMAIL_ADDRESS,
        PREFERREDLANGUAGE AS PREFERRED_LANGUAGE,
        CAST(CREATIONDATE AS DATE) AS CREATION_DATE,
        CREATIONDATE AS CREATION_DATETIME,
        NOTES,
        MIGRATIONEVENTKEY AS MIGRATION_EVENT_KEY,
        CAST(LASTMODIFIEDDATE AS DATE) AS LAST_MODIFIED_DATE,
        CAST(LASTMODIFIEDDATE AS TIMESTAMP) AS LAST_MODIFIED_DATETIME,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_mambu_con.client
    """
    )
    
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_branch", 
    comment="A branch is the main division criteria of the organization"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_branch():

    df = spark.sql(
        f"""
    SELECT   
        ENCODEDKEY AS ENCODED_KEY,
        ID,
        NAME,
        STATE,
        EMAILADDRESS AS EMAIL_ADDRESS,
        PHONENUMBER AS PHONE_NUMBER,
        NOTES,
        CAST(CREATIONDATE AS DATE) AS CREATION_DATE,
        CAST(CREATIONDATE AS TIMESTAMP) AS CREATION_DATETIME,
        CAST(LASTMODIFIEDDATE AS DATE) AS LAST_MODIFIED_DATE,
        CAST(LASTMODIFIEDDATE AS TIMESTAMP) AS LAST_MODIFIED_DATETIME,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_mambu_con.branch
    """
    )

   
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")
    df = dlf.get_row_number(df, windowSpec)    

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_payment_detail", 
    comment="Holds details of payments made via the Mambu payments gateway including SEPA Direct Debit and Credit Transfers"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_payment_detail():

    df = spark.sql(
        f"""
    SELECT  
        ENCODEDKEY AS ENCODED_KEY,
        SAVINGSTRANSACTIONKEY AS SAVING_TRANSACTION_KEY,        
        CREDITORACCOUNTIBAN AS CREDITOR_ACCOUNT_IBAN,
        CREDITORNAME AS CREDITOR_NAME,
        CREDITORACCOUNTCURRENCY AS CREDITOR_ACCOUNT_CURRENCY,
        CREDITORACCOUNTOTHERIDENTIFICATION AS CREDITOR_ACCOUNT_OTHER_IDENTIFICATION,
        CREDITORACCOUNTOTHERSCHEME AS CREDITOR_ACCOUNT_OTHER_SCHEME,
        CREDITORAGENTBIC AS CREDITOR_AGENT_BIC,
        DEBTORACCOUNTIBAN AS DEBTOR_ACCOUNT_IBAN,
        DEBTORNAME AS DEBTOR_NAME,
        DEBTORACCOUNTCURRENCY AS DEBTOR_ACCOUNT_CURRENCY,
        DEBTORACCOUNTOTHERIDENTIFICATION AS DEBTOR_ACCOUNT_OTHER_IDENTIFICATION,
        DEBTORACCOUNTOTHERSCHEME AS DEBTOR_ACCOUNT_OTHER_SCHEME,
        DEBTORAGENTBIC AS DEBTOR_AGENT_BIC,
        ENDTOENDIDENTIFICATION AS END_TO_END_IDENTIFICATION,
        INSTRUCTIONIDENTIFICATION AS INSTRUCTION_IDENTIFICATION,
        REMITTANCEINFORMATIONSTRUCTUREDREFERENCE AS REMITTANCE_INFORMATION_STRUCTURED_REFERENCE,
        REMITTANCEINFORMATIONSTRUCTUREDREFERENCEISSUER AS REMITTANCE_INFORMATION_STRUCTURED_REFERENCE_ISSUER,
        REMITTANCEINFORMATIONSTRUCTUREDREFERENCETYPE AS REMITTANCE_INFORMATION_STRUCTURED_REFERENCE_TYPE,
        REMITTANCEINFORMATIONUNSTRUCTURED AS REMITTANCE_INFORMATION_UNSTRUCTURED,
        SERVICELEVELCODE AS SERVICE_LEVEL_CODE,
        TRANSACTIONIDENTIFICATION AS TRANSACTION_IDENTIFICATION,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        TO_TIMESTAMP(NULL, 'dd-MMM-yy HH:mm:ss') AS ROW_END_DATETIME       
    FROM {env_var}_catalog.bronze_mambu_con.paymentdetails
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME") 
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_user", 
    comment="A user is the entity that can access the mambu application. it can be a person: the manager of the organization, a credit officer or another stuff memeber, or it can be another application (that access the api)"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_user():

    df = spark.sql(
        f"""
    SELECT  
        ENCODEDKEY AS ENCODED_KEY,
        ASSIGNEDBRANCHKEY AS ASSIGNED_BRANCH_KEY,
        USERPREFERENCESKEY AS USER_PREFERENCES_KEY,
        ROLE_ENCODEDKEY_OID AS ROLE_ENCODED_KEY_OID,
        PERMISSIONS_ENCODEDKEY_OID AS PERMISSIONS_ENCODED_KEY_OID,
        ID,
        TITLE,
        USERNAME AS USER_NAME,
        FIRSTNAME AS FIRST_NAME,
        LASTNAME AS LAST_NAME,
        ALIAS,
        USERSTATE AS USER_STATE,
        EMAIL,
        MOBILEPHONE1 AS MOBILE_PHONE_1,
        HOMEPHONE AS HOME_PHONE,
        LANGUAGE,
        TRANSACTIONLIMITS AS TRANSACTION_LIMITS,
        TWOFACTORAUTHENTICATION AS TWO_FACTOR_AUTHENTICATION,
        ISADMINISTRATOR AS IS_ADMINISTRATOR,
        ISCREDITOFFICER AS IS_CREDIT_OFFICER,
        ISDELIVERY AS IS_DELIVERY,
        ISSUPPORT AS IS_SUPPORT,
        ISTELLER AS IS_TELLER,
        LASTLOGGEDINDATE AS LAST_LOGGED_IN_DATE,
        FAILEDLOGINSCOUNT AS FAILED_LOGINS_COUNT,
        FAILEDLOGINSDATES AS FAILED_LOGINS_DATES,
        NOTES,
        CAST(CREATIONDATE AS DATE) AS CREATION_DATE,
        CAST(CREATIONDATE AS TIMESTAMP) AS CREATION_DATETIME,
        CAST(LASTMODIFIEDDATE AS DATE) AS LAST_MODIFIED_DATE,
        CAST(LASTMODIFIEDDATE AS TIMESTAMP) AS LAST_MODIFIED_DATETIME,          
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME        
    FROM {env_var}_catalog.bronze_mambu_con.user
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")  
    df = dlf.get_row_number(df, windowSpec) 
     
    return df

# COMMAND ----------

@dlt.table(
    name="mambu_index_rate", 
    comment="index value used as a base interest rate in some organizations for the calculation of the loan interest rate as relative to this value. the amount they give it out for is fixed, but they or an external source such as the government, etc, may change the base interest rate and this means that the loans need to be updated so that the interest rate value should be changed as well."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_index_rate():

    df = spark.sql(
        f"""
    SELECT  
        ENCODEDKEY AS ENCODED_KEY,
        INDEXINTERESTRATESOURCE_ENCODEDKEY_OID AS SOURCE_ENCODED_KEY_OID,
        USERKEY AS USER_KEY,
        ROUND(RATE, 2) AS VALUE,
        STARTDATE AS START_DATE,
        NOTES,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        TO_TIMESTAMP(NULL, 'dd-MMM-yy HH:mm:ss') AS ROW_END_DATETIME 
    FROM {env_var}_catalog.bronze_mambu_con.indexrate
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")       
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_index_rate_source", 
    comment="The set of possible sources for an index interest rate that can come from (ex: one source may be libor) or for a tax rate."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_index_rate_source():

    df = spark.sql(
        f"""
    SELECT  
        ENCODEDKEY AS ENCODED_KEY,
        ID,
        NAME,
        TYPE,
        NOTES,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        TO_TIMESTAMP(NULL, 'dd-MMM-yy HH:mm:ss') AS ROW_END_DATETIME 
    FROM {env_var}_catalog.bronze_mambu_con.indexratesource
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")
    df = dlf.get_row_number(df, windowSpec)    

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_custom_field", 
    comment="To store the value for a custom field for a client or group, for example, if the customfield is education then the value may store the string bachelor's degree."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_custom_field():

    df = spark.sql(
        f"""
    SELECT  
        ENCODEDKEY AS ENCODED_KEY,
        ID,
        DATATYPE AS DATA_TYPE,
        ISDEFAULT AS IS_DEFAULT,
        ISREQUIRED AS IS_REQUIRED,
        NAME,
        VALUES,
        AMOUNTS,
        DESCRIPTION,
        TYPE,
        VALUELENGTH AS VALUE_LENGTH,
        INDEXINLIST AS INDEX_IN_LIST,
        CUSTOMFIELDSET_ENCODEDKEY_OID AS CUSTOM_FIELD_SET_ENCODED_KEY_OID,
        STATE,
        VALIDATIONPATTERN AS VALIDATION_PATTERN,
        VIEWUSAGERIGHTSKEY AS VIEW_USAGE_RIGHTS_ENCODED_KEY,
        EDITUSAGERIGHTSKEY AS EDIT_USAGE_RIGHTS_ENCODED_KEY,
        BUILTINCUSTOMFIELDID AS BUILT_IN_CUSTOM_FIELD_ID, 
        UNIQUE,
        TEMPORARYID AS TEMPORARY_ID,
        AVAILABLEFORALL AS AVAILABLE_FOR_ALL,
        ID_GENERATED,
        CREATIONDATE AS CREATION_DATE,
        LASTMODIFIEDDATE AS LAST_MODIFIED_DATE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        TO_TIMESTAMP(NULL, 'dd-MMM-yy HH:mm:ss') AS ROW_END_DATETIME 
    FROM {env_var}_catalog.bronze_mambu_con.customfield
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_custom_field_value", 
    comment="To store the value for a custom field for a client or group, for example if the customfield is education then the value may store the string bachelor's degree."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_custom_field_value():

    df = spark.sql(
        f"""
    SELECT  
        ENCODEDKEY AS ENCODED_KEY,
        CUSTOMFIELDKEY AS CUSTOM_FIELD_KEY,
        PARENTKEY AS PARENT_KEY,
        INDEXINLIST AS INDEX_IN_LIST,
        VALUE,
        LINKEDENTITYKEYVALUE AS LINKED_ENTITY_KEY_VALUE,
        AMOUNT,
        CUSTOMFIELDSETGROUPINDEX AS CUSTOM_FIELD_SET_GROUP_INDEX,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
        TO_TIMESTAMP(NULL, 'dd-MMM-yy HH:mm:ss') AS ROW_END_DATETIME 
    FROM {env_var}_catalog.bronze_mambu_con.customfieldvalue
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_custom_field_value_pivot", 
    comment="Pivot the rows into columns to store the value for a custom field"
)
@dlt.expect("Check that PARENT_KEY is not null", "PARENT_KEY IS NOT NULL")
def mambu_custom_field_value():

    df = spark.sql(
        f"""
    SELECT
        PARENT_KEY,
        MAX(CASE WHEN ID = 'Birth_Date_Details_Clients' THEN VALUE END) AS BIRTH_DATE_DETAILS_CLIENTS,
        MAX(CASE WHEN ID = 'EMail_Address_Details_Clients' THEN VALUE END) AS EMAIL_ADDRESS_DETAILS_CLIENTS,
        MAX(CASE WHEN ID = 'First_Names_General_Clients' THEN VALUE END) AS FIRST_NAMES_GENERAL_CLIENTS,
        MAX(CASE WHEN ID = 'Gender_Details_Clients' THEN VALUE END) AS GENDER_DETAILS_CLIENTS,
        MAX(CASE WHEN ID = 'Home_Phone_Details_Clients' THEN VALUE END) AS HOME_PHONE_DETAILS_CLIENTS,
        MAX(CASE WHEN ID = 'Last_Name_General_Clients' THEN VALUE END) AS LAST_NAME_GENERAL_CLIENTS,
        MAX(CASE WHEN ID = 'Middle_Names_General_Clients' THEN VALUE END) AS MIDDLE_NAMES_GENERAL_CLIENTS,
        MAX(CASE WHEN ID = 'Mobile_Phone_2_Details_Clients' THEN VALUE END) AS MOBILE_PHONE_2_DETAILS_CLIENTS,
        MAX(CASE WHEN ID = 'Mobile_Phone_Details_Clients' THEN VALUE END) AS MOBILE_PHONE_DETAILS_CLIENTS,
        MAX(CASE WHEN ID = 'accountNumber' THEN VALUE END) AS ACCOUNT_NUMBER,
        MAX(CASE WHEN ID = 'accountNumberAndAccountSuffix' THEN VALUE END) AS ACCOUNT_NUMBER_AND_ACCOUNT_SUFFIX,
        MAX(CASE WHEN ID = 'accountNumberCA' THEN VALUE END) AS ACCOUNT_NUMBER_CA,
        MAX(CASE WHEN ID = 'accountNumberLA' THEN VALUE END) AS ACCOUNT_NUMBER_LA,
        MAX(CASE WHEN ID = 'accountSuffix' THEN VALUE END) AS ACCOUNT_SUFFIX,
        MAX(CASE WHEN ID = 'accountSuffixCA' THEN VALUE END) AS ACCOUNT_SUFFIX_CA,
        MAX(CASE WHEN ID = 'accountSuffixLA' THEN VALUE END) AS ACCOUNT_SUFFIX_LA,
        MAX(CASE WHEN ID = 'applicationCostCentre' THEN VALUE END) AS APPLICATION_COST_CENTRE,
        MAX(CASE WHEN ID = 'collectionId' THEN VALUE END) AS COLLECTION_ID,
        MAX(CASE WHEN ID = 'contractualMonthlyPayment' THEN TRY_CAST(VALUE AS DECIMAL(38,2)) END) AS CONTRACTUAL_MONTHLY_PAYMENT,
        MAX(CASE WHEN ID = 'costCentre' THEN VALUE END) AS COST_CENTRE,
        MAX(CASE WHEN ID = 'mcdRegulatedLA' THEN VALUE END) AS MCD_REGULATED_LA,
        MAX(CASE WHEN ID = 'mcobRegulatedLA' THEN VALUE END) AS MCOB_REGULATED_LA,
        MAX(CASE WHEN ID = 'originalAdvanceAmountLA' THEN TRY_CAST(VALUE AS DECIMAL(38,2)) END) AS ORIGINAL_ADVANCE_AMOUNT_LA,
        MAX(CASE WHEN ID = 'originalOpenDateCA' THEN VALUE END) AS ORIGINAL_OPEN_DATE_CA,
        MAX(CASE WHEN ID = 'originalStartDateLA' THEN VALUE END) AS ORIGINAL_START_DATE_LA,
        MAX(CASE WHEN ID = 'originalTermLA' THEN VALUE END) AS ORIGINAL_TERM_LA,
        MAX(CASE WHEN ID = 'originationCostCentreCA' THEN VALUE END) AS ORIGINATION_COST_CENTRE_CA,
        MAX(CASE WHEN ID = 'originationCostCentreLA' THEN VALUE END) AS ORIGINATION_COST_CENTRE_LA,
        MAX(CASE WHEN ID = 'partNumberLA' THEN VALUE END) AS PART_NUMBER_LA,
        MAX(CASE WHEN ID = 'preferredPaymentDay' THEN VALUE END) AS PREFERRED_PAYMENT_DAY,
        MAX(CASE WHEN ID = 'productId' THEN VALUE END) AS PRODUCT_ID,
        MAX(CASE WHEN ID = 'productIdLA' THEN VALUE END) AS PRODUCT_ID_LA,
        MAX(CASE WHEN ID = 'productNameLA' THEN VALUE END) AS PRODUCT_NAME_LA,
        MAX(CASE WHEN ID = 'purposeCodeLA' THEN VALUE END) AS PURPOSE_CODE_LA,
        MAX(CASE WHEN ID = 'repaymentTransactionId' THEN VALUE END) AS REPAYMENT_TRANSACTION_ID,
        MAX(CASE WHEN ID = 'sortCode' THEN VALUE END) AS SORT_CODE,
        MAX(CASE WHEN ID = 'sortCodeAndAccountNumber' THEN VALUE END) AS SORT_CODE_AND_ACCOUNT_NUMBER,
        MAX(CASE WHEN ID = 'sortCodeCA' THEN VALUE END) AS SORT_CODE_CA,
        MAX(CASE WHEN ID = 'sortCodeLA' THEN VALUE END) AS SORT_CODE_LA,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME
    FROM
    (
        SELECT
            val.PARENTKEY AS PARENT_KEY,
            field.ID,
            val.VALUE,
            CAST(val.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
            TO_TIMESTAMP(val.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
            TO_TIMESTAMP(val.MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,
            TO_TIMESTAMP(NULL, 'dd-MMM-yy HH:mm:ss') AS ROW_END_DATETIME 
        FROM {env_var}_catalog.bronze_mambu_con.customfieldvalue AS val
        INNER JOIN {env_var}_catalog.bronze_mambu_con.customfield AS field
        ON field.ENCODEDKEY = val.CUSTOMFIELDKEY
    ) AS subquery
    GROUP BY 
        PARENT_KEY,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME
    """
    )

    # Define the window specification
    windowSpec = dlf.get_window_spec("PARENT_KEY","MDP_LOAD_DATETIME")
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------------------------------
# MAGIC # Mambu TS2-Prove-Point-One (Mortgage Tables -- deployed to Production in August 2025)

# COMMAND ----------

@dlt.table(
    name="mambu_custom_field_set", 
    comment="defines a set of custom fields for grouping them on the interface"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_custom_field_set():

    df = spark.sql(
        f"""
    SELECT   
        ENCODEDKEY AS ENCODED_KEY,
        ID ,
        NAME,
        NOTES,
        TYPE,               
        INDEXINLIST AS INDEX_IN_LIST, 
        USAGE, 
        BUILTINTYPE AS BUILT_IN_TYPE, 
        TEMPORARYID AS TEMPORARY_ID, 
        CAST(CREATEDDATE AS DATE) AS CREATED_DATE, 
        CAST(CREATEDDATE AS TIMESTAMP) AS CREATED_DATETIME,
        CAST(LASTMODIFIEDDATE AS DATE) AS LAST_MODIFIED_DATE,
        CAST(LASTMODIFIEDDATE AS TIMESTAMP) AS LAST_MODIFIED_DATETIME,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_mambu_con.customfieldset
    """
    )
    
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_gl_journal_entry", 
    comment="a journal entry is an accounting’s version of a transaction. it records an event being written to the general ledger."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_gl_journal_entry():

    df = spark.sql(
        f"""
    SELECT   
        ENCODEDKEY AS ENCODED_KEY,
        ACCOUNTKEY AS ACCOUNT_KEY,
        AMOUNT,
        ENTRYID AS ENTRY_ID,
        GLACCOUNT_ENCODEDKEY_OID AS GL_ACCOUNT_ENCODED_KEY_OID,
        NOTES,
        PRODUCTTYPE AS PRODUCT_TYPE,  
        TRANSACTIONID AS TRANSACTION_ID,
        TYPE,
        USERKEY AS USER_KEY,
        ASSIGNEDBRANCHKEY AS ASSIGNED_BRANCH_KEY,
        REVERSALENTRYKEY AS REVERSAL_ENTRY_KEY,
        PRODUCTKEY AS PRODUCT_KEY,
        CAST(CREATIONDATE AS DATE) AS CREATION_DATE,
        CAST(CREATIONDATE AS TIMESTAMP) AS CREATION_DATETIME,
        CAST(ENTRYDATE AS DATE) AS ENTRY_DATE,
        CAST(ENTRYDATE AS TIMESTAMP) AS ENTRY_DATETIME,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_mambu_con.gljournalentry
    """
    )
    
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_line_of_credit", 
    comment="a maximum loan amount that can be approved or disbursed for a client or a group. it is a limit also for the period under which the clients/groups are allowed to make loans or overdrafts"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_line_of_credit():

    df = spark.sql(
        f"""
    SELECT   
        ENCODEDKEY AS ENCODED_KEY,
        STATE,
        SUBSTATE AS SUB_STATE,
        AMOUNT,
        NOTES ,       
        CLIENTKEY AS CLIENT_KEY,
        GROUPKEY AS GROUP_KEY,
        ID,       
        EXPOSURELIMITTYPE AS EXPOSURE_LIMIT_TYPE,
        CURRENCYCODE AS CURRENCY_CODE,        
        CAST(APPROVEDDATE AS DATE) AS APPROVED_DATE,
        CAST(APPROVEDDATE AS TIMESTAMP) AS APPROVED_DATETIME,
        CAST(CLOSEDDATE AS DATE) AS CLOSED_DATE,
        CAST(CLOSEDDATE AS TIMESTAMP) AS CLOSED_DATETIME,
        CAST(CREATIONDATE AS DATE) AS CREATION_DATE,
        CAST(CREATIONDATE AS TIMESTAMP) AS CREATION_DATETIME,
        CAST(EXPIREDATE AS DATE) AS EXPIRE_DATE,
        CAST(EXPIREDATE AS TIMESTAMP) AS EXPIRE_DATETIME,
        CAST(LASTMODIFIEDDATE AS DATE) AS LAST_MODIFIED_DATE,
        CAST(LASTMODIFIEDDATE AS TIMESTAMP) AS LAST_MODIFIED_DATETIME,
        CAST(STARTDATE AS DATE) AS START_DATE,
        CAST(STARTDATE AS TIMESTAMP) AS START_DATETIME,  
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_mambu_con.lineofcredit
    """
    )
    
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_loan_account", 
    comment="stores a loan account or a loan application (which is just a loan account which is not yet active). a loan account always belongs to a client or group and must be of a certain loan product type."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_loan_account():

    df = spark.sql(
        f"""
    SELECT   
        ENCODEDKEY AS ENCODED_KEY,
        ACCOUNTHOLDERKEY AS ACCOUNT_HOLDER_KEY,
        ACCOUNTHOLDERTYPE AS ACCOUNT_HOLDER_TYPE,
        ACCOUNTSTATE AS STATE,
        ACCOUNTSUBSTATE AS SUB_STATE,
        RESCHEDULEDACCOUNTKEY AS RESCHEDULED_ACCOUNT_KEY,        
        ASSIGNEDBRANCHKEY AS ASSIGNED_BRANCH_KEY,
        ASSIGNEDUSERKEY AS ASSIGNED_USER_KEY,
        FEESDUE AS TOTAL_FEES_DUE,
        FEESPAID AS TOTAL_FEES_PAID,
        GRACEPERIOD AS GRACE_PERIOD,
        GRACEPERIODTYPE AS GRACE_PERIOD_TYPE,
        ID,
        INTERESTCALCULATIONMETHOD AS INTEREST_CALCULATION_METHOD,
        INTERESTTYPE AS INTEREST_TYPE,
        REPAYMENTSCHEDULEMETHOD AS REPAYMENT_SCHEDULE_METHOD,
        INTERESTAPPLICATIONMETHOD AS INTEREST_APPLICATION_METHOD,
        PAYMENTMETHOD AS PAYMENT_METHOD,
        INTERESTCHARGEFREQUENCY AS INTEREST_CHARGE_FREQUENCY,
        INTERESTBALANCE AS INTEREST_BALANCE,
        INTERESTPAID AS INTEREST_PAID,
        INTERESTRATE AS INTEREST_RATE,
        LOANAMOUNT AS LOAN_AMOUNT,
        PERIODICPAYMENT AS PERIODIC_PAYMENT,
        LOANGROUP_ENCODEDKEY_OID AS LOAN_GROUP_ENCODED_KEY_OID,
        LOANNAME AS LOAN_NAME,
        NOTES,
        PENALTYDUE AS PENALTY_DUE,
        PENALTYPAID AS PENALTY_PAID,
        PRINCIPALBALANCE AS PRINCIPAL_BALANCE,
        PRINCIPALPAID AS PRINCIPAL_PAID,
        PRODUCTTYPEKEY AS PRODUCT_TYPE_KEY,
        REPAYMENTINSTALLMENTS AS REPAYMENT_INSTALLMENTS,
        REPAYMENTPERIODCOUNT AS REPAYMENT_PERIOD_COUNT,
        REPAYMENTPERIODUNIT AS REPAYMENT_PERIOD_UNIT,
        ACCOUNTS_INTEGER_IDX AS ACCOUNTS_INTEGER_IDX,
        MIGRATIONEVENTKEY AS MIGRATION_EVENT_KEY,
        ASSIGNEDCENTREKEY AS ASSIGNED_CENTRE_KEY,        
        PRINCIPALREPAYMENTINTERVAL AS PRINCIPAL_REPAYMENT_INTERVAL,
        PRINCIPALDUE AS PRINCIPAL_DUE,
        INTERESTDUE AS INTEREST_DUE,
        ACCRUELATEINTEREST AS ACCRUE_LATE_INTEREST,
        INTERESTSPREAD AS INTEREST_SPREAD,
        INTERESTRATESOURCE AS INTEREST_RATE_SOURCE,
        INTERESTRATEREVIEWUNIT AS INTEREST_RATE_REVIEW_UNIT,
        INTERESTRATEREVIEWCOUNT AS INTEREST_RATE_REVIEW_COUNT,
        ACCRUEDINTEREST AS ACCRUED_INTEREST,
        FEESBALANCE AS FEES_BALANCE,
        PENALTYBALANCE AS PENALTY_BALANCE,
        SCHEDULEDUEDATESMETHOD AS SCHEDULED_DUE_DATE_METHOD,
        HASCUSTOMSCHEDULE AS HAS_CUSTOM_SCHEDULE,
        FIXEDDAYSOFMONTH AS FIXED_DAYS_OF_MONTH,
        SHORTMONTHHANDLINGMETHOD AS SHORT_MONTH_HANDLING_METHOD,
        TAXRATE AS TAX_RATE,  
        PENALTYRATE AS PENALTY_RATE,
        LOANPENALTYCALCULATIONMETHOD AS LOAN_PENALTY_CALCULATION_METHOD,
        ACCRUEDPENALTY AS ACCRUED_PENALTY,
        ACTIVATIONTRANSACTIONKEY AS ACTIVATION_TRANSACTION_KEY,
        LINEOFCREDITKEY AS LINE_OF_CREDIT_KEY,
        LOCKEDOPERATIONS AS LOCKED_OPERATIONS,
        INTERESTCOMMISSION AS INTEREST_COMMISSION,
        DEFAULTFIRSTREPAYMENTDUEDATEOFFSET AS DEFAULT_FIRST_REPAYMENT_DUE_DATE_OFFSET,
        PRINCIPALPAYMENTSETTINGSKEY AS PRINCIPAL_PAYMENT_SETTINGS_KEY,
        INTERESTBALANCECALCULATIONMETHOD AS INTEREST_BALANCE_CALCULATION_METHOD,
        DISBURSEMENTDETAILSKEY AS DISBURSEMENT_DETAILS_KEY,
        ARREARSTOLERANCEPERIOD AS ARREARS_TOLERANCE_PERIOD,
        ACCRUEINTERESTAFTERMATURITY AS ACCRUE_INTEREST_AFTER_MATURITY,
        PREPAYMENTRECALCULATIONMETHOD AS PRE_PAYMENT_RECALCULATION_METHOD,
        PRINCIPALPAIDINSTALLMENTSTATUS AS PRINCIPAL_PAID_INSTALLMENT_STATUS,
        ELEMENTSRECALCULATIONMETHOD AS ELEMENTS_RECALCULATION_METHOD,
        LATEPAYMENTSRECALCULATIONMETHOD AS LATE_PAYMENTS_RECALCULATION_METHOD,
        APPLYINTERESTONPREPAYMENTMETHOD AS APPLY_INTEREST_ON_PRE_PAYMENT_METHOD,
        ALLOWOFFSET AS ALLOW_OFFSET,
        FUTUREPAYMENTSACCEPTANCE AS FUTURE_PAYMENTS_ACCEPTANCE,
        REDRAWBALANCE AS REDRAW_BALANCE,
        PREPAYMENTACCEPTANCE AS PRE_PAYMENT_ACCEPTANCE,
        INTERESTFROMARREARSACCRUED AS INTEREST_FROM_ARREARS_ACCRUED,
        INTERESTFROMARREARSDUE AS INTEREST_FROM_ARREARS_DUE,
        INTERESTFROMARREARSPAID AS INTEREST_FROM_ARREARS_PAID,
        INTERESTFROMARREARSBALANCE AS INTEREST_FROM_ARREARS_BALANCE,
        INTERESTROUNDINGVERSION AS INTEREST_ROUNDING_VERSION,
        ACCOUNTARREARSSETTINGSKEY AS ACCOUNT_ARREARS_SETTINGS_KEY,
        HOLDBALANCE AS HOLD_BALANCE,     
        CAST(CLOSEDDATE AS DATE) AS CLOSED_DATE,
        CAST(CLOSEDDATE AS TIMESTAMP) AS CLOSED_DATETIME,
        CAST(LASTLOCKEDDATE AS DATE) AS LAST_LOCKED_DATE,
        CAST(LASTLOCKEDDATE AS TIMESTAMP) AS LAST_LOCKED_DATETIME,
        CAST(CREATIONDATE AS DATE) AS CREATION_DATE,
        CAST(CREATIONDATE AS TIMESTAMP) AS CREATION_DATETIME,
        CAST(APPROVEDDATE AS DATE) AS APPROVED_DATE,
        CAST(APPROVEDDATE AS TIMESTAMP) AS APPROVED_DATETIME,
        CAST(LASTMODIFIEDDATE AS DATE) AS LAST_MODIFIED_DATE,
        CAST(LASTMODIFIEDDATE AS TIMESTAMP) AS LAST_MODIFIED_DATETIME,
        CAST(LASTSETTOARREARSDATE AS DATE) AS LAST_SET_TO_ARREARS_DATE,
        CAST(LASTSETTOARREARSDATE AS TIMESTAMP) AS LAST_SET_TO_ARREARS_DATETIME,
        CAST(LASTACCOUNTAPPRAISALDATE AS DATE) AS LAST_ACCOUNT_APPRAISAL_DATE,
        CAST(LASTACCOUNTAPPRAISALDATE AS TIMESTAMP) AS LAST_ACCOUNT_APPRAISAL_DATETIME,
        CAST(LASTINTERESTREVIEWDATE AS DATE) AS LAST_INTEREST_REVIEW_DATE,
        CAST(LASTINTERESTREVIEWDATE AS TIMESTAMP) AS LAST_INTEREST_REVIEW_DATETIME,
        CAST(LASTINTERESTAPPLIEDDATE AS DATE) AS LAST_INTEREST_APPLIED_DATE,
        CAST(LASTINTERESTAPPLIEDDATE AS TIMESTAMP) AS LAST_INTEREST_APPLIED_DATETIME,
        CAST(LASTTAXRATEREVIEWDATE AS DATE) AS LAST_TAX_RATE_REVIEW_DATE,
        CAST(LASTTAXRATEREVIEWDATE AS TIMESTAMP) AS LAST_TAX_RATE_REVIEW_DATETIME,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_mambu_con.loanaccount
    """
    )
    
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_repayment", 
    comment="captures the details about repayments which are both due and have been paid off. all repayments belong to a loan account but are also themselves assigned to creditofficer & branches (for performance look-up reasons)."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_repayment():

    df = spark.sql(
        f"""
    SELECT
        ENCODEDKEY AS ENCODED_KEY,
        ASSIGNEDBRANCHKEY AS ASSIGNED_BRANCH_KEY,
        ASSIGNEDUSERKEY AS ASSIGNED_USER_KEY,
        INTERESTDUE AS INTEREST_DUE,
        INTERESTPAID AS INTEREST_PAID,       
        NOTES,
        PARENTACCOUNTKEY AS PARENT_ACCOUNT_KEY,
        PRINCIPALDUE AS PRINCIPAL_DUE,
        PRINCIPALPAID AS PRINCIPAL_PAID,        
        STATE AS STATUS,
        ASSIGNEDCENTREKEY AS ASSIGNED_CENTRE_KEY,
        FEESDUE AS FEES_DUE,
        FEESPAID AS FEES_PAID,
        PENALTYDUE AS PENALTY_DUE,
        PENALTYPAID AS PENALTY_PAID,
        TAXINTERESTDUE AS TAX_INTEREST_DUE,
        TAXINTERESTPAID AS TAX_INTEREST_PAID,
        TAXFEESDUE AS TAX_FEES_DUE,
        TAXFEESPAID AS TAX_FEES_PAID,
        TAXPENALTYDUE AS TAX_PENALTY_DUE,
        TAXPENALTYPAID AS TAX_PENALTY_PAID,
        ORGANIZATIONCOMMISSIONDUE AS ORGANIZATION_COMMISSION_DUE,
        FUNDERSINTERESTDUE AS FUNDERS_INTEREST_DUE,
        CAST(DUEDATE AS DATE) AS DUE_DATE,
        CAST(DUEDATE AS TIMESTAMP) AS DUE_DATE_DATETIME,
        CAST(LASTPAIDDATE AS DATE) AS LAST_PAID_DATE,
        CAST(LASTPAIDDATE AS TIMESTAMP) AS LAST_PAID_DATETIME,
        CAST(LASTPENALTYAPPLIEDDATE AS DATE) AS LAST_PENALTY_APPLIED_DATE,
        CAST(LASTPENALTYAPPLIEDDATE AS TIMESTAMP) AS LAST_PENALTY_APPLIED_DATETIME,
        CAST(REPAIDDATE AS DATE) AS REPAID_DATE,
        CAST(REPAIDDATE AS TIMESTAMP) AS REPAID_DATETIME,     
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS ROW_START_DATETIME,        
        TO_TIMESTAMP(NULL, 'dd-MMM-yy HH:mm:ss') AS ROW_END_DATETIME        
    FROM {env_var}_catalog.bronze_mambu_con.repayment
    """
    )
    
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_loan_transaction", 
    comment="keeps track of all transactions which occur with loan accounts such as state changes, repayments, fees, etc."
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_loan_transaction():

    df = spark.sql(
        f"""
    SELECT
        ENCODEDKEY AS ENCODED_KEY,
        AMOUNT,
        BALANCE,
        BRANCHKEY AS BRANCH_KEY,
        CENTREKEY AS CENTRE_KEY,
        COMMENT,
        DETAILS_ENCODEDKEY_OID AS DETAILS_ENCODEDKEY_OID,
        PARENTACCOUNTKEY AS PARENT_ACCOUNT_KEY,
        TRANSACTIONID AS TRANSACTION_ID,
        TYPE,
        USERKEY AS USER_KEY,
        REVERSALTRANSACTIONKEY AS REVERSAL_TRANSACTION_KEY,
        PRINCIPALAMOUNT AS PRINCIPAL_AMOUNT,
        INTERESTAMOUNT AS INTEREST_AMOUNT,
        FUNDERSINTERESTAMOUNT AS FUNDERS_INTEREST_AMOUNT,
        ORGANIZATIONCOMMISSIONAMOUNT AS ORGANIZATION_COMMISSION_AMOUNT,
        FEESAMOUNT AS FEES_AMOUNT,
        PENALTYAMOUNT AS PENALTY_AMOUNT,
        INDEXINTERESTRATE_ENCODEDKEY_OID AS INDEX_INTEREST_RATE_ENCODED_KEY_OID,
        TAXRATE_ENCODEDKEY_OID AS TAX_RATE_ENCODED_KEY_OID,
        TAXONINTERESTAMOUNT AS TAX_ON_INTEREST_AMOUNT,
        DEFERREDINTERESTAMOUNT  AS DEFERRED_INTEREST_AMOUNT,
        DEFERREDTAXONINTERESTAMOUNT AS DEFERRED_TAX_ON_INTEREST_AMOUNT,
        MIGRATIONEVENTKEY AS MIGRATION_EVENT_KEY,
        TAXONFEESAMOUNT AS TAX_ON_FEES_AMOUNT,
        TAXONPENALTYAMOUNT AS TAX_ON_PENALTY_AMOUNT,
        PRODUCTTYPEKEY AS PRODUCT_TYPE_KEY,
        PARENTLOANTRANSACTIONKEY AS PARENT_LOAN_TRANSACTION_KEY,
        ORIGINALCURRENCYCODE AS ORIGINAL_CURRENCY_CODE,
        ORIGINALAMOUNT AS ORIGINAL_AMOUNT,
        TILLKEY AS TILL_KEY,
        INTERESTRATE AS INTEREST_RATE,
        LOANTRANSACTIONTERMSKEY AS LOAN_TRANSACTION_TERMS_KEY,
        REDRAWBALANCE AS REDRAW_BALANCE,
        PRINCIPALBALANCE AS PRINCIPAL_BALANCE,
        ADVANCEPOSITION AS ADVANCE_POSITION,
        ARREARSPOSITION AS ARREARS_POSITION,
        EXPECTEDPRINCIPALREDRAW AS EXPECTED_PRINCIPAL_REDRAW,
        INTERESTFROMARREARSAMOUNT AS INTEREST_FROM_ARREARS_AMOUNT,
        TAXONINTERESTFROMARREARSAMOUNT AS TAX_ON_INTEREST_FROM_ARREARS_AMOUNT,
        EXTERNALID AS EXTERNAL_ID,
        CAST(CREATIONDATE AS DATE) AS CREATION_DATE,
        CAST(CREATIONDATE AS TIMESTAMP) AS CREATION_DATETIME,
        CAST(ENTRYDATE AS DATE) AS ENTRY_DATE,
        CAST(ENTRYDATE AS TIMESTAMP) AS ENTRY_DATETIME,        
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_mambu_con.loantransaction
    """
    )
    
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.table(
    name="mambu_activity", 
    comment="each action that takes place in the application is followed by an activity that is logged and posted on the dashboard and on the activity feed"
)
@dlt.expect("Check that ENCODED_KEY is not null", "ENCODED_KEY IS NOT NULL")
def mambu_activity():

    df = spark.sql(
        f"""
    SELECT
        ENCODEDKEY AS ENCODED_KEY,
        TRANSACTIONID AS TRANSACTION_ID,
        ASSIGNEDUSERKEY AS ASSIGNED_USER_KEY,
        BRANCHKEY AS BRANCH_KEY,
        CENTREKEY AS CENTRE_KEY,
        CLIENTKEY AS CLIENT_KEY,
        GROUPKEY AS GROUP_KEY,
        LINEOFCREDITKEY AS LINE_OF_CREDIT_KEY,
        LOANACCOUNTKEY AS LOAN_ACCOUNT_KEY,
        LOANPRODUCTKEY AS LOAN_PRODUCT_KEY,
        NOTES,
        SAVINGSACCOUNTKEY AS SAVINGS_ACCOUNT_KEY,
        SAVINGSPRODUCTKEY AS SAVING_PRODUCT_KEY,
        TYPE,
        USERKEY AS USER_KEY, 
        ASSIGNEDCENTREKEY AS ASSIGNED_CENTRE_KEY,        
        TASKKEY AS TASK_KEY,
        GLACCOUNTKEY AS GL_ACCOUNT_KEY,
        GLACCOUNTSCLOSUREKEY AS GL_ACCOUNT_CLOSURE_KEY,
        ENTITYTYPE AS ENTITY_TYPE,
        ENTITYKEY AS ENTITY_KEY,
        PARENT_KEY AS PARENT_KEY,
        ACTIVITYCHANGES_INTEGER_IDX AS ACTIVITY_CHANGES_INTEGER_IDX,
        FIELDCHANGENAME AS FIELD_CHANGE_NAME,    
        CAST(TIMESTAMP AS DATE) AS DATE,
        CAST(TIMESTAMP AS TIMESTAMP) AS DATETIME,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        TO_TIMESTAMP(MDP_LOAD_DATETIME, 'dd-MMM-yy HH:mm:ss') AS MDP_LOAD_DATETIME,
        __START_AT AS ROW_START_DATETIME,
        __END_AT AS ROW_END_DATETIME
    FROM {env_var}_catalog.bronze_mambu_con.activity
    """
    )
    
    windowSpec = dlf.get_window_spec("ENCODED_KEY","MDP_LOAD_DATETIME")   
    df = dlf.get_row_number(df, windowSpec)   

    return df

# COMMAND ----------

@dlt.materialized_view(
    name="mview_mambu_transformed_fields", 
    comment="each action that takes place in the application is followed by an activity that is logged and posted on the dashboard and on the activity feed"
)
@dlt.expect("Check that LINE_OF_CREDIT_KEY is not null", "LINE_OF_CREDIT_KEY IS NOT NULL")
def mview_mambu_transformed_fields():

    df = spark.sql(
        f"""
    SELECT
        CAST(mla.ID AS STRING) AS PK_VW_MAMBU_TRANSFORMED_FIELDS,
        CAST(xxhash64(mla.ID) AS BIGINT) AS FK_FACT_MORTGAGE_PART,
        CAST(mloc.ID AS STRING) AS BK_MORTGAGE_ACCOUNT,  
        CAST(cfvp.PRODUCT_ID_LA AS STRING) AS BK_MORTGAGE_PRODUCT,   
        mloc.ENCODED_KEY AS LINE_OF_CREDIT_KEY,
        CASE 
            WHEN lps.RANK1_END_DATE IS NOT NULL THEN lps.RANK1_END_DATE
            ELSE ADD_MONTHS(TRY_CAST(cfvp.ORIGINAL_START_DATE_LA AS DATE), lps.RANK1_FOR_MONTHS) 
        END AS RANK1_BALLOON_DATE,
        CASE 
            WHEN lps.RANK2_END_DATE IS NOT NULL THEN lps.RANK2_END_DATE
            ELSE ADD_MONTHS(RANK1_BALLOON_DATE, lps.RANK2_FOR_MONTHS)
        END AS RANK2_BALLOON_DATE,
        CASE
            WHEN lps.RANK3_END_DATE IS NOT NULL THEN lps.RANK3_END_DATE
            ELSE ADD_MONTHS(RANK2_BALLOON_DATE, lps.RANK3_FOR_MONTHS)
        END AS RANK3_BALLOON_DATE,
        CASE
            WHEN lps.RANK4_END_DATE IS NOT NULL THEN lps.RANK4_END_DATE
            ELSE ADD_MONTHS(RANK3_BALLOON_DATE, lps.RANK4_FOR_MONTHS)
        END AS RANK4_BALLOON_DATE,
        CASE
            WHEN lps.RANK5_END_DATE IS NOT NULL THEN lps.RANK5_END_DATE
            ELSE ADD_MONTHS(RANK4_BALLOON_DATE, lps.RANK5_FOR_MONTHS)
        END AS RANK5_BALLOON_DATE,
        CASE
            WHEN RANK1_BALLOON_DATE >= '{RUN_DATE}' THEN RANK1_BALLOON_DATE 
            WHEN RANK2_BALLOON_DATE >= '{RUN_DATE}' THEN RANK2_BALLOON_DATE
            WHEN RANK3_BALLOON_DATE >= '{RUN_DATE}' THEN RANK3_BALLOON_DATE
            WHEN RANK4_BALLOON_DATE >= '{RUN_DATE}' THEN RANK4_BALLOON_DATE
            WHEN RANK5_BALLOON_DATE >= '{RUN_DATE}' THEN RANK5_BALLOON_DATE
            ELSE '1900-01-01'
        END AS FK_NEXT_BALLOON_DATE,
        CASE
            WHEN RANK1_BALLOON_DATE >= '{RUN_DATE}' THEN 1
            WHEN RANK2_BALLOON_DATE >= '{RUN_DATE}' THEN 2
            WHEN RANK3_BALLOON_DATE >= '{RUN_DATE}' THEN 3
            WHEN RANK4_BALLOON_DATE >= '{RUN_DATE}' THEN 4
            WHEN RANK5_BALLOON_DATE >= '{RUN_DATE}' THEN 5
            ELSE 1
        END AS CURRENT_RANK,
        CAST(
            CASE
                WHEN CURRENT_RANK = 1 AND lps.RANK1_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 2 AND lps.RANK2_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 3 AND lps.RANK3_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 4 AND lps.RANK4_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                WHEN CURRENT_RANK = 5 AND lps.RANK5_TYPE = 'Fixed' THEN lps.BENCHMARK_RATE
                ELSE (0.05 + (brl.RATE/100))
            END AS DECIMAL(38,6)) AS BENCHMARK_RATE,
        CASE
            WHEN CURRENT_RANK = 1 THEN lps.RANK1_TYPE
            WHEN CURRENT_RANK = 2 THEN lps.RANK2_TYPE
            WHEN CURRENT_RANK = 3 THEN lps.RANK3_TYPE
            WHEN CURRENT_RANK = 4 THEN lps.RANK4_TYPE
            WHEN CURRENT_RANK = 5 THEN lps.RANK5_TYPE
        END AS TYPE,
        CASE
            WHEN CURRENT_RANK = 1 THEN COALESCE(lps.LAUNCH_DATE, CAST('1900-01-01' AS DATE))
            WHEN CURRENT_RANK = 2 THEN lps.RANK1_END_DATE
            WHEN CURRENT_RANK = 3 THEN lps.RANK2_END_DATE
            WHEN CURRENT_RANK = 4 THEN lps.RANK3_END_DATE
            WHEN CURRENT_RANK = 5 THEN lps.RANK4_END_DATE
        END AS RANK_START_DATE,
        CASE
            WHEN CURRENT_RANK = 1 THEN lps.RANK1_END_DATE
            WHEN CURRENT_RANK = 2 THEN lps.RANK2_END_DATE
            WHEN CURRENT_RANK = 3 THEN lps.RANK3_END_DATE
            WHEN CURRENT_RANK = 4 THEN lps.RANK4_END_DATE
            WHEN CURRENT_RANK = 5 THEN lps.RANK5_END_DATE
        END AS RANK_END_DATE,
        mla.ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.mambu_loan_account AS mla
    INNER JOIN {env_var}_catalog.silver_con.mambu_custom_field_value_pivot AS cfvp
    ON mla.ENCODED_KEY = cfvp.PARENT_KEY
    AND mla.ROW_IS_CURRENT = 1
    AND cfvp.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.silver_int.lps_product AS lps
    ON cfvp.PRODUCT_ID_LA = lps.PRODUCT_CODE
    AND cfvp.ROW_IS_CURRENT = 1
    AND lps.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.silver_con.mambu_line_of_credit AS mloc
    ON mla.LINE_OF_CREDIT_KEY = mloc.ENCODED_KEY
    AND mloc.ROW_IS_CURRENT = 1
    LEFT JOIN {env_var}_catalog.silver_int.reference_data_base_rate_loading AS brl
    ON brl.END_DATE >= '{RUN_DATE}'
    AND brl.ROW_IS_CURRENT = 1
    """
    )
    
    # windowSpec = dlf.get_window_spec("LINE_OF_CREDIT_KEY","ROW_START_DATETIME")   
    # df = dlf.get_row_number(df, windowSpec)   

    return df
