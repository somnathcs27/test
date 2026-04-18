# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr, lead, to_timestamp, when, last, max
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf
from mdp_databricks_common.feature_toggle import feature_toggle as ft

# Import common ETL and metadata functions
import sys

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

def add_housekeeping_columns(df, window_spec_partition_key):

    # Define the ascending window specification
    windowSpecAsc = Window.partitionBy(window_spec_partition_key).orderBy(
        col("ROW_START_DATETIME").asc(),
        col("MDP_LOAD_ID").asc()
    )

    # Define the descending window specification
    windowSpecDesc = Window.partitionBy(window_spec_partition_key).orderBy(
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

# COMMAND ----------

@dlt.table(
    name="dataverse_contact",
    comment="This table stores the customer management contact data from dataverse"
)
@dlt.expect_all_or_drop({
    "valid_customer_id": "CUSTOMER_ID IS NOT NULL"
})
def dataverse_contact():

    df = spark.sql(
        f"""
    SELECT
        Id AS CUSTOMER_ID,  
        CAST(lbs_customernumber AS INT) AS CUSTOMER_NUMBER,
        CAST(gendercode AS STRING) AS GENDER,
        CAST(lbs_title AS STRING) AS TITLE,
        INITCAP(firstname) AS FORENAMES,
        lbs_initial AS INITIALS,
        INITCAP(lastname) AS SURNAME,
        birthdate AS DATE_OF_BIRTH,
        CASE 
           WHEN lbs_customerage LIKE '%months%' THEN 0
           ELSE TRY_CAST(REGEXP_EXTRACT(lbs_customerage, '[0-9]+', 0) AS INTEGER)
        END AS AGE,
        lbs_ninumber AS NI_NUMBER,
        address1_name AS BUILDING_NAME,
        lbs_address1_buildingnumber AS BUILDING_NUMBER,
        address1_line1 AS STREET_NAME,
        address1_city AS CITY,
        address1_county AS COUNTY,
        lbs_address1_country AS COUNTRY,
        address1_stateorprovince AS REGION,
        UPPER(address1_postalcode) AS POSTCODE,
        emailaddress1 AS EMAIL_ADDRESS,
        CAST(preferredcontactmethodcode AS STRING) AS PREFERRED_CONTACT_METHOD,
        telephone2 AS TELEPHONE_HOME,
        mobilephone AS TELEPHONE_MOBILE,
        telephone1 AS TELEPHONE_WORK,
        CAST(customertypecode AS STRING) AS CUSTOMER_TYPE_CODE,
        lbs_customerstartdate AS CUSTOMER_START_DATE,
        lbs_customerenddate AS CUSTOMER_END_DATE,
        statecode AS STATE_CODE,
        statuscode AS STATUS_CODE,
        accountrolecode AS ACCOUNT_ROLE_CODE,
        address1_addresstypecode AS ADDRESS1_ADDRESS_TYPE_CODE,
        address1_freighttermscode AS ADDRESS1_FREIGHT_TERMS_CODE,
        address1_shippingmethodcode AS ADDRESS1_SHIPPING_METHOD_CODE,
        address2_addresstypecode AS ADDRESS2_ADDRESS_TYPE_CODE,
        address2_freighttermscode AS ADDRESS2_FREIGHT_TERMS_CODE,
        address2_shippingmethodcode AS ADDRESS2_SHIPPING_METHOD_CODE,
        address3_addresstypecode AS ADDRESS3_ADDRESS_TYPE_CODE,
        address3_freighttermscode AS ADDRESS3_FREIGHT_TERMS_CODE,
        address3_shippingmethodcode AS ADDRESS3_SHIPPING_METHOD_CODE,
        customersizecode AS CUSTOMER_SIZE_CODE,
        educationcode AS EDUCATION_CODE,
        familystatuscode AS FAMILY_STATUS_CODE,
        haschildrencode AS HAS_CHILDREN_CODE,
        lbs_address1addresstype AS LBS_ADDRESS1_ADDRESS_TYPE,
        lbs_address2addresstype AS LBS_ADDRESS2_ADDRESS_TYPE,
        lbs_electronicidentificationstatus AS LBS_ELECTRONIC_IDENTIFICATION_STATUS,
        lbs_pronoun AS LBS_PRONOUN,
        lbs_reasonforchange AS LBS_REASON_FOR_CHANGE,
        lbs_typeofchange AS LBS_TYPE_OF_CHANGE,
        lbs_typeofevidence AS LBS_TYPE_OF_EVIDENCE,
        leadsourcecode AS LEAD_SOURCE_CODE,
        msdyn_decisioninfluencetag AS MSDYN_DECISION_INFLUENCE_TAG,
        msdyn_orgchangestatus AS MSDYN_ORG_CHANGE_STATUS,
        mspp_userpreferredlcid AS MSPP_USER_PREFERRED_LCID,
        paymenttermscode AS PAYMENT_TERMS_CODE,
        preferredappointmentdaycode AS PREFERRED_APPOINTMENT_DAY_CODE,
        preferredappointmenttimecode AS PREFERRED_APPOINTMENT_TIME_CODE,
        shippingmethodcode AS SHIPPING_METHOD_CODE,
        territorycode AS TERRITORY_CODE,
        lbs_nationality AS LBS_NATIONALITY,
        creditonhold AS CREATION_ON_HOLD,
        donotbulkemail AS DO_NOT_BULK_EMAIL,
        donotbulkpostalmail AS DO_NOT_BULK_POSTAL_MAIL,
        donotemail AS DO_NOT_EMAIL,
        donotfax AS DO_NOT_FAX,
        donotphone AS DO_NOT_PHONE,
        donotpostalmail AS DO_NOT_POSTAL_MAIL,
        donotsendmm AS DO_NOT_SEND_MM,
        followemail AS FOLLOW_EMAIL,
        isautocreate AS IS_AUTO_CREATE,
        isbackofficecustomer AS IS_BACK_OFFICE_CUSTOMER,
        isprivate AS IS_PRIVATE,
        lbs_correspondenceaddresssameascurrentresidential AS LBS_CORRESPONDENCE_ADDRESS_SAME_AS_CURRENT_RESIDENTIAL,
        lbs_lbscolleague AS LBS_LBSCOLLEAGUE,
        marketingonly AS MARKETING_ONLY,
        merged AS MERGED,
        msdyn_disablewebtracking AS MSDYN_DISABLE_WEB_TRACKING,
        msdyn_gdproptout AS MSDYN_GDPR_OPT_OUT,
        msdyn_isassistantinorgchart AS MSDYN_IS_ASSISTANT_IN_ORG_CHART,
        msdyn_isminor AS MSDYN_IS_MINOR,
        msdyn_isminorwithparentalconsent AS MSDYN_IS_MINOR_WITH_PARENTAL_CONSENT,
        participatesinworkflow AS PARTICIPATES_IN_WORKFLOW,
        accountid AS ACCOUNT_ID,
        accountid_entitytype AS ACCOUNT_ID_ENTITY_TYPE,
        createdby AS CREATED_BY,
        createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
        createdbyexternalparty AS CREATED_BY_EXTERNAL_PARTY,
        createdbyexternalparty_entitytype AS CREATED_BY_EXTERNAL_PARTY_ENTITY_TYPE,
        createdonbehalfby AS CREATED_ON_BEHALF_BY,
        createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
        defaultpricelevelid AS DEFAULT_PRICE_LEVEL_ID,
        defaultpricelevelid_entitytype AS DEFAULT_PRICE_LEVEL_ID_ENTITY_TYPE,
        lbs_address1_country AS LBS_ADDRESS1_COUNTRY,
        lbs_address1_country_entitytype AS LBS_ADDRESS1_COUNTRY_ENTITY_TYPE,
        lbs_address2_country AS LBS_ADDRESS2_COUNTRY,
        lbs_address2_country_entitytype AS LBS_ADDDRESS2_COUNTRY_ENTITY_TYPE,
        lbs_countryforhometelephonenumber AS LBS_COUNTRY_FOR_HOME_TELEPHONE_NUMBER,
        lbs_countryforhometelephonenumber_entitytype AS LBS_COUNTRY_FOR_HOME_TELEPHONE_NUMBER_ENTITY_TYPE,
        lbs_countryformobiletelephonenumber AS LBS_COUNTRY_FOR_MOBILE_TELEPHONE_NUMBER,
        lbs_countryformobiletelephonenumber_entitytype AS LBS_COUNTRY_FOR_MOBILE_TELEPHONE_NUMBER_ENTITY_TYPE,
        lbs_countryforworktelephonenumber AS LBS_COUNTRY_FOR_WORK_TELEPHONE_NUMBER,
        lbs_countryforworktelephonenumber_entitytype AS LBS_COUNTRY_FOR_WORK_TELEPHONE_NUMBER_ENTITY_TYPE,
        lbs_countryofbirthid AS LBS_COUNTRY_OF_BIRTH_ID,
        lbs_countryofbirthid_entitytype AS LBS_COUNTRY_OF_BIRTH_ID_ENTITY_TYPE,
        lbs_currentoccupation AS LBS_CURRENT_OCCUPATION,
        lbs_currentoccupation_entitytype AS LBS_CURRENT_OCCUPATION_ENTITY_TYPE,
        lbs_employmentstatusid AS LBS_EMPLOYMENT_STATUS_ID,
        lbs_employmentstatusid_entitytype AS LBS_EMPLOYMENT_STATUS_ID_ENTITY_TYPE,
        masterid AS MASTER_ID,
        masterid_entitytype AS MASTER_ID_ENTITY_TYPE,
        modifiedby AS MODIFIED_BY,
        modifiedby_entitytype AS MOFIFIED_BY_ENTITY_TYPE,
        modifiedbyexternalparty AS MODIFIED_BY_EXTERNAL_PARTY,
        modifiedbyexternalparty_entitytype AS MODIFIED_BY_EXTERNAL_PARTY_ENTITY_TYPE,
        modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
        modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
        msa_managingpartnerid AS MSA_MANAGING_PARTNER_ID,
        msa_managingpartnerid_entitytype AS MSA_MANAGING_PARTNER_ID_ENTITY_TYPE,
        msdyn_contactkpiid AS MSDYN_CONTACT_KPI_ID,
        msdyn_contactkpiid_entitytype AS MSDYN_CONTACT_KPI_ID_ENTITY_TYPE,
        msdyn_segmentid AS MSDYN_SEGMENT_ID,
        msdyn_segmentid_entitytype AS MSDYN_SEGMENT_ID_ENTITY_TYPE,
        originatingleadid AS ORIGINATING_LEAD_ID,
        originatingleadid_entitytype AS ORGINATING_LEAD_ID_ENTITY_TYPE,
        owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
        owningteam AS OWNING_TEAM,
        owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
        owninguser AS OWNING_USER,
        owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
        parentcontactid AS PARENT_CONTACT_ID,
        parentcontactid_entitytype AS PARENT_CONTACT_ID_ENTITY_TYPE,
        preferredequipmentid AS PREFERRED_EQUIPMENT_ID,
        preferredequipmentid_entitytype AS PREFFERRRED_EQUIPMENT_ID_ENTITY_TYPE,
        preferredserviceid AS PREFFERRRED_SERVICE_ID,
        preferredserviceid_entitytype AS PREFFERRRED_SERVICE_ID_ENTITY_TYPE,
        preferredsystemuserid AS PREFERRRED_SYSTEM_USER_ID,
        preferredsystemuserid_entitytype AS PREFFERRRED_SYSTEM_USER_ID_ENTITY_TYPE,
        slaid AS SLA_ID,
        slaid_entitytype AS SLA_ID_ENTITY_TYPE,
        slainvokedid AS SLA_INVOKED_ID,
        slainvokedid_entitytype AS SLA_INVOKED_ID_ENTITY_TYPE,
        transactioncurrencyid AS TRANSACTION_CURRENCY_ID,
        transactioncurrencyid_entitytype AS TRANSACTION_CURRENCY_ID_ENTITY_TYPE,
        ownerid AS OWNER_ID,
        ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
        parentcustomerid AS PARENT_CUSTOMER_ID,
        parentcustomerid_entitytype AS PARENT_CUSTOMER_ID_ENTITY_TYPE,
        aging30 AS AGING_30,
        aging30_base AS AGING_30_BASE,
        aging60 AS AGING_60,
        aging60_base AS AGING_60_BASE,
        aging90 AS AGING_90,
        aging90_base AS AGING_90_BASE,
        annualincome AS ANNUAL_INCOME,
        annualincome_base AS ANNUAL_INCOME_BASE,
        creditlimit AS CREDIT_LIMIT,
        creditlimit_base AS CREDIT_LIMIT_BASE,
        accountidname AS ACCOUNT_ID_NAME,
        accountidyominame AS ACCOUNT_ID_YOMINAME,
        address1_addressid AS ADDRESS1_ADDRESS_ID,
        address1_composite AS ADDRESS1_COMPOSITE,
        address1_country AS ADDRESS1_COUNTRY,
        address1_fax AS ADDRESS1_FAX,
        address1_latitude AS ADDRESS1_LATITUDE,
        address1_line2 AS ADDRESS1_LINE2,
        address1_line3 AS ADDRESS1_LINE3,
        address1_longitude AS ADDRESS1_LONGITUDE,
        address1_postofficebox AS ADDRESS1_POST_OFFICE_BOX,
        address1_primarycontactname AS ADDRESS1_PRIMARY_CONTACT_NAME,
        address1_telephone1 AS ADDRESS1_TELEPHONE1,
        address1_telephone2 AS ADDRESS1_TELEPHONE2,
        address1_telephone3 AS ADDRESS1_TELEPHONE3,
        address1_upszone AS ADDRESS1_UPS_ZONE,
        address1_utcoffset AS ADDRESS1_UTCOFFSET,
        address2_addressid AS ADDRESS2_ADDRESS_ID,
        address2_city AS ADDRESS2_CITY,
        address2_composite AS ADDRESS2_COMPOSITE,
        address2_country AS ADDRESS2_COUNTRY,
        address2_county AS ADDRESS2_COUNTY,
        address2_fax AS ADDRESS2_FAX,
        address2_latitude AS ADDRESS2_LATITUDE,
        address2_line1 AS ADDRESS2_LINE1,
        address2_line2 AS ADDRESS2_LINE2,
        address2_line3 AS ADDRESS2_LINE3,
        address2_longitude AS ADDRESS2_LONGITUDE,
        address2_name AS ADDRESS2_NAME,
        address2_postalcode AS ADDRESS2_POSTALCODE,
        address2_postofficebox AS ADDRESS2_POST_OFFICE_BOX,
        address2_primarycontactname AS ADDRESS2_PRIMARY_CONTACT_NAME,
        address2_stateorprovince AS ADDRESS2_STATE_OR_PROVINCE,
        address2_telephone1 AS ADDRESS2_TELEPHONE1,
        address2_telephone2 AS ADDRESS2_TELEPHONE2,
        address2_telephone3 AS ADDRESS2_TELEPHONE3,
        address2_upszone AS ADDRESS2_UPS_ZONE,
        address2_utcoffset AS ADDRESS2_UTCOFFSET,
        address3_addressid AS ADDRESS3_ADDRESS_ID,
        address3_city AS ADDRESS3_CITY,
        address3_composite AS ADDRESS3_COMPOSITE,
        address3_country AS ADDRESS3_COUNTRY,
        address3_county AS ADDRESS3_COUNTY,
        address3_fax AS ADDRESS3_FAX,
        address3_latitude AS ADDRESS3_LATITUDE,
        address3_line1 AS ADDRESS3_LINE1,
        address3_line2 AS ADDRESS3_LINE2,
        address3_line3 AS ADDRESS3_LINE3,
        address3_longitude AS ADDRESS3_LONGITUDE,
        address3_name AS ADDRESS3_NAME,
        address3_postalcode AS ADDRESS3_POSTALCODE,
        address3_postofficebox AS ADDRESS3_POST_OFFICE_BOX,
        address3_primarycontactname AS ADDRESS3_PRIMARY_CONTACT_NAME,
        address3_stateorprovince AS ADDRESS3_STATE_OR_PROVINCE,
        address3_telephone1 AS ADDRESS3_TELEPHONE1,
        address3_telephone2 AS ADDRESS3_TELEPHONE2,
        address3_telephone3 AS ADDRESS3_TELEPHONE3,
        address3_upszone AS ADDRESS3_UPS_ZONE,
        address3_utcoffset AS ADDRESS3_UTCOFFSET,
        anniversary AS ANNIVERSARY,
        assistantname AS ASSISTANT_NAME,
        assistantphone AS ASSISTANT_PHONE,
        business2 AS BUSINESS2,
        businesscard AS BUSINESS_CARD,
        businesscardattributes AS BUSINESS_CARD_ATTRIBUTES,
        callback AS CALL_BACK,
        childrensnames AS CHILDREN_NAMES,
        company AS COMPANY,
        contactid AS CONTACT_ID,
        createdbyexternalpartyname AS CREATED_BY_EXTERNAL_PARTY_NAME,
        createdbyexternalpartyyominame AS CREATED_BY_EXTERNAL_PARTY_YOMINAME,
        createdbyname AS CREATED_BY_NAME,
        createdbyyominame AS CREATED_BY_YOMINAME,
        createdon AS CRATEDED_ON,
        createdonbehalfbyname AS CREATED_BY_ON_BEHALF_BY_NAME,
        createdonbehalfbyyominame AS CREATED_BY_ON_BEHALF_BY_YOMINAME, 
        defaultpricelevelidname AS DEFAULT_PRICE_LEVEL_ID_NAME,
        department AS DEPARTMENT,
        description AS DESCRIPTION,
        emailaddress2 AS EMAIL_ADDRESS2,
        emailaddress3 AS EMAIL_ADDRESS3,
        employeeid AS EMPLOYEE_ID,
        entityimage AS ENTITY_IMAGE,
        entityimage_timestamp AS ENTITY_IMAGE_TIMESTAMP,
        entityimage_url AS ENTITY_IMAGE_URL,
        entityimageid AS ENTITY_IMAGE_ID,
        exchangerate AS EXCHANGE_RATE,
        externaluseridentifier AS EXTERNAL_USER_IDENTIFIER,
        fax AS FAX,
        ftpsiteurl AS FTP_SITE_URL,
        fullname AS FULL_NAME,
        governmentid AS GOVERNMENT_ID,
        home2 AS HOME2,
        importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
        jobtitle AS JOB_TITLE,
        lastonholdtime AS LAST_ON_HOLD_TIME,
        lastusedincampaign AS LAST_USED_IN_CAMPAIGN,
        lbs_address1_bfpo_bfponumber AS LBS_ADDRESS1_BFPO_BFPO_NUMBER,
        lbs_address1_bfpo_careofsurname AS LBS_ADDRESS1_BFPO_CAREO_SURNAME,
        lbs_address1_bfpo_operationname AS LBS_ADDRESS1_BFPO_OPERATION_NAME,
        lbs_address1_bfpo_rank AS LBS_ADDRESS1_BFPO_RANK,
        lbs_address1_bfpo_servicenumber AS LBS_ADDRESS1_BFPO_SERVICE_NUMBER,
        lbs_address1_bfpo_unit AS LBS_ADDRESS1_BFPO_UNIT,
        lbs_address1_buildingnumber AS LBS_ADDRESS1_BUILDING_NUMBER,
        lbs_address1_countryname AS LBS_ADDRESS1_COUNTRY_NAME,
        lbs_address1_flat AS LBS_ADDRESS1_FLAT,
        lbs_address1_overseas_line1 AS LBS_ADDRESS1_OVERSEAS_LINE1,
        lbs_address1_overseas_line2 AS LBS_ADDRESS1_OVERSEAS_LINE2,
        lbs_address1_overseas_line3 AS LBS_ADDRESS1_OVERSEAS_LINE3,
        lbs_address1_overseas_line4 AS LBS_ADDRESS1_OVERSEAS_LINE4,
        lbs_address2_bfpo_bfponumber AS LBS_ADDRESS2_BFPO_BFPO_NUMBER,
        lbs_address2_bfpo_careofsurname AS LBS_ADDRESS2_BFPO_CARE_OF_SURNAME,
        lbs_address2_bfpo_operationname AS LBS_ADDRESS2_BFPO_OPERATION_NAME,
        lbs_address2_bfpo_rank AS LBS_ADDRESS2_BFPO_RANK,
        lbs_address2_bfpo_servicenumber AS LBS_ADDRESS2_BFPO_SERVICE_NUMBER,
        lbs_address2_bfpo_unit AS LBS_ADDRESS2_BFPO_UNIT,
        lbs_address2_buildingnumber AS LBS_ADDRESS2_BUILDING_NUMBER,
        lbs_address2_countryname AS LBS_ADDRESS2_COUNTRY_NAME,
        lbs_address2_flat AS LBS_ADDRESS2_FLAT,
        lbs_address2_overseas_line1 AS LBS_ADDRESS2_OVERSEAS_LINE1,
        lbs_address2_overseas_line2 AS LBS_ADDRESS2_OVERSEAS_LINE2,
        lbs_address2_overseas_line3 AS LBS_ADDRESS2_OVERSEAS_LINE3,
        lbs_address2_overseas_line4 AS LBS_ADDRESS2_OVERSEAS_LINE4,
        lbs_correspondenceaddressenddate AS LBS_CORRESPONDENCE_ADDRESS_END_DATE,
        lbs_correspondenceaddressstartdate AS LBS_CORRESPONDENCE_ADDRESS_START_DATE,
        lbs_countryforhometelephonenumbername AS LBS_COUNTRY_FOR_HOME_TELEPHONE_NUMBER_NAME,
        lbs_countryformobiletelephonenumbername AS LBS_COUNTRY_FOR_MOBILE_TELEPHONE_NUMBER_NAME,
        lbs_countryforworktelephonenumbername AS LBS_COUNTRY_FOR_WORK_TELEPHONE_NUMBER_NAME,
        lbs_countryofbirthidname AS LBS_COUNTRY_OF_BIRTH_ID_NAME,
        lbs_currentoccupationname AS LBS_CURRENT_OCCUPATION_NAME,
        lbs_dialingcodehome AS LBS_DIALING_CODE_HOME,
        lbs_dialingcodemobile AS LBS_DIALING_CODE_MOBILE,
        lbs_dialingcodework AS LBS_DIALING_CODE_WORK,
        lbs_electronicidentificationdate AS LBS_ELECTRONIC_IDENTIFICATION_DATE,
        lbs_employmentstatusidname AS LBS_EMPLOYMENT_STATUS_ID_NAME,
        lbs_extensioncode AS LBS_EXTENSION_CODE,
        lbs_otherevidence AS LBS_OTHER_EVIDENCE,
        lbs_othernames AS LBS_OTHER_NAMES,
        lbs_reasonforamendment AS LBS_REASON_FOR_AMENDMENT,
        lbs_reasonholder AS LBS_REASON_HOLDER,
        lbs_residentialaddressenddate AS LBS_RESIDENTIAL_ADDRESS_END_DATE,
        lbs_residentialaddressstartdate AS LBS_RESIDENTIAL_ADDRESS_START_DATE,
        lbs_townofbirth AS LBS_TOWN_OF_BIRTH,
        lbs_yearsuntilretirement AS LBS_YEARS_UNTIL_RETIREMENT,
        managername AS MANAGER_NAME,
        managerphone AS MANAGER_PHONE,
        mastercontactidname AS MASTER_CONTACT_ID_NAME,
        mastercontactidyominame AS MASTER_CONTACT_ID_YOMINAME,
        INITCAP(middlename) AS MIDDLE_NAME,
        mobilephone AS MOBILE_PHONE,
        modifiedbyexternalpartyname AS MODFIED_BY_EXTERNAL_PARTY_NAME,
        modifiedbyexternalpartyyominame AS MODIFIED_BY_EXTERNAL_PARTY_YOMINAME,
        modifiedbyname AS MODIFIFIED_BY_NAME,
        modifiedbyyominame AS MODIFFIED_BY_YOMINAME,
        modifiedon AS MODIFIED_ON,
        modifiedonbehalfbyname AS MODIFIED_BY_ON_BEHALF_BY_NAME,
        modifiedonbehalfbyyominame AS MODIFIED_BY_ON_BEHALF_BY_YOMINAME,
        msa_managingpartneridname AS MSA_MANAGING_PARTNER_ID_NAME,
        msa_managingpartneridyominame AS MSA_MANAGING_PARTNER_ID_YOMINAME,
        msdyn_contactkpiidname AS MSDYN_CONTACT_KPI_ID_NAME,
        msdyn_portaltermsagreementdate AS MSDYN_PORTAL_TERMS_AGREEMENT_DATE,
        msdyn_primarytimezone AS MSDYN_PRIMARY_TIME_ZONE,
        msdyn_segmentidname AS MSDYN_SEGMENT_ID_NAME,
        nickname AS NICKNAME,
        numberofchildren AS NUMBER_OF_CHILDREN,
        onholdtime AS ON_HOLD_TIME,
        originatingleadidname AS ORIGINATING_LEAD_ID_NAME,
        originatingleadidyominame AS ORIGINATING_LEAD_ID_YOMINAME,
        overriddencreatedon AS OVERRIDDEN_CREATED_ON,
        owneridname AS OWNER_ID_NAME,
        owneridtype AS OWNER_ID_TYPE,
        owneridyominame AS OWNER_ID_YOMINAME,
        owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
        pager AS PAGER,
        parentcontactidname AS PARENT_CONTACT_ID_NAME,
        parentcontactidyominame AS PARENT_CONTACT_ID_YOMINAME,
        parentcustomeridname AS PARENT_CUSTOMER_ID_NAME,
        parentcustomeridtype AS PARENT_CUSTOMER_ID_TYPE,
        parentcustomeridyominame AS PARARENT_CUSTOMER_ID_YOMINAME,
        preferredequipmentidname AS PREFERRED_EQUIPMENT_ID_NAME,
        preferredserviceidname AS PREFERRED_SERVICE_ID_NAME,
        preferredsystemuseridname AS PREFERRED_SYSTEM_USER_ID_NAME,
        preferredsystemuseridyominame AS PREFERRED_SYSTEM_USER_ID_YOMINAME,
        processid AS PROCESS_ID,
        salutation AS SALUTATION,
        slainvokedidname AS SLA_INVOKED_ID_NAME,
        slaname AS SLA_NAME,
        spousesname AS SPOUSES_NAME,
        stageid AS STAGE_ID,
        subscriptionid AS SUBSCRIPTION_ID,
        suffix AS SUFFIX,
        teamsfollowed AS TEAM_FOLLOWED,
        telephone1 AS TELEPHONE1,
        telephone2 AS TELEPHONE2,
        telephone3 AS TELEPHONE3,
        timespentbymeonemailandmeetings AS TIMESPENT_BY_ME_ON_EMAIL_AND_MEETINGS,
        timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
        transactioncurrencyidname AS TRANSACTION_CURRENCY_ID_NAME,
        traversedpath AS TRAVERSED_PATH,
        utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
        versionnumber AS VERSION_NUMBER,
        websiteurl AS WEBSITE_URL,
        yomifirstname AS YOMI_FIRST_NAME,
        yomifullname AS YOMI_FULL_NAME,
        yomilastname AS YOMI_LAST_NAME,
        yomimiddlename AS YOMI_MIDDLE_NAME,
        IsDelete AS IS_DELETE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME
    FROM {env_var}_catalog.bronze_dataverse_con.contact
    """
    )

    df = add_housekeeping_columns(df, "CUSTOMER_ID")

    return df


# COMMAND ----------

@dlt.table(
    name="dataverse_address", 
    comment="This table stores the customer management address data from dataverse" 
)
@dlt.expect_all_or_drop({
    "valid_customer_id": "ADDRESS_ID IS NOT NULL"
}) 
def dataverse_address():

    df = spark.sql(
        f"""
    SELECT   
        Id AS ADDRESS_ID,
        statecode AS STATE_CODE,
        statuscode AS STATUS_CODE,
        lbs_address1_addressuse AS LBS_ADDRESS1_ADDRESS_USE,
        CAST(lbs_address1_typecode AS STRING) AS POSTCODE_ADDRESS_FILE_STATUS, 
        createdby AS CREATED_BY,
        createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
        createdonbehalfby AS CREATED_ON_BEHALF_BY,
        createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
        lbs_address1_country AS LBS_ADDRESS1_COUNTRY,
        lbs_address1_country_entitytype AS LBS_ADDRESS1_COUNTRY_ENTITY_TYPE,
        modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
        modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
        modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
        owningbusinessunit AS OWNING_BUSINESS_UNIT,
        owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
        owningteam AS OWNING_TEAM,
        owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
        owninguser AS OWNING_USER,
        owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
        ownerid AS OWNING_ID,
        ownerid_entitytype AS OWNING_ID_ENTITY_TYPE,
        lbs_customerid AS LBS_CUSTOMER_ID,
        lbs_customerid_entitytype AS LBS_CUSTOMER_ID_ENTITY_TYPE,
        createdbyname AS CREATED_BY_NAME,
        createdbyyominame AS CREATED_BY_YOMINAME,
        createdon AS CREATED_ON,
        createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
        createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMINAME,
        importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
        lbs_address1_bfpo_bfponumber AS LBS_ADDRESS1_BFPO_BFPO_NUMBER,
        lbs_address1_bfpo_careofsurname AS LBS_ADDRESS1_BFPO_CARE_OF_SURNAME,
        INITCAP(lbs_address1_bfpo_operationname) AS LBS_ADDRESS1_BFPO_OPERATION_NAME,
        INITCAP(lbs_address1_bfpo_rank) AS LBS_ADDRESS1_BFPO_RANK,
        INITCAP(lbs_address1_bfpo_servicenumber) AS LBS_ADDRESS1_BFPO_SERVICE_NUMBER,
        INITCAP(lbs_address1_bfpo_unit) AS LBS_ADDRESS1_BFPO_UNIT,
        lbs_address1_buildingname AS LBS_ADDRESS1_BUILDING_NAME,
        lbs_address1_buildingnumber AS LBS_ADDRESS1_BUILDING_NUMBER,
        INITCAP(lbs_address1_city) AS LBS_ADDRESS1_CITY,
        lbs_address1_countryid AS LBS_ADDRESS1_COUNTRY_ID,
        lbs_address1_countryname AS LBS_ADDRESS1_COUNTRY_NAME,
        INITCAP(lbs_address1_county) AS LBS_ADDRESS1_COUNTY,
        lbs_address1_enddate AS LBS_ADDRESS1_END_DATE,
        lbs_address1_flat AS LBS_ADDRESS1_FLAT,
        INITCAP(lbs_address1_line1) AS LBS_ADDRESS1_LINE1,
        INITCAP(lbs_address1_overseas_line1) AS LBS_ADDRESS1_OVERSEAS_LINE1,
        INITCAP(lbs_address1_overseas_line2) AS LBS_ADDRESS1_OVERSEAS_LINE2,
        INITCAP(lbs_address1_overseas_line3) AS LBS_ADDRESS1_OVERSEAS_LINE3,
        INITCAP(lbs_address1_overseas_line4) AS LBS_ADDRESS1_OVERSEAS_LINE4,
        lbs_address1_postalcode AS LBS_ADDRESS1_POSTAL_CODE,
        lbs_address1_startdate AS LBS_ADDRESS1_START_DATE,
        INITCAP(lbs_address1_stateorprovince) AS LBS_ADDRESS1_STATE_OR_PROVINCE,
        lbs_addressid AS LBS_ADDRESS_ID,
        lbs_customerididtype AS LBS_CUSTOMER_ID_ID_TYPE,
        lbs_customeridname AS LBS_CUSTOMER_ID_NAME,
        lbs_customeridyominame AS LBS_CUSTOMER_ID_YOMINAME,
        lbs_name AS LBS_NAME,
        modifiedbyname AS MODIFIED_BY_NAME,
        modifiedbyyominame AS MODIFIED_BY_YOMINAME,
        modifiedon AS MODIFIED_ON,
        modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
        modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMINAME,
        overriddencreatedon AS OVERRIDDEN_CREATED_ON,
        owneridname AS OWNER_ID_NAME,
        owneridtype AS OWNER_ID_TYPE,
        owneridyominame AS OWNER_ID_YOMINAME,
        owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
        timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
        utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
        versionnumber AS VERSION_NUMBER,
        IsDelete AS IS_DELETE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME
    FROM {env_var}_catalog.bronze_dataverse_con.lbs_address
    """
    )

    df = add_housekeeping_columns(df, "ADDRESS_ID")

    return df


# COMMAND ----------

@dlt.table(
    name="dataverse_country", 
    comment="This table stores the customer management country data from dataverse" 
)
@dlt.expect_all_or_drop({
    "valid_customer_id": "COUNTRY_ID IS NOT NULL"
})
def dataverse_country():

    df = spark.sql(
        f"""
    SELECT   
        Id AS COUNTRY_ID,
        statecode AS STATE_CODE,
        statuscode AS STATUS_CODE,
        createdby AS CREATED_BY,
        createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
        createdonbehalfby AS CREATED_ON_BEHALF_BY,
        createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
        modifiedby AS MODIFIED_BY,
        modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
        modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
        modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
        organizationid AS ORGANIZATION_ID,
        organizationid_entitytype AS ORGANIZATION_ID_ENTITY_TYPE,
        createdbyname AS CREATED_BY_NAME,
        createdbyyominame AS CREATED_BY_YOMINAME,
        createdon AS CREATED_ON,
        createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
        createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMINAME,
        importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
        lbs_countrycode AS LBS_COUNTRY_CODE,
        lbs_countryid AS LBS_COUNTRY_ID,
        lbs_dialingcode AS LBS_DIALING_CODE,
        TRIM(REPLACE(lbs_name,'(the)','')) AS COUNTRY_NAME, 
        modifiedbyname AS MODIFIED_BY_NAME,
        modifiedbyyominame AS MODIFIED_BY_YOMINAME,
        modifiedon AS MODIFIED_ON,
        modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
        modifiedonbehalfbyyominame AS MODIFED_ON_BEHALF_BY_YOMINAME,
        organizationidname AS ORGANIZATION_ID_NAME,
        overriddencreatedon AS OVERRIDDEN_CREATED_ON,
        timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
        utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
        versionnumber AS VERSION_NUMBER,
        IsDelete AS IS_DELETE,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME
    FROM {env_var}_catalog.bronze_dataverse_con.lbs_country
    """
    )
    
    df = add_housekeeping_columns(df, "COUNTRY_ID")

    return df


# COMMAND ----------

@dlt.table(
    name="dataverse_customer_account", 
    comment="This table stores the customer management customer account data from dataverse "
)
@dlt.expect_all_or_drop({
    "valid_customer_id": "CUSTOMER_ACCOUNT_ID IS NOT NULL"
})
def dataverse_customer_account():

    df = spark.sql(
        f"""
    SELECT   
        Id AS CUSTOMER_ACCOUNT_ID,
        lbs_customer AS CUSTOMER_ACCOUNT_KEY,
        lbs_sortcode AS ACCOUNT_SORTCODE,
        lbs_accountid AS ACCOUNT_NUMBER,
        lbs_accountsuffix AS ACCOUNT_SUFFIX,
        lbs_fullaccountnumber AS ACCOUNT_NUMBER_AND_SUFFIX,
        lbs_holdertype AS CUSTOMER_ACCOUNT_HOLDER_TYPE_CODE,
        lbs_holderrelationtypename AS CUSTOMER_ACCOUNT_HOLDER_TYPE,
        statecode AS STATE_CODE,
        statuscode AS STATUS_CODE,
        createdby AS CREATED_BY,
        createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
        createdonbehalfby AS CREATED_ON_BEHALF_BY,
        createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
        lbs_customer AS LBS_CUSTOMER,
        lbs_customer_entitytype AS LBS_CUSTOMER_ENTITY_TYPE,
        modifiedby AS MODIFIED_BY,
        modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
        modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
        modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
        owningbusinessunit AS OWNING_BUSINESS_UNIT,
        owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
        owningteam AS OWNING_TEAM,
        owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
        owninguser AS OWNING_USER,
        owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
        ownerid AS OWNER_ID,
        ownerid_entitytype AS OWNDER_ID_ENTITY_TYPE,
        createdbyname AS CREATED_BY_NAME,
        createdbyyominame AS CREATED_BY_YOMINAME,
        createdon AS CREATED_ON,
        createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
        createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMINAME,
        importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
        lbs_correlationid AS LBS_CORRELATION_ID,
        lbs_customeraccountid AS LBS_CUSTOMER_ACCOUNT_ID,
        lbs_customername AS LBS_CUSTOMER_NAME,
        lbs_customeryominame AS LBS_CUSTOMER_YOMINAME,
        lbs_name AS LBS_NAME,
        modifiedbyname AS MODIFIED_BY_NAME,
        modifiedbyyominame AS MODIFIED_BY_YOMINAME,
        modifiedon AS MODIFIED_ON,
        modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
        modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMINAME,
        overriddencreatedon AS OVERRIDDEN_CREATED_ON, 
        owneridname AS OWNER_ID_NAME,
        owneridtype AS OWNER_ID_TYPE,
        owneridyominame AS OWNER_ID_YOMINAME,
        owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
        timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
        utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
        versionnumber AS VERSION_NUMBER,
        IsDelete AS IS_DELETE,
        lbs_startdate AS LBS_START_DATE,
        lbs_enddate AS LBS_END_DATE,
        CAST(CA.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(CA.MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME
    FROM {env_var}_catalog.bronze_dataverse_con.lbs_customeraccount AS CA
    """
    )
    
    df = add_housekeeping_columns(df, "CUSTOMER_ACCOUNT_ID")

    return df


# COMMAND ----------

@dlt.table(
    name="dataverse_customer_details", 
    comment="This table stores the customer deatils information a from dataverse "
)
@dlt.expect_all_or_drop({
    "valid_customer_id": "CUSTOMER_DETAILS_ID IS NOT NULL"
})
def dataverse_customer_details():
    df = spark.sql(
        f"""
    SELECT  
        Id AS CUSTOMER_DETAILS_ID,        
        statecode AS STATE_CODE,
        statuscode AS STATUS_CODE,
        lbs_address1addresstype AS LBS_ADDRESS1_ADDRESSTYPE,
        lbs_address2addresstype AS LBS_ADDRESS2_ADDRESSTYPE,
        lbs_electronicidentificationstatus AS LBS_ELECTRONIC_IDENTIFICATION_STATUS,
        lbs_familystatuscode AS LBS_FAMILY_STATUS_CODE,
        lbs_gender AS LBS_GENDER,
        lbs_pronoun AS LBS_PRONOUN,
        lbs_reasonforchange AS LBS_REASON_FOR_CHANGE,
        lbs_title AS LBS_TITLE,
        lbs_typeofchange AS LBS_TYPE_OF_CHANGE,
        lbs_typeofevidence AS LBS_TYPE_OF_EVIDENCE,
        lbs_nationality AS LBS_NATIONALITY,
        lbs_commitchanges AS LBS_COMMIT_CHANGES,
        lbs_correspondenceaddresssameascurrentresidential AS LBS_CORRESPONDENCE_ADDRESS_SAME_AS_CURRENT_RESIDENTIAL,
        lbs_dataprepopulated AS LBS_DATA_PREPOPULATED,
        lbs_lbscolleague AS LBS_COLLEAGUE,
        lbs_lockfieldsaftercommit AS LBS_LOCK_FIELDS_AFTER_COMMIT,
        lbs_lockpreviousstages AS LBS_LOCK_PREVIOUS_STAGES,
        createdby AS CREATED_BY,
        createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
        createdonbehalfby AS CREATED_ON_BEHALF_BY,
        createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE, 
        lbs_address1_country AS LBS_ADDRESS1_COUNTRY,       
        lbs_address1_country_entitytype AS LBS_ADDRESS1_COUNTRY_ENTITY_TYPE,
        lbs_address2_country AS LBS_ADDRESS2_COUNTRY,        
        lbs_address2_country_entitytype AS LBS_ADDRESS2_COUNTRY_ENTITY_TYPE,
        lbs_countryforhometelephonenumber AS LBS_COUNTRY_FOR_HOME_TELEPHONE_NUMBER,
        lbs_countryforhometelephonenumber_entitytype AS LBS_COUNTRY_FOR_HOME_TELEPHONE_NUMBER_ENTITY_TYPE,
        lbs_countryformobiletelephonenumber AS LBS_COUNTRY_FOR_MOBILE_TELEPHONE_NUMBER,
        lbs_countryformobiletelephonenumber_entitytype AS LBS_COUNTRY_FOR_MOBILE_TELEPHONE_NUMBER_ENTITY_TYPE,
        lbs_countryforworktelephonenumber AS LBS_COUNTRY_FOR_WORK_TELEPHONE_NUMBER,
        lbs_countryforworktelephonenumber_entitytype AS LBS_COUNTRY_FOR_WORK_TELEPHONE_NUMBER_ENTITY_TYPE,
        lbs_countryofbirth AS LBS_COUNTRY_OF_BIRTH,
        lbs_countryofbirth_entitytype AS LBS_COUNTRY_OF_BIRTH_ENTITY_TYPE,
        lbs_currentoccupation AS LBS_CURRENT_OCCUPATION,
        lbs_currentoccupation_entitytype AS LBS_CURRENT_OCCUPATION_ENTITY_TYPE,
        lbs_customer AS LBS_CUSTOMER,
        lbs_customer_entitytype AS LBS_CUSTOMER_ENTITY_TYPE,
        lbs_employmentstatusid AS LBS_EMPLOYMENT_STATUS_ID,
        lbs_employmentstatusid_entitytype AS LBS_EMPLOYMENT_STATUS_ID_ENTITY_TYPE,
        modifiedby AS MODIFIED_BY,
        modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
        modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
        modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
        owningbusinessunit AS OWNING_BUSINESS_UNIT,
        owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
        owningteam AS OWNING_TEAM,
        owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
        owninguser AS OWNING_USER,
        owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
        transactioncurrencyid AS TRANSACTION_CURRENCY_ID,
        transactioncurrencyid_entitytype AS TRANSACTION_CURRENCY_ID_ENTITY_TYPE,
        ownerid AS OWNER_ID,
        ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
        lbs_annualincome AS LBS_ANNUAL_INCOME,
        lbs_annualincome_base AS LBS_ANNUAL_INCOME_BASE,
        createdbyname AS CREATED_BY_NAME,
        createdbyyominame AS CREATED_BY_YOMI_NAME,
        createdon AS CREATED_ON,
        createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
        createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMI_NAME,
        exchangerate AS EXCHANGE_RATE,
        importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
        lbs_address1_bfpo_bfponumber AS LBS_ADDRESS1_BFPO_BFPO_NUMBER,
        lbs_address1_bfpo_careofsurname AS LBS_ADDRESS1_BFPO_CARE_OF_SURNAME,
        lbs_address1_bfpo_operationname AS LBS_ADDRESS1_BFPO_OPERATION_NAME,
        lbs_address1_bfpo_rank AS LBS_ADDRESS1_BFPO_RANK,
        lbs_address1_bfpo_servicenumber AS LBS_ADDRESS1_BFPO_SERVICE_NUMBER,
        lbs_address1_bfpo_unit AS LBS_ADDRESS1_BFPO_UNIT,
        lbs_address1_buildingnumber AS LBS_ADDRESS1_BUILDING_NUMBER,
        lbs_address1_city AS LBS_ADDRESS1_CITY,
        lbs_address1_countryname AS LBS_ADDRESS1_COUNTRY_NAME,
        lbs_address1_county AS LBS_ADDRESS1_COUNTY,
        lbs_address1_flat AS LBS_ADDRESS1_FLAT,
        lbs_address1_line1 AS LBS_ADDRESS1_LINE1,
        lbs_address1_name AS LBS_ADDRESS1_NAME,
        lbs_address1_overseas_line1 AS LBS_ADDRESS1_OVERSEAS_LINE1,
        lbs_address1_overseas_line2 AS LBS_ADDRESS1_OVERSEAS_LINE2,
        lbs_address1_overseas_line3 AS LBS_ADDRESS1_OVERSEAS_LINE3,
        lbs_address1_overseas_line4 AS LBS_ADDRESS1_OVERSEAS_LINE4,
        lbs_address1_postalcode AS LBS_ADDRESS1_POSTAL_CODE,
        lbs_address1_stateorprovince AS LBS_ADDRESS1_STATE_OR_PROVINCE,
        lbs_address2_bfpo_bfponumber AS LBS_ADDRESS2_BFPO_BFPO_NUMBER,
        lbs_address2_bfpo_careofsurname AS LBS_ADDRESS2_BFPO_CARE_OF_SURNAME,
        lbs_address2_bfpo_operationname AS LBS_ADDRESS2_BFPO_OPERATION_NAME,
        lbs_address2_bfpo_rank AS LBS_ADDRESS2_BFPO_RANK,
        lbs_address2_bfpo_servicenumber AS LBS_ADDRESS2_BFPO_SERVICE_NUMBER,
        lbs_address2_bfpo_unit AS LBS_ADDRESS2_BFPO_UNIT,
        lbs_address2_buildingnumber AS LBS_ADDRESS2_BUILDING_NUMBER,
        lbs_address2_city AS LBS_ADDRESS2_CITY,
        lbs_address2_countryname AS LBS_ADDRESS2_COUNTRY_NAME,
        lbs_address2_county AS LBS_ADDRESS2_COUNTY,
        lbs_address2_flat AS LBS_ADDRESS2_FLAT,
        lbs_address2_line1 AS LBS_ADDRESS2_LINE1,
        lbs_address2_name AS LBS_ADDRESS2_NAME,
        lbs_address2_overseas_line1 AS LBS_ADDRESS2_OVERSEAS_LINE1,
        lbs_address2_overseas_line2 AS LBS_ADDRESS2_OVERSEAS_LINE2,
        lbs_address2_overseas_line3 AS LBS_ADDRESS2_OVERSEAS_LINE3,
        lbs_address2_overseas_line4 AS LBS_ADDRESS2_OVERSEAS_LINE4,
        lbs_address2_postalcode AS LBS_ADDRESS2_POSTAL_CODE,
        lbs_address2_stateorprovince AS LBS_ADDRESS2_STATE_OR_PROVINCE,
        lbs_birthdate AS LBS_BIRTH_DATE,
        lbs_correspondenceaddressstartdate AS LBS_CORRESPONDENCE_ADDRESS_START_DATE,
        lbs_countryforhometelephonenumbername AS LBS_COUNTRY_FOR_HOME_TELEPHONE_NUMBER_NAME,
        lbs_countryformobiletelephonenumbername AS LBS_COUNTRY_FOR_MOBILE_TELEPHONE_NUMBER_NAME,
        lbs_countryforworktelephonenumbername AS LBS_COUNTRY_FOR_WORK_TELEPHONE_NUMBER_NAME,
        lbs_countryofbirthname AS LBS_COUNTRY_OF_BIRTH_NAME,
        lbs_currentoccupationname AS LBS_CURRENT_OCCUPATION_NAME,
        lbs_customername AS LBS_CUSTOMER_NAME,
        lbs_customeryominame AS LBS_CUSTOMER_YOMI_NAME,
        lbs_dialingcodehome AS LBS_DIALING_CODE_HOME,
        lbs_dialingcodemobile AS LBS_DIALING_CODE_MOBILE,
        lbs_dialingcodework AS LBS_DIALING_CODE_WORK,
        lbs_editcustomerdetailsid AS LBS_EDIT_CUSTOMER_DETAILS_ID,
        lbs_emailaddress1 AS LBS_EMAIL_ADDRESS1,
        lbs_employmentstatusidname AS LBS_EMPLOYMENT_STATUS_ID_NAME,
        lbs_extensioncode AS LBS_EXTENSION_CODE,       
        lbs_firstname AS LBS_FIRST_NAME,
        lbs_middlename AS LBS_MIDDLE_NAME,
        lbs_mobilephone AS LBS_MOBILE_PHONE,
        lbs_name AS LBS_NAME,
        lbs_ninumber AS LBS_NI_NUMBER,
        lbs_otherevidence AS LBS_OTHER_EVIDENCE,
        lbs_preferredname AS LBS_PREFERRED_NAME, 
        lbs_previouscorrespondenceaddressenddate AS LBS_PREVIOUS_CORRESPONDENCE_ADDRESS_END_DATE,
        lbs_previousresidentialaddressenddate AS LBS_PREVIOUS_RESIDENTIAL_ADDRESS_END_DATE,       
        lbs_residentialaddressstartdate AS LBS_RESIDENTIAL_ADDRESS_START_DATE,
        lbs_surname AS LBS_SURNAME,
        lbs_telephone1 AS LBS_TELEPHONE1,
        lbs_telephone2 AS LBS_TELEPHONE2,
        lbs_townofbirth AS LBS_TOWN_OF_BIRTH,
        lbs_uploadfirstdocument AS LBS_UPLOAD_FIRST_DOCUMENT,
        lbs_uploadfirstdocument_name AS LBS_UPLOAD_FIRST_DOCUMENT_NAME,
        lbs_uploadseconddocument AS LBS_UPLOAD_SECOND_DOCUMENT,
        lbs_uploadseconddocument_name AS LBS_UPLOAD_SECOND_DOCUMENT_NAME,
        lbs_uploadthirddocument AS LBS_UPLOAD_THIRD_DOCUMENT,
        lbs_uploadthirddocument_name AS LBS_UPLOAD_THIRD_DOCUMENT_NAME,
        modifiedbyname AS MODIFIED_BY_NAME,
        modifiedbyyominame AS MODIFIED_BY_YOMI_NAME,
        modifiedon AS MODIFIED_ON,
        modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
        modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMI_NAME,
        overriddencreatedon AS OVERRIDDEN_CREATED_ON,
        owneridname AS OWNER_ID_NAME,
        owneridtype AS OWNER_ID_TYPE,
        owneridyominame AS OWNER_ID_YOMI_NAME,        
        owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
        processid AS PROCESS_ID,
        stageid AS STAGE_ID,
        timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
        transactioncurrencyidname AS TRANSACTION_CURRENCY_ID_NAME,
        traversedpath AS TRAVERSED_PATH,
        utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
        versionnumber AS VERSION_NUMBER,
        IsDelete as IS_DELETE,
        lbs_editcompleted AS LBS_EDIT_COMPLETED,
        lbs_colleaguestaffnumber AS LBS_COLLEAGUE_STAFF_NUMBER,
        CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
        CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
        TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
        TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME       
    FROM {env_var}_catalog.bronze_dataverse_con.lbs_editcustomerdetails
    """
    )
    
    df = add_housekeeping_columns(df, "CUSTOMER_DETAILS_ID")
    return df


# COMMAND ----------

if(ft.FeatureToggle.is_toggle_enabled(env_var, 'DATAVERSE', 'core_ts2_pp2_dataverse_a')):

    @dlt.table(
        name="dataverse_employment_status", 
        comment="This table stores the employee details information a from dataverse "
    )
    @dlt.expect_all_or_drop({
        "valid_customer_id": "EMPLOYMENT_STATUS_ID IS NOT NULL"
    })
    def dataverse_employment_status():
        df = spark.sql(
            f"""
        SELECT 
            Id AS EMPLOYMENT_STATUS_ID,
            statecode AS STATE_CODE,
            statuscode AS STATUS_CODE,
            createdby AS CREATED_BY,
            createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
            createdonbehalfby AS CREATED_ON_BEHALF_BY,
            createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
            modifiedby AS MODIFIED_BY,
            modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
            modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
            modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
            owningbusinessunit AS OWNING_BUSINESS_UNIT,
            owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
            owningteam AS OWNING_TEAM,
            owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
            owninguser AS OWNING_USER,
            owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
            ownerid AS OWNER_ID,
            ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
            createdbyname AS CREATED_BY_NAME,
            createdbyyominame AS CREATED_BY_YOMI_NAME,
            createdon AS CREATED_ON,
            createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
            createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMI_NAME,
            importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
            lbs_employmentstatusid AS LBS_EMPLOYMENT_STATUS_ID,
            lbs_name AS LBS_NAME,
            modifiedbyname AS MODIFIED_BY_NAME,
            modifiedbyyominame AS MODIFIED_BY_YOMI_NAME,
            modifiedon AS MODIFIED_ON,
            modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
            modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMI_NAME,
            overriddencreatedon AS OVERRIDDEN_CREATED_ON,
            owneridname AS OWNER_ID_NAME,
            owneridtype AS OWNER_ID_TYPE,
            owneridyominame AS OWNER_ID_YOMI_NAME,
            owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
            timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
            utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
            versionnumber AS VERSION_NUMBER,
            IsDelete as IS_DELETE,        
            CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
            CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
            TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME       
        FROM {env_var}_catalog.bronze_dataverse_con.lbs_employmentstatus
        """
        )
        
        df = add_housekeeping_columns(df, "EMPLOYMENT_STATUS_ID")
        return df



# COMMAND ----------

if(ft.FeatureToggle.is_toggle_enabled(env_var, 'DATAVERSE', 'core_ts2_dataverse_mar26')):

    @dlt.table(
        name="dataverse_account", 
        comment="This table stores dataverse account information from dataverse "
    )
    @dlt.expect_all_or_drop({
        "valid_account_id": "ACCOUNT_ID IS NOT NULL"
    })
    def dataverse_account():
        df = spark.sql(
            f"""
        SELECT 
            Id AS ACCOUNT_ID,
            statecode AS STATE_CODE,
            statuscode AS STATUS_CODE,
            createdby AS CREATED_BY,
            createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
            createdonbehalfby AS CREATED_ON_BEHALF_BY,
            createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
            modifiedby AS MODIFIED_BY,
            modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
            modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
            modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
            owningbusinessunit AS OWNING_BUSINESS_UNIT,
            owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
            owningteam AS OWNING_TEAM,
            owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
            owninguser AS OWNING_USER,
            owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
            ownerid AS OWNER_ID,
            ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
            createdbyname AS CREATED_BY_NAME,
            createdbyyominame AS CREATED_BY_YOMI_NAME,
            createdon AS CREATED_ON,
            createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
            createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMI_NAME,
            importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
            modifiedbyname AS MODIFIED_BY_NAME,
            modifiedbyyominame AS MODIFIED_BY_YOMI_NAME,
            modifiedon AS MODIFIED_ON,
            modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
            modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMI_NAME,
            overriddencreatedon AS OVERRIDDEN_CREATED_ON,
            owneridname AS OWNER_ID_NAME,
            owneridtype AS OWNER_ID_TYPE,
            owneridyominame AS OWNER_ID_YOMI_NAME,
            owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
            timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
            utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
            versionnumber AS VERSION_NUMBER,
            lbs_accountid AS LBS_ACCOUNT_ID,
            lbs_fullaccountnumber AS FULL_ACCOUNT_NUMBER,
            lbs_lbssavingsaccountdetailsappidholder AS SAVINGS_ACCOUNT_DETAILS_APP_ID_HOLDER,
            lbs_name AS NAME, 
            mdp_load_type AS MDP_LOAD_TYPE,
            _metadata AS METADATA,           
            CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
            CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
            TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME      
        FROM {env_var}_catalog.bronze_dataverse_con.lbs_account
        """
        )
        
        df = add_housekeeping_columns(df, "ACCOUNT_ID")
        return df



# COMMAND ----------

if(ft.FeatureToggle.is_toggle_enabled(env_var, 'DATAVERSE', 'core_ts2_dataverse_mar26')):

    @dlt.table(
        name="dataverse_datacatalog", 
        comment="This table stores dataverse datacatalog information from dataverse "
    )
    @dlt.expect_all_or_drop({
        "valid_account_id": "CATALOG_ID IS NOT NULL"
        
    })
    def dataverse_datacatalog():
        df = spark.sql(
            f"""
        SELECT 
            Id AS CATALOG_ID,
            statecode AS STATE_CODE,
            statuscode AS STATUS_CODE,
            createdby AS CREATED_BY,
            createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
            createdonbehalfby AS CREATED_ON_BEHALF_BY,
            createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
            modifiedby AS MODIFIED_BY,
            modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
            modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
            modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
            owningbusinessunit AS OWNING_BUSINESS_UNIT,
            owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
            owningteam AS OWNING_TEAM,
            owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
            owninguser AS OWNING_USER,
            owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
            ownerid AS OWNER_ID,
            ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
            createdbyname AS CREATED_BY_NAME,
            createdbyyominame AS CREATED_BY_YOMI_NAME,
            createdon AS CREATED_ON,
            createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
            createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMI_NAME,
            importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
            modifiedbyname AS MODIFIED_BY_NAME,
            modifiedbyyominame AS MODIFIED_BY_YOMI_NAME,
            modifiedon AS MODIFIED_ON,
            modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
            modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMI_NAME,
            overriddencreatedon AS OVERRIDDEN_CREATED_ON,
            owneridname AS OWNER_ID_NAME,
            owneridtype AS OWNER_ID_TYPE,
            owneridyominame AS OWNER_ID_YOMI_NAME,
            owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
            timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
            utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
            versionnumber AS VERSION_NUMBER,
            lbs_consumers AS CONSUMERS,
            lbs_currentappid AS CURRENT_APP_ID,
            lbs_datatablelink AS DATA_TABLE_LINK,
            lbs_domain AS DOMAIN,
            lbs_entitytype AS ENTITY_TYPE,
            lbs_foreignkeys AS FOREIGN_KEYS,
            lbs_indexes AS INDEXES,
            lbs_physicallocation AS PHYSICAL_LOCATION,
            lbs_primarykey AS PRIMARY_KEY,
            lbs_primarytableowner AS PRIMARY_TABLE_OWNER,
            lbs_referentialintegrity AS REFERENTIAL_INTEGRITY,
            lbs_secondarytableowner AS SECONDARY_TABLE_OWNER,
            lbs_datacatalogid AS DATA_CATALOG_ID,
            lbs_securityclassification AS SECURITY_CLASSIFICATION,
            lbs_subscriptiondetails AS SUBSCRIPTION_DETAILS,
            lbs_tableconstraints AS TABLE_CONSTRAINTS,
            lbs_tabledescription AS TABLE_DESCRIPTION,
            lbs_tablename AS TABLE_NAME,
            mdp_load_type AS MDP_LOAD_TYPE,
            _metadata AS METADATA,           
            CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
            CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
            TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME        
        FROM {env_var}_catalog.bronze_dataverse_con.lbs_datacatalog
        """
        )
        
        df = add_housekeeping_columns(df, "CATALOG_ID")
        return df



# COMMAND ----------

if(ft.FeatureToggle.is_toggle_enabled(env_var, 'DATAVERSE', 'core_ts2_dataverse_mar26')):

    @dlt.table(
        name="dataverse_facetoface", 
        comment="This table stores dataverse factoface information from dataverse "
    )
    @dlt.expect_all_or_drop({
        "valid_account_id": "FACETOFACE_ID IS NOT NULL"
    })
    def dataverse_facetoface():
        df = spark.sql(
            f"""
        SELECT 
            Id AS FACETOFACE_ID,
            statecode AS STATE_CODE,
            statuscode AS STATUS_CODE,
            createdby AS CREATED_BY,
            createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
            createdonbehalfby AS CREATED_ON_BEHALF_BY,
            createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
            modifiedby AS MODIFIED_BY,
            modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
            modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
            modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
            owningbusinessunit AS OWNING_BUSINESS_UNIT,
            owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
            owningteam AS OWNING_TEAM,
            owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
            owninguser AS OWNING_USER,
            owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
            ownerid AS OWNER_ID,
            ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
            createdbyname AS CREATED_BY_NAME,
            createdbyyominame AS CREATED_BY_YOMI_NAME,
            createdon AS CREATED_ON,
            createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
            createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMI_NAME,
            importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
            modifiedbyname AS MODIFIED_BY_NAME,
            modifiedbyyominame AS MODIFIED_BY_YOMI_NAME,
            modifiedon AS MODIFIED_ON,
            modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
            modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMI_NAME,
            overriddencreatedon AS OVERRIDDEN_CREATED_ON,
            owneridname AS OWNER_ID_NAME,
            owneridtype AS OWNER_ID_TYPE,
            owneridyominame AS OWNER_ID_YOMI_NAME,
            owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
            timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
            utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
            versionnumber AS VERSION_NUMBER,
            activityid AS ACTIVITY_ID,
            scheduledend AS SCHEDULED_END,
            lbs_contactmethod AS CONTACT_METHOD,
            lbs_contacttype AS CONTACT_TYPE,
            from AS FROM,
            lbs_whomadecontact AS WHO_MADE_CONTACT,
            regardingobjectid AS REGARDING_OBJECT_ID,
            regardingobjecttypecode AS REGARDING_OBJECT_TYPE_CODE,
            description AS DESCRIPTION,
            mdp_load_type AS MDP_LOAD_TYPE,
            _metadata AS METADATA,           
            CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
            CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
            TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME       
        FROM {env_var}_catalog.bronze_dataverse_con.lbs_facetoface
        """
        )
        
        df = add_housekeeping_columns(df, "FACETOFACE_ID")
        return df



# COMMAND ----------

if(ft.FeatureToggle.is_toggle_enabled(env_var, 'DATAVERSE', 'core_ts2_dataverse_mar26')):

    @dlt.table(
        name="dataverse_investigation", 
        comment="This table stores investigation information from dataverse "
    )
    @dlt.expect_all_or_drop({
        "valid_account_id": "INVESTIGATION_ID IS NOT NULL"
    })
    def dataverse_investigation():
        df = spark.sql(
            f"""
        SELECT 
            Id AS INVESTIGATION_ID,
            statecode AS STATE_CODE,
            statuscode AS STATUS_CODE,
            createdby AS CREATED_BY,
            createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
            createdonbehalfby AS CREATED_ON_BEHALF_BY,
            createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
            modifiedby AS MODIFIED_BY,
            modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
            modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
            modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
            owningbusinessunit AS OWNING_BUSINESS_UNIT,
            owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
            owningteam AS OWNING_TEAM,
            owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
            owninguser AS OWNING_USER,
            owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
            ownerid AS OWNER_ID,
            ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
            createdbyname AS CREATED_BY_NAME,
            createdbyyominame AS CREATED_BY_YOMI_NAME,
            createdon AS CREATED_ON,
            createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
            createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMI_NAME,
            importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
            modifiedbyname AS MODIFIED_BY_NAME,
            modifiedbyyominame AS MODIFIED_BY_YOMI_NAME,
            modifiedon AS MODIFIED_ON,
            modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
            modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMI_NAME,
            overriddencreatedon AS OVERRIDDEN_CREATED_ON,
            owneridname AS OWNER_ID_NAME,
            owneridtype AS OWNER_ID_TYPE,
            owneridyominame AS OWNER_ID_YOMI_NAME,
            owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
            timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
            utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
            versionnumber AS VERSION_NUMBER,
            activityid AS ACTIVITY_ID,
            lbs_contactmethod AS CONTACT_METHOD,
            lbs_contacttype AS CONTACT_TYPE,
            lbs_checkcompleted AS CHECK_COMPLETED,
            regardingobjectid AS REGARDING_OBJECT_ID,
            regardingobjecttypecode AS REGARDING_OBJECT_TYPE_CODE,
            description AS DESCRIPTION,
            mdp_load_type AS MDP_LOAD_TYPE,
            _metadata AS METADATA,           
            CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
            CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
            TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME       
        FROM {env_var}_catalog.bronze_dataverse_con.lbs_investigation
        """
        )
        
        df = add_housekeeping_columns(df, "INVESTIGATION_ID")
        return df



# COMMAND ----------

if(ft.FeatureToggle.is_toggle_enabled(env_var, 'DATAVERSE', 'core_ts2_dataverse_mar26')):

    @dlt.table(
        name="dataverse_nonworkingday", 
        comment="This table stores nonworkingday information from dataverse "
    )
    @dlt.expect_all_or_drop({
        "valid_account_id": "NONWORKINGDAY_ID IS NOT NULL"
    })
    def dataverse_nonworkingday():
        df = spark.sql(
            f"""
        SELECT 
            Id AS NONWORKINGDAY_ID,
            statecode AS STATE_CODE,
            statuscode AS STATUS_CODE,
            createdby AS CREATED_BY,
            createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
            createdonbehalfby AS CREATED_ON_BEHALF_BY,
            createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
            modifiedby AS MODIFIED_BY,
            modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
            modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
            modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
            owningbusinessunit AS OWNING_BUSINESS_UNIT,
            owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
            owningteam AS OWNING_TEAM,
            owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
            owninguser AS OWNING_USER,
            owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
            ownerid AS OWNER_ID,
            ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
            createdbyname AS CREATED_BY_NAME,
            createdbyyominame AS CREATED_BY_YOMI_NAME,
            createdon AS CREATED_ON,
            createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
            createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMI_NAME,
            importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
            modifiedbyname AS MODIFIED_BY_NAME,
            modifiedbyyominame AS MODIFIED_BY_YOMI_NAME,
            modifiedon AS MODIFIED_ON,
            modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
            modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMI_NAME,
            overriddencreatedon AS OVERRIDDEN_CREATED_ON,
            owneridname AS OWNER_ID_NAME,
            owneridtype AS OWNER_ID_TYPE,
            owneridyominame AS OWNER_ID_YOMI_NAME,
            owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
            timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
            utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
            versionnumber AS VERSION_NUMBER,
            lbs_date AS DATE,
            lbs_division AS DIVISION,
            lbs_source AS SOURCE,
            lbs_title AS TITLE,
            lbs_type AS TYPE,
            lbs_nonworkingdayid AS LBS_NONWORKINGDAY_ID,
            mdp_load_type AS MDP_LOAD_TYPE,
            _metadata AS METADATA,           
            CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
            CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
            TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME       
        FROM {env_var}_catalog.bronze_dataverse_con.lbs_nonworkingday
        """
        )
        
        df = add_housekeeping_columns(df, "NONWORKINGDAY_ID")
        return df



# COMMAND ----------

if(ft.FeatureToggle.is_toggle_enabled(env_var, 'DATAVERSE', 'core_ts2_dataverse_mar26')):

    @dlt.table(
        name="dataverse_phonecall", 
        comment="This table stores phonecall information from dataverse "
    )
    @dlt.expect_all_or_drop({
        "valid_account_id": "PHONECALL_ID IS NOT NULL"
    })
    def dataverse_phonecall():
        df = spark.sql(
            f"""
        SELECT 
            Id AS PHONECALL_ID,
            statecode AS STATE_CODE,
            statuscode AS STATUS_CODE,
            createdby AS CREATED_BY,
            createdby_entitytype AS CREATED_BY_ENTITY_TYPE,
            createdonbehalfby AS CREATED_ON_BEHALF_BY,
            createdonbehalfby_entitytype AS CREATED_ON_BEHALF_BY_ENTITY_TYPE,
            modifiedby AS MODIFIED_BY,
            modifiedby_entitytype AS MODIFIED_BY_ENTITY_TYPE,
            modifiedonbehalfby AS MODIFIED_ON_BEHALF_BY,
            modifiedonbehalfby_entitytype AS MODIFIED_ON_BEHALF_BY_ENTITY_TYPE,
            owningbusinessunit AS OWNING_BUSINESS_UNIT,
            owningbusinessunit_entitytype AS OWNING_BUSINESS_UNIT_ENTITY_TYPE,
            owningteam AS OWNING_TEAM,
            owningteam_entitytype AS OWNING_TEAM_ENTITY_TYPE,
            owninguser AS OWNING_USER,
            owninguser_entitytype AS OWNING_USER_ENTITY_TYPE,
            ownerid AS OWNER_ID,
            ownerid_entitytype AS OWNER_ID_ENTITY_TYPE,
            createdbyname AS CREATED_BY_NAME,
            createdbyyominame AS CREATED_BY_YOMI_NAME,
            createdon AS CREATED_ON,
            createdonbehalfbyname AS CREATED_ON_BEHALF_BY_NAME,
            createdonbehalfbyyominame AS CREATED_ON_BEHALF_BY_YOMI_NAME,
            importsequencenumber AS IMPORT_SEQUENCE_NUMBER,
            modifiedbyname AS MODIFIED_BY_NAME,
            modifiedbyyominame AS MODIFIED_BY_YOMI_NAME,
            modifiedon AS MODIFIED_ON,
            modifiedonbehalfbyname AS MODIFIED_ON_BEHALF_BY_NAME,
            modifiedonbehalfbyyominame AS MODIFIED_ON_BEHALF_BY_YOMI_NAME,
            overriddencreatedon AS OVERRIDDEN_CREATED_ON,
            owneridname AS OWNER_ID_NAME,
            owneridtype AS OWNER_ID_TYPE,
            owneridyominame AS OWNER_ID_YOMI_NAME,
            owningbusinessunitname AS OWNING_BUSINESS_UNIT_NAME,
            timezoneruleversionnumber AS TIME_ZONE_RULE_VERSION_NUMBER,
            utcconversiontimezonecode AS UTC_CONVERSION_TIME_ZONE_CODE,
            versionnumber AS VERSION_NUMBER,
            activityid AS ACTIVITY_ID,
            description AS DESCRIPTION,
            directioncode AS DIRECTION_CODE,
            from AS FROM,
            lbs_callfrom AS CALL_FROM,
            regardingobjectid AS REGARDING_OBJECT_ID,
            regardingobjecttypecode AS REGARDING_OBJECT_TYPE_CODE,
            scheduledend AS SCHEDULED_END,
            to AS TO,
            lbs_callto AS CALL_TO,
            lbs_contacttype AS CONTACT_TYPE,
            lbs_calldemandreason AS CALL_DEMAND_REASON,
            mdp_load_type AS MDP_LOAD_TYPE,
            _metadata AS METADATA,           
            CAST(MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
            CAST(MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
            TO_TIMESTAMP(SinkCreatedOn, 'M/d/yyyy h:mm:ss a') AS SINK_CREATED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS SINK_MODIFIED_ON,
            TO_TIMESTAMP(SinkModifiedOn, 'M/d/yyyy h:mm:ss a') AS ROW_START_DATETIME       
        FROM {env_var}_catalog.bronze_dataverse_con.phonecall
        """
        )
        
        df = add_housekeeping_columns(df, "PHONECALL_ID")
        return df


