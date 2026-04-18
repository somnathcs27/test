# Databricks notebook source
import dlt
from pyspark.sql.functions import row_number, col, expr
from pyspark.sql.window import Window

import mdp_databricks_common.silver_layer_functions as dlf

env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name = "select_hr_employee_appointment",
    comment = "relates to the appointment and onboarding of employees at LBS"
)
@dlt.expect("APPOINTMENT_NUMBER is not null", "APPOINTMENT_NUMBER IS NOT NULL")
def silver_select_hr_employee_appointment():

    df = spark.sql(f"""
    SELECT
    CAST(Appointment_Number AS BIGINT) AS APPOINTMENT_NUMBER,
    CAST(Starting_Date AS DATE) AS STARTING_DATE,
    Leaving AS LEAVING,
    Appointment_Reference AS APPOINTMENT_REFERENCE,
    Appointment_Type AS APPOINTMENT_TYPE,
    Payroll_Name AS PAYROLL_NAME,
    CAST(Payroll_Number AS INT) AS PAYROLL_NUMBER,
    Pay_Method AS PAY_METHOD,
    Notice_Rule AS NOTICE_RULE,
    CAST(Probation_Review_Date AS DATE) AS PROBATION_REVIEW_DATE,
    WTD_Opt_Out AS WTD_OPT_OUT,
    CAST(Next_Increment_Date AS DATE) AS NEXT_INCREMENT_DATE,
    Appointment_Notes AS APPOINTMENT_NOTES,
    CV_Document_Number AS CV_DOCUMENT_NUMBER,
    CAST(Non_Starter AS DATE) AS NON_STARTER,
    CAST(Leaving_Date AS DATE) AS LEAVING_DATE,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.employee_appointment_history
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("APPOINTMENT_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "select_hr_employee_career",
    comment = "relates to the career changes of an employee at LBS"
)
@dlt.expect("CAREER_NUMBER is not null", "CAREER_NUMBER IS NOT NULL")
def silver_select_hr_employee_appointment():

    df = spark.sql(f"""
    SELECT
    CAST(CAREER_NUMBER AS BIGINT) AS CAREER_NUMBER,
    CAST(Workflow AS INT) AS WORKFLOW,
    CAST(APPOINTMENT_NUMBER AS BIGINT) AS APPOINTMENT_NUMBER,
    CAST(Effective_Date AS DATE) AS EFFECTIVE_DATE,
    Career_Change_Reason AS CAREER_CHANGE_REASON,
    Status AS STATUS,
    CAST(Cost_To_Number AS BIGINT) AS COST_TO_NUMBER,
    CAST(Post_Number AS BIGINT) AS POST_NUMBER,
    CAST(Contract_End_Date AS DATE) AS CONTRACT_END_DATE,
    CAST(Location_Number AS BIGINT) AS LOCATION_NUMBER,
    CAST(Pattern_Number AS BIGINT) AS PATTERN_NUMBER,
    CAST(Hours_Per_Week AS DECIMAL(18,2)) AS HOURS_PER_WEEK,
    CAST(Weeks_Per_Year AS DECIMAL(18,2)) AS WEEKS_PER_YEAR,
    CAST(Hours_Per_Year AS DECIMAL(18,2)) AS HOURS_PER_YEAR,
    CAST(Standard_Hours AS DECIMAL(18,2)) AS STANDARD_HOURS,
    FTE AS FTE,
    Career_Notes AS CAREER_NOTES,
    CAST(Days_Per_Week AS DECIMAL(18,1)) AS DAYS_PER_WEEK,
    TTO_Rule AS TTO_RULE,
    HolidayRuleNumber AS HOLIDAY_RULE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.employee_career_history
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("CAREER_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "select_hr_employee_snapshot_data",
    comment = "a snapshot of the employee data"
)
@dlt.expect("SNAPSHOT_NUMBER is not null", "SNAPSHOT_NUMBER IS NOT NULL")
def silver_select_hr_employee_appointment():

    df = spark.sql(f"""
    SELECT
    CAST(Snapshot_Number AS BIGINT) AS SNAPSHOT_NUMBER,
    CAST(Person_Number AS BIGINT) AS PERSON_NUMBER,
    Effective_Status AS EFFECTIVE_STATUS,
    CAST(Career_Number AS BIGINT) AS CAREER_NUMBER,
    CAST(Appointment_Number AS BIGINT) AS APPOINTMENT_NUMBER,
    CAST(Manager_Number AS BIGINT) AS MANAGER_NUMBER,
    CAST(Manager_Person_Number AS BIGINT) AS MANAGER_PERSON_NUMBER,
    CAST(Supervisor_Person_Number AS BIGINT) AS SUPERVISOR_PERSON_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.employee_snapshot_data
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("SNAPSHOT_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "select_hr_employee_detail",
    comment = "relates to the details of the LBS employee"
)
@dlt.expect("PERSON_NUMBER is not null", "PERSON_NUMBER IS NOT NULL")
def silver_select_hr_employee_appointment():

    df = spark.sql(f"""
    SELECT
    CAST(Person_Number AS BIGINT) AS PERSON_NUMBER,
    Workflow AS WORKFLOW,
    CAST(Date_Purged AS DATE) AS DATE_PURGED,
    Formal_Name as FORMAL_NAME,
    Informal_Name AS INFORMAL_NAME,
    Formal_Name_Reverse AS FORMAL_NAME_REVERSE,
    Informal_Name_Reverse AS INFORMAL_NAME_REVERSE,
    Unique_Name AS UNIQUE_NAME,
    Surname AS SURNAME,
    First_Name AS FIRST_NAME,
    Known_As AS KNOWN_AS,
    Second_Name as SECOND_NAME,
    Title AS TITLE,
    Initials AS INITIALS,
    Previous_Surname as PREVIOUS_SURNAME,
    Designatory_Letters AS DESIGNATORY_LETTERS,
    EMail AS EMAIL,
    Private_EMail as PRIVATE_EMAIL,
    Mobile_Phone_Number AS MOBILE_PHONE_NUMBER,
    Gender AS GENDER,
    CAST(Birth_Date AS DATE) AS BIRTH_DATE,
    CAST(Birth_Certificate_Seen_Date AS DATE) AS BIRTH_CERTIFICATE_SEEN_DATE,
    Ethnic_Origin AS ETHNIC_ORIGIN,
    Nationality AS NATIONALITY,
    NI_Number as NI_NUMBER,
    NI_Letter AS NI_LETTER,
    Marital_Status AS MARITAL_STATUS,
    Religion AS RELIGION,
    Sexual_Orientation AS SEXUAL_ORIENTATION,
    Disabled AS DISABLED,
    Disability AS DISABILITY,
    Dietary_Requirements AS DIETARY_REQUIREMENTS,
    Special_Requirements AS SPECIAL_REQUIREMENTS,
    Work_Phone_Number AS WORK_PHONE_NUMBER,
    Work_Extension_Number AS WORK_EXTENSION_NUMBER,
    Work_Mobile_Phone_Number AS WORK_MOBILE_PHONE_NUMBER,
    Work_Fax_Number AS WORK_FAX_NUMBER,
    Work_Pager_Number AS WORK_PAGER_NUMBER,
    Bank_Sort_Code AS BANK_SORT_CODE,
    Bank_Account_Number AS BANK_ACCOUNT_NUMBER,
    Building_Society_Roll_No AS BUILDING_SOCIETY_ROLL_NO,
    Bank_Account_Name AS BANK_ACCOUNT_NAME,
    Currently_Unsuitable AS CURRENTLY_UNSUITABLE,
    Driving_Licence_No AS DRIVING_LICENCE_NO,
    Driving_Licence_Type AS DRIVING_LICENCE_TYPE,
    Personal_Car_Reg_1 AS PERSONAL_CAR_REG_1,
    Personal_Car_Description_1 AS PERSONAL_CAR_DESCRIPTION_1,
    Personal_Car_Reg_2 AS PERSONAL_CAR_REG_2,
    Personal_Car_Description_2 AS PERSONAL_CAR_DESCRIPTION_2,
    Person_Notes AS PERSON_NOTES,
    External_Activities AS EXTERNAL_ACTIVITIES,
    External_Job_Title AS EXTERNAL_JOB_TITLE,
    External_Company_Name AS EXTERNAL_COMPANY_NAME,
    External_Notes AS EXTERNAL_NOTES,
    Car_Access AS CAR_ACCESS,
    Age_Band AS AGE_BAND,
    Bank_Name AS BANK_NAME,
    Bank_Branch AS BANK_BRANCH,
    Gender_Reassignment AS GENDER_REASSIGNMENT,
    CAST(CV_Item_Number AS INT) AS CV_ITEM_NUMBER,
    Temp_Doc_Item_Number AS TEMP_DOC_ITEM_NUMBER,
    Social_Security_Number AS SOCIAL_SECURITY_NUMBER,
    CAST(Date_Name_Changed AS DATE) AS DATE_NAME_CHANGED,
    Biography AS BIOGRAPHY,
    Salutation AS SALUTATION,
    IBAN AS IBAN,
    SWIFT_Code AS SWIFT_CODE,
    Locale_Number AS LOCALE_NUMBER,
    Region_Number AS REGION_NUMBER,
    Number_of_Children AS NUMBER_OF_CHILDREN,
    Identification_1 AS IDENTIFICATION_1,
    Identification_2 AS IDENTIFICATION_2,
    Identification_3 AS IDENTIFICATION_3,
    GDPR_Consent_Given AS GDPR_CONSENT_GIVEN,
    CAST(GDPR_Date_Consent_Given AS DATE) AS GDPR_DATE_CONSENT_GIVEN,
    GenderIdentityNumber AS GENDERIDENTITYNUMBER,
    GenderIdentitySelfDescribed AS GENDERIDENTITYSELFCDESCRIBED,
    CarerStatusNumberForChild AS CARERSTATUSNUMBERFORCHILD,
    CarerStatusNumberForAdult AS CARERSTATUSNUMBERFORADULT,
    WorkspaceId AS WORKSPACEID,
    Single_Point_Failure AS SINGLE_POINT_FAILURE,
    Mitigate_Risk AS MITIGATE_RISK,
    Residual_KPR AS RESIDUAL_KPR,
    After_Review_is_there_a_Residual_KPR AS AFTER_REVIEW_IS_THERE_A_RESIDUAL_KPR,
    Is_this_colleague_classified_as_a_single_point_of_failure AS IS_THIS_COLLEAGUE_CLASSIFIED_AS_A_SINGLE_POINT_OF_FAILURE,
    Risk_Score AS RISK_SCORE,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.person_details
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("PERSON_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "select_hr_post_detail",
    comment = "the details of the LBS post the employee is assigned to"
)
@dlt.expect("POST_NUMBER is not null", "POST_NUMBER IS NOT NULL")
def silver_select_hr_employee_appointment():

    df = spark.sql(f"""
    SELECT
    CAST(Post_Number AS BIGINT) AS POST_NUMBER,
    Workflow AS WORKFLOW,
    CAST(Post_ID AS BIGINT) AS POST_ID,
    Post_Name AS POST_NAME,
    Post_Unique_Name AS POST_UNIQUE_NAME,
    CAST(Expiry_Date AS DATE) AS EXPIRY_DATE,
    Expired AS EXPIRED,
    Description AS DESCRIPTION,
    CAST(Parent_Unit_Number AS BIGINT) AS PARENT_UNIT_NUMBER,
    CAST(Manager_Post_Number AS BIGINT) AS MANAGER_POST_NUMBER,
    CAST(Supervisor_Post_Number AS BIGINT) AS SUPERVISOR_POST_NUMBER,
    Disclosure_Level AS DISCLOSURE_LEVEL,
    Category AS CATEGORY,
    Framework_Number AS FRAMEWORK_NUMBER,
    CAST(Default_Grade_Number AS BIGINT) AS DEFAULT_GRADE_NUMBER,
    CAST(Default_Pattern_Number AS BIGINT) AS DEFAULT_PATTERN_NUMBER,
    CAST(Default_Location_Number AS BIGINT) AS DEFAULT_LOCATION_NUMBER,
    Minimum_Age AS MINIMUM_AGE,
    Assistant AS ASSISTANT,
    Manual AS MANUAL,
    Temporary AS TEMPORARY,
    Template AS TEMPLATE,
    Shared AS SHARED,
    Description_Document_Number AS DESCRIPTION_DOCUMENT_NUMBER,
    CAST(Job_Evaluation_Date AS DATE) AS JOB_EVALUATION_DATE,
    Know_How_Code AS KNOW_HOW_CODE,
    Know_How_Points AS KNOW_HOW_POINTS,
    Problem_Solving_Code AS PROBLEM_SOLVING_CODE,
    Problem_Solving_Points AS PROBLEM_SOLVING_POINTS,
    Accountability_Code AS ACCOUNTABILITY_CODE,
    Accountability_Points AS ACCOUNTABILITY_POINTS,
    Classification AS CLASSIFICATION,
    Total_Evaluation_Points AS TOTAL_EVALUATION_POINTS,
    Flight_Impact_Number AS FLIGHT_IMPACT_NUMBER,
    CAST(Cost_To_Replace AS DECIMAL(18,2)) AS COST_TO_REPLACE,
    Time_To_Replace AS TIME_TO_REPLACE,
    HR_Role AS HR_ROLE,
    Enabling_Role AS ENABLING_ROLE,
    Revenue_Earning_Role AS REVENUE_EARNING_ROLE,
    Customer_Facing_Role AS CUSTOMER_FACING_ROLE,
    CEO_Level_Role AS CEO_LEVEL_ROLE,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.organisation_post_details
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("POST_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "select_hr_Organisation_Key_level",
    comment = "the table to join the departments to the employee"
)
@dlt.expect("POST_NUMBER is not null", "POST_NUMBER IS NOT NULL")
def silver_select_hr_employee_appointment():

    df = spark.sql(f"""
    SELECT
    CAST(Post_Number AS BIGINT) AS POST_NUMBER,
    Key_Unit_1 AS KEY_UNIT_1,
    Key_Unit_2 AS KEY_UNIT_2,
    Key_Unit_3 AS KEY_UNIT_3,
    Key_Unit_4 AS KEY_UNIT_4,
    Key_Unit_5 AS KEY_UNIT_5,
    Key_Unit_6 AS KEY_UNIT_6,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.organisation_key_levels
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("POST_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "select_hr_Organisation_detail",
    comment = "the details of the organisation"
)
@dlt.expect("UNIT_NUMBER is not null", "UNIT_NUMBER IS NOT NULL")
def silver_select_hr_organisation_details():

    df = spark.sql(f"""
    SELECT
    CAST(Unit_Number AS BIGINT) AS UNIT_NUMBER,
    CAST(Unit_ID AS BIGINT) AS UNIT_ID,
    Unit_Name AS UNIT_NAME,
    Unit_Unique_Name AS UNIT_UNIQUE_NAME,
    CAST(Level_Number AS BIGINT) AS LEVEL_NUMBER,
    CAST(Expiry_Date AS DATE) AS EXPIRY_DATE,
    Expired AS EXPIRED,
    CAST(Parent_Unit_Number AS BIGINT) AS PARENT_UNIT_NUMBER,
    Cost_To_Number AS COST_TO_NUMBER,
    CAST(Location_Number AS BIGINT) AS LOCATION_NUMBER,
    CAST(Head_of_Unit_Number AS BIGINT) AS HEAD_OF_UNIT_NUMBER,
    PAYE_Reference AS PAYE_REFERENCE,
    Allow_Posts AS ALLOW_POSTS,
    Trading_Organisation AS TRASING_ORGANISATION,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.organisation_details
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("UNIT_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "select_hr_Organisation_location",
    comment = "the organisation details specific to the location"
)
@dlt.expect("LOCATION_NUMBER is not null", "LOCATION_NUMBER IS NOT NULL")
def silver_select_hr_organisation_details():

    df = spark.sql(f"""
    SELECT
    Location_Number AS LOCATION_NUMBER,
    Location_Name AS LOCATION_NAME,
    Location_Type AS LOCATION_TYPE,
    Location_Code AS LOCATION_CODE,
    Address_Block AS ADDRESS_BLOCK,
    Address_Line_1 AS ADDRESS_LINE_1,
    Address_Line_2 AS ADDRESS_LINE_2,
    Address_Line_3 AS ADDRESS_LINE_3,
    Address_Line_4 AS ADDRESS_LINE_4,
    Address_Line_5 AS ADDRESS_LINE_5,
    Post_Code AS POST_CODE,
    Phone_Number AS PHONE_NUMBER,
    Fax_Number AS FAX_NUMBER,
    CAST(Expiry_Date AS DATE) AS EXPIRY_DATE,
    Expired AS EXPIRED,
    Locale_Number AS LOCALE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.organisation_locations
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("LOCATION_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df

# COMMAND ----------

@dlt.table(
    name = "select_hr_cost_centre_detail",
    comment = "the details of the cost centres for the organisation"
)
@dlt.expect("COST_TO_NUMBER is not null", "COST_TO_NUMBER IS NOT NULL")
def silver_select_hr_cost_centre_detail():

    df = spark.sql(f"""
    SELECT
    Cost_To_Number AS COST_TO_NUMBER,
    CAST(Cost_To AS INT) AS COST_NO,
    Cost_To_Description AS COST_TO_DESCRIPTION,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.bronze_select_hr_sec.organisation_cost_centre_details
    """)

    # Define the window specification
    windowSpec = dlf.get_window_spec("COST_TO_NUMBER", "MDP_LOAD_DATETIME")

    # Add the CURRENT_FLAG column
    df = dlf.get_row_number(df, windowSpec)

    return df
