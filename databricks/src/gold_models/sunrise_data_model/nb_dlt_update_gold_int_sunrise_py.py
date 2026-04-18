# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
 
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
  name = "dim_sunrise_incident_state",
  comment = "tbc"
)
def gold_dim_sunrise_incident_state ():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    STATEID AS BK_SUNRISE_INCIDENT_STATE,
    LANGUAGE_ID,
    INCIDENT_STATE,
    INCIDENT_DESCRIPTION,
    ICON,
    TOOL_TIP_TEXT,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_state
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_STATE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_STATE",
    "BK_SUNRISE_INCIDENT_STATE",
    "LANGUAGE_ID",
    "INCIDENT_STATE",
    "INCIDENT_DESCRIPTION",
    "ICON",
    "TOOL_TIP_TEXT",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name = "dim_sunrise_contact",
  comment = "tbc"
)
def gold_dim_sunrise_contact():
    # Execute SQL query to select and transform data
    query = f"""
        SELECT 
        SERVICEREQNO AS BK_SUNRISE_CONTACT,
        CONTACT_ID,
        CONTACT_NAME,
        CONTACT_DESCRIPTION,
        STATE_ID,
        LIFECYCLE_ID,
        ADDED_DATE_TIME,
        ADDED_BY,
        UPDATED_DATE_TIME,
        UPDATED_BY,
        SYS_ACCOUNT_NAME,
        SYS_ACCOUNT_DESC,
        EXTERNAL_ACCOUNT,
        PASSWORD_LAST_CHANGED,
        STATUS_FLAG,
        DELETE_STATUS_FLAG,
        OWNER_ACCOUNT,
        OWNER_GROUP,
        INITIALS,
        TITLE,
        CONTACT_FORENAME,
        PERSONAL_EMAIL,
        BUSINESS_EMAIL,
        TELEPHONE_NUMBER,
        FAX,
        EXTENSION,
        MOBILE_NUMBER,
        JOB_TITLE,
        CATEGORY_1,
        CATEGORY_2,
        START_DATE_TIME,
        END_DATE_TIME,
        FK_DEPARTMENT,
        EMAIL_ADDRESS,
        NETWORK_LOGON,
        PREF_NAME,
        MARITAL_STATUS_FLAG,
        DATE_OF_BIRTH,
        HOME_TELEPHONE_NUMBER,
        CONTACT_ADDRESS,
        RELATIONSHIP,
        ADDED_ZONE,
        UPDATED_ZONE,
        INITIAL_PASSWORD,
        RETURN_DATE_TIME,
        `A#112001135`,
        ATTACHEMENTS,
        VIP,
        MAILBOX,
        GRAPHIC,
        ASSOCIATION,
        PREFEREDCONTACT,
        TWITTER_ID,
        CHATTER_ID,
        OBJECTGUID,
        `A#112001237`,
        STAFF_ID,
        `A#112001243`,
        LINE_MANAGER,
        DTSX_ID,
        DISTINGUISHEDNA,
        `A#112001244`,
        LINE_MANAGER_EMAIL,
        LINEMANDN,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
        FROM {env_var}_catalog.silver_int.sunrise_contact
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_CONTACT", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_CONTACT",
    "BK_SUNRISE_CONTACT",
    "CONTACT_ID",
    "CONTACT_NAME",
    "CONTACT_DESCRIPTION",
    "STATE_ID",
    "LIFECYCLE_ID",
    "ADDED_DATE_TIME",
    "ADDED_BY",
    "UPDATED_DATE_TIME",
    "UPDATED_BY",
    "SYS_ACCOUNT_NAME",
    "SYS_ACCOUNT_DESC",
    "EXTERNAL_ACCOUNT",
    "PASSWORD_LAST_CHANGED",
    "STATUS_FLAG",
    "DELETE_STATUS_FLAG",
    "OWNER_ACCOUNT",
    "OWNER_GROUP",
    "INITIALS",
    "TITLE",
    "CONTACT_FORENAME",
    "PERSONAL_EMAIL",
    "BUSINESS_EMAIL",
    "TELEPHONE_NUMBER",
    "FAX",
    "EXTENSION",
    "MOBILE_NUMBER",
    "JOB_TITLE",
    "CATEGORY_1",
    "CATEGORY_2",
    "START_DATE_TIME",
    "END_DATE_TIME",
    "FK_DEPARTMENT",
    "EMAIL_ADDRESS",
    "NETWORK_LOGON",
    "PREF_NAME",
    "MARITAL_STATUS_FLAG",
    "DATE_OF_BIRTH",
    "HOME_TELEPHONE_NUMBER",
    "CONTACT_ADDRESS",
    "RELATIONSHIP",
    "ADDED_ZONE",
    "UPDATED_ZONE",
    "INITIAL_PASSWORD",
    "RETURN_DATE_TIME",
    "A#112001135",
    "ATTACHEMENTS",
    "VIP",
    "MAILBOX",
    "GRAPHIC",
    "ASSOCIATION",
    "PREFEREDCONTACT",
    "TWITTER_ID",
    "CHATTER_ID",
    "OBJECTGUID",
    "A#112001237",
    "STAFF_ID",
    "A#112001243",
    "LINE_MANAGER",
    "DTSX_ID",
    "DISTINGUISHEDNA",
    "A#112001244",
    "LINE_MANAGER_EMAIL",
    "LINEMANDN",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
  name = "dim_sunrise_team",
  comment = "tbc"
)
def gold_dim_sunrise_team ():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    GROUPID AS BK_SUNRISE_TEAM,
    LANGUAGE_ID,
    TEAM_NAME,
    CASE WHEN TEAM_NAME = 'IT Service Desk' THEN '1st Line'
            WHEN TEAM_NAME IN ('IT Support','Production and Operations','IT Security - Support') THEN '2nd Line'
            ELSE '3rd Line'
    END AS LINE_RESOLUTION,
    TEAM_DESCRIPTION,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_team
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_TEAM", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_TEAM",
    "BK_SUNRISE_TEAM",
    "LANGUAGE_ID",
    "TEAM_NAME",
    "LINE_RESOLUTION",
    "TEAM_DESCRIPTION",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name = "dim_sunrise_department",
  comment = "tbc"
)
def gold_dim_sunrise_department():
    # Execute SQL query to select and transform data
    query = f"""
        SELECT 
        DEPARTMENT_ID AS BK_SUNRISE_DEPARTMENT,
        DEPARTMENT_ID,
        DEPARTMENT_NAME,
        DEPARTMENT_DESCRIPTION,
        STATE_ID,
        LIFECYCLE_ID,
        ADDED_DATE_TIME,
        ADDED_BY,
        UPDATED_DATE_TIME,
        UPDATED_BY,
        SYS_ACCOUNT_NAME,
        SYS_ACCOUNT_DESC,
        EXTERNAL_ACCOUNT,
        PASSWORD_LAST_CHANGED,
        STATUS_FLAG,
        DELETE_STATUS_FLAG,
        OWNER_ACCOUNT,
        OWNER_GROUP,
        SITE,
        ADDRESS_1,
        ADDRESS_2,
        ADDRESS_3,
        ADDRESS_4,
        ADDRESS_5,
        POSTCODE,
        SWITCH_BOARD,
        FAX,
        WEBSITE,
        CATEGORY_1,
        CATEGORY_2,
        COUNTRY,
        `A#112001103`,
        TYPE,
        ADDED_ZONE,
        UPDATED_ZONE,
        ATTACHEMENTS,
        `A#112001163`,
        SLA_USAGE,
        GRAPHIC,
        ASSOCIATION,
        IS_CUSTOMER_FLAG,
        MAP,
        LONGITUDE,
        LATITUDE,
        `A#112001239`,
        `A#112001242`,
        FLOOR,
        GENERAL_MANAGER,
        MDP_LOAD_ID,
        MDP_LOAD_DATETIME,
        ROW_START_DATETIME,
        ROW_END_DATETIME,
        ROW_IS_CURRENT
        FROM {env_var}_catalog.silver_int.sunrise_department
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_DEPARTMENT", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_DEPARTMENT",
    "BK_SUNRISE_DEPARTMENT",
    "DEPARTMENT_ID",
    "DEPARTMENT_NAME",
    "DEPARTMENT_DESCRIPTION",
    "STATE_ID",
    "LIFECYCLE_ID",
    "ADDED_DATE_TIME",
    "ADDED_BY",
    "UPDATED_DATE_TIME",
    "UPDATED_BY",
    "SYS_ACCOUNT_NAME",
    "SYS_ACCOUNT_DESC",
    "EXTERNAL_ACCOUNT",
    "PASSWORD_LAST_CHANGED",
    "STATUS_FLAG",
    "DELETE_STATUS_FLAG",
    "OWNER_ACCOUNT",
    "OWNER_GROUP",
    "SITE",
    "ADDRESS_1",
    "ADDRESS_2",
    "ADDRESS_3",
    "ADDRESS_4",
    "ADDRESS_5",
    "POSTCODE",
    "SWITCH_BOARD",
    "FAX",
    "WEBSITE",
    "CATEGORY_1",
    "CATEGORY_2",
    "COUNTRY",
    "`A#112001103`",
    "TYPE",
    "ADDED_ZONE",
    "UPDATED_ZONE",
    "ATTACHEMENTS",
    "`A#112001163`",
    "SLA_USAGE",
    "GRAPHIC",
    "ASSOCIATION",
    "IS_CUSTOMER_FLAG",
    "MAP",
    "LONGITUDE",
    "LATITUDE",
    "`A#112001239`",
    "`A#112001242`",
    "FLOOR",
    "GENERAL_MANAGER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df

# COMMAND ----------

@dlt.table(
  name = "dim_sunrise_incident_resolve_cause",
  comment = "tbc"
)
def gold_dim_sunrise_incident_resolve_cause():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    FIELDID AS BK_SUNRISE_INCIDENT_RESOLVE_CAUSE,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup where FIELDID = 106013592
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_RESOLVE_CAUSE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_RESOLVE_CAUSE",
    "BK_SUNRISE_INCIDENT_RESOLVE_CAUSE",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name = "dim_sunrise_incident_resolve_type",
  comment = "tbc"
)
def gold_dim_sunrise_incident_resolve_type():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    FIELDID AS BK_SUNRISE_INCIDENT_RESOLVE_TYPE,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup where FIELDID = 106013746
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_RESOLVE_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_RESOLVE_TYPE",
    "BK_SUNRISE_INCIDENT_RESOLVE_TYPE",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_escalation_level",
  comment="tbc"
)
def gold_dim_sunrise_incident_escalation_level():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    FIELDID AS BK_SUNRISE_INCIDENT_ESCALATION_LEVEL,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106009534
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_ESCALATION_LEVEL", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_ESCALATION_LEVEL",
    "BK_SUNRISE_INCIDENT_ESCALATION_LEVEL",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_source",
  comment="tbc"
)
def gold_dim_sunrise_incident_source():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    FIELDID AS BK_SUNRISE_INCIDENT_SOURCE,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106011692
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_SOURCE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_SOURCE",
    "BK_SUNRISE_INCIDENT_SOURCE",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_subcategory",
  comment="tbc"
)
def gold_dim_sunrise_incident_subcategory():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    FIELDID AS BK_SUNRISE_INCIDENT_SUBCATEGORY,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    CONCAT(PARENT_ACTUAL_VALUE, "-", ACTUAL_VALUE) AS INCIDENT_SUB_CATEGORY_KEY,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106010118
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_SUBCATEGORY", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_SUBCATEGORY",
    "BK_SUNRISE_INCIDENT_SUBCATEGORY",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_type",
  comment="tbc"
)
def gold_dim_sunrise_incident_type():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    FIELDID AS BK_SUNRISE_INCIDENT_TYPE,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106009562
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_TYPE",
    "BK_SUNRISE_INCIDENT_TYPE",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_priority",
  comment="tbc"
)
def gold_dim_sunrise_incident_priority():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    FIELDID AS BK_SUNRISE_INCIDENT_PRIORITY,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106009533
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_PRIORITY", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_PRIORITY",
    "BK_SUNRISE_INCIDENT_PRIORITY",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_request_type",
  comment="tbc"
)
def gold_dim_sunrise_incident_request_type():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    FIELDID AS BK_SUNRISE_INCIDENT_REQUEST_TYPE,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106013487
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_REQUEST_TYPE", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_REQUEST_TYPE",
    "BK_SUNRISE_INCIDENT_REQUEST_TYPE",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_feedback",
  comment="tbc"
)
def gold_dim_sunrise_incident_feedback():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    FIELDID AS BK_SUNRISE_INCIDENT_FEEDBACK,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106013597
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_FEEDBACK", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_FEEDBACK",
    "BK_SUNRISE_INCIDENT_FEEDBACK",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_tech_area",
  comment="tbc"
)
def gold_dim_sunrise_incident_tech_area():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    FIELDID AS BK_SUNRISE_INCIDENT_TECH_AREA,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106011322
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_TECH_AREA", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_TECH_AREA",
    "BK_SUNRISE_INCIDENT_TECH_AREA",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name="dim_sunrise_incident_platform",
  comment="tbc"
)
def gold_dim_sunrise_incident_platform():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT
    FIELDID AS BK_SUNRISE_INCIDENT_PLATFORM,
    LANGUAGE_ID,
    ACTUAL_VALUE,
    DESCRIPTION,
    DELETE_FLAG,
    IMAGE_PATH,
    DEFAULT_VALUE,
    SORT_ORDER,
    PARENT_ACTUAL_VALUE,
    PREFIXED_DATA,
    SEQUENCE_NUMBER,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME
    FROM {env_var}_catalog.silver_int.sunrise_lookup WHERE FIELDID = 106011323
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENT_PLATFORM", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENT_PLATFORM",
    "BK_SUNRISE_INCIDENT_PLATFORM",
    "LANGUAGE_ID",
    "ACTUAL_VALUE",
    "DESCRIPTION",
    "DELETE_FLAG",
    "IMAGE_PATH",
    "DEFAULT_VALUE",
    "SORT_ORDER",
    "PARENT_ACTUAL_VALUE",
    "PREFIXED_DATA",
    "SEQUENCE_NUMBER",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df

# COMMAND ----------

@dlt.table(
  name = "fact_sunrise_incidents",
  comment = "tbc"
)
def gold_fact_sunrise_incidents ():
    # Execute SQL query to select and transform data
    query = f"""
        SELECT
        I.SERVICEREQNO AS BK_SUNRISE_INCIDENTS,
        I.INCIDENTS_ID,
        I.INCIDENT_NAME,
        I.CATEGORY_1 AS INCIDENT_TYPE_KEY,
        CONCAT(I.CATEGORY_1, "-", I.CATEGORY_2) AS INCIDENT_CATEGORY_KEY,
        CONCAT(I.CATEGORY_2, "-", I.CATEGORY_3) AS INCIDENT_SUB_CATEGORY_KEY,
        I.CATEGORY_1,
        I.CATEGORY_2,
        I.CATEGORY_3,
        I.ACTIVE_TIME / 1000 as ACTIVE_TIME_IN_SECONDS,
        (I.ACTIVE_TIME / 1000) / 60 /60 as ACTIVE_TIME_IN_HOURS,
        I.`H#SERVICEREQID0` as IMPACTED_COLLEAGUE_REFERENCE,
        CASE
        WHEN (I.SLA = 'SLA000001') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 9 < 1 ) THEN    '< 1 day'
        WHEN (I.SLA = 'SLA000001') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 9 < 3 AND (I.ACTIVE_TIME / 1000) / 60 / 60 / 9 >= 1) THEN    '< 3 day'
        WHEN (I.SLA = 'SLA000001') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 9 < 5 AND (I.ACTIVE_TIME / 1000) / 60 / 60 / 9 >= 3) THEN    '< 5 day'
        WHEN (I.SLA = 'SLA000001') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 9 < 10 AND (I.ACTIVE_TIME / 1000) / 60 / 60 / 9 >= 5) THEN    '< 10 day'
        WHEN (I.SLA = 'SLA000001') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 9 > 10) THEN    '> 10 day'
        WHEN (I.SLA = 'SLA000004') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 8 < 1 ) THEN    '< 1 day'
        WHEN (I.SLA = 'SLA000004') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 8 < 3 AND (I.ACTIVE_TIME / 1000) / 60 / 60 / 8 >= 1) THEN    '< 3 day'
        WHEN (I.SLA = 'SLA000004') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 8 < 5 AND (I.ACTIVE_TIME / 1000) / 60 / 60 / 8 >= 3) THEN    '< 5 day'
        WHEN (I.SLA = 'SLA000004') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 8 < 10 AND (I.ACTIVE_TIME / 1000) / 60 / 60 / 8 >= 5) THEN    '< 10 day'
        WHEN (I.SLA = 'SLA000004') AND ((I.ACTIVE_TIME / 1000) / 60 / 60 / 8 > 10 ) THEN    '> 10 day'
        WHEN cast(I.ADDED_DATE_TIME AS DATE) <= cast(I.RESOLVE_DATE_TIME AS DATE) + 1 THEN    '< 1 day'
        WHEN cast(I.ADDED_DATE_TIME AS DATE) <= cast(I.RESOLVE_DATE_TIME AS DATE) + 3 THEN    '< 3 day'
        WHEN cast(I.ADDED_DATE_TIME AS DATE) <= cast(I.RESOLVE_DATE_TIME AS DATE) + 5 THEN    '< 5 day'
        WHEN cast(I.ADDED_DATE_TIME AS DATE) <= cast(I.RESOLVE_DATE_TIME AS DATE) + 10 THEN    '< 10 day'
        WHEN I.ACTIVE_TIME = 0 OR I.ACTIVE_TIME IS NULL THEN  'N/A'
        ELSE    '> 10 day'
        END  AGE_SLA,
        I.ADDED_BY AS  ADDED_BY_USER_KEY,
        CAST(I.ADDED_DATE_TIME AS DATE) AS ADDED_DATE_KEY,
        DATEPART('hour',I.ADDED_DATE_TIME) AS ADDED_HOUR_OF_DAY,
        I.ADDED_DATE_TIME,
        I.SLA,
        I.OWNER_ACCOUNT AS OWNED_BY_USER_KEY,
        I.GROUP_TEXT AS DEPARTMENT,
        I.PRIORITY AS INCIDENT_PRIORITY_KEY,
        I.ESCALATION_LEVEL AS INCIDENT_ESCULATION_LEVEL_KEY,
        I.CONTACT_DEPARTMENT_NAME AS AFFECTED_DEPARTMENT,
        I.STATE_ID AS INCIDENT_STATE_KEY,
        CASE WHEN I.FAST_FIX = 1 THEN 'Y' ELSE 'N' END AS QUICK_CLOSE,
        CAST(I.RESOLVE_DATE_TIME AS DATE) RESOLVED_DATE_KEY,
        I.RESOLVE_DATE_TIME,
        DATEPART('hour',I.RESOLVE_DATE_TIME) AS RESOLVED_HOUR_OF_DATE,
        I.RESOLVED_BY_2 AS RESOLVED_BY_TEAM_KEY,
        I.RESOLVED_BY_1 AS RESOLVED_BY_USER_KEY,
        I.TECHNICAL_RES as TECHNICAL_RESOLUTION,
        I.RESOLVE_CAUSE_TYPE AS RESOLVE_CAUSE_TYPE_KEY,
        I.RESOLVE_TYPE_1 AS RESOLVE_TYPE_KEY,
        CAST(I.UPDATED_DATE_TIME AS DATE) LAST_UPDATE_DATE_KEY,
        DATEPART('hour',I.UPDATED_DATE_TIME) as LAST_UPDATE_HOUR,
        I.UPDATED_DATE_TIME AS LAST_UPDATE_DATE_TIME,
        datediff(MONTH, getdate(), I.RESOLVE_DATE_TIME) AS RESOLVE_DATE_SHOW,
        datediff(MONTH, getdate(), I.ADDED_DATE_TIME) AS ADDED_DATE_SHOW,
        CASE WHEN I.COMMS_SLA_BRCH_CNT = 0 THEN 'Pass' ELSE 'Fail' END AS COMMS_SLA,
        CASE WHEN I.TRIAGE_SLA_BREACH_COUNT = 0 THEN 'Pass' ELSE 'Fail' END AS TRIAGE_SLA,
        I.INCIDENT_SOURCE AS INCIDENT_SOURCE_KEY,
        I.REQUEST_TYPE AS INCIDENT_REQUEST_TYPE_KEY,
        I.FEEDBACK AS INCIDENT_FEEDBACK_KEY,
        CASE WHEN (datediff(HOUR, I.ADDED_DATE_TIME, INITTRI.INITTRIDATE)) > 1  THEN 'N' ELSE 'Y' END AS INITIAL_TRIAGE_HIT,
        I.BREACH_GROUP,
        I.BREACH_ASSIGNEE,
        SERV.TYPE AS TECH_AREA_KEY,
        CONCAT(SERV.TYPE, "-", SERV.SUB_TYPE) AS PLATFORM_KEY,
        CUSTESC.TOTAL AS CUSTOMER_ESCALATION_COUNT,
        I.BOUNCE_COUNT as BOUNCE_COUNT,
        CUSTUPD.TOTAL as CUSTOMER_UPDATE_REQUEST_COUNT,
        I.RESOLUTION_TARGET_DATE,
        I.RESPONSE_TARGET_DATE,
        I.NSF_START_DATE as NEW_STARTER_DATE,
        I.MDP_LOAD_ID,
        I.MDP_LOAD_DATETIME
        FROM {env_var}_catalog.silver_con.sunrise_incidents AS I
        LEFT OUTER JOIN ( --subquery to add Initial Triage Date into fact table
        SELECT 
        IH.INCIDENT_HISTORY_NO,
        min(ADDED_DATE_TIME) AS INITTRIDATE
        FROM  
        {env_var}_catalog.silver_int.sunrise_incident_history AS IH 
        WHERE
        IH.OPERATION_ID IN (
        102005161 --Close Incident
        , 102005165 --Update
        , 102005166 --Assign Incident
        , 102005259 --Search Problem Database
        , 102005479 --Update Priority
        , 102005922 --Master Incident Association
        , 102005159 --Hold Incident
        , 102005160 --Release Incident
        , 102005169 --Supplier Close
        , 102005170 --Supplier Abort
        , 102005904 --Set Master Incident
        , 102005911 --Authorise Request
        , 102005925 --Non Standard Service Request
        , 102005162 --Reopen Incident
        , 102005167 --Assign Supplier
        , 102005199 --Cancel Resolution
        , 102005315 --Initiate Problem
        , 102005485 --Assign To Me
        , 102005553 --Initiate Change
        , 102005617 --Initiate Incident
        , 102005923 --Quality Assurance
        , 102005991 --BYODtask
        , 102005168 --Supplier Action
        , 102005197 --Resolve Incident
        , 102005288 --Search Change Database
        , 102005906 --Remove Master Incident
        , 102005990 --Service Outage Review
        )
        AND IH.ACTION_DETAILS <> 'Incident has been assigned to user:  in IT Service Desk.  Auto Assigned On Open'
        group by
        IH.INCIDENT_HISTORY_NO
        ) as INITTRI on INITTRI.INCIDENT_HISTORY_NO = I.SERVICEREQNO 
        LEFT OUTER JOIN {env_var}_catalog.silver_int.sunrise_map AS MAP ON MAP.CATEGORY_1 = I.CATEGORY_1 AND MAP.CATEGORY_2 = I.CATEGORY_2 AND MAP.CATEGORY_3 = I.CATEGORY_3
        LEFT OUTER JOIN {env_var}_catalog.silver_int.sunrise_service AS SERV on SERV.SERVICEREQNO = MAP.SERVICE_ID
        LEFT OUTER JOIN --subquery to add Customer Escalation Count into fact table
        (
        SELECT 
        IH.INCIDENT_HISTORY_NO
        , count(*) TOTAL
        FROM 
        {env_var}_catalog.silver_int.sunrise_incident_history as IH
        WHERE 
        OPERATION_ID = 102005165
        AND INT_ACTION_TYPE = 13
        GROUP BY
        IH.INCIDENT_HISTORY_NO
        ) AS CUSTESC ON CUSTESC.INCIDENT_HISTORY_NO = I.SERVICEREQNO
        LEFT OUTER JOIN  --subquery to add Customer Update Request Count into fact table
        (
        SELECT 
        IH.INCIDENT_HISTORY_NO
        , count(*) TOTAL
        FROM 
        {env_var}_catalog.silver_int.sunrise_incident_history as IH
        WHERE 
        OPERATION_ID = 102005165
        AND INT_ACTION_TYPE = 16
        GROUP BY
        IH.INCIDENT_HISTORY_NO
        ) AS CUSTUPD ON CUSTUPD.INCIDENT_HISTORY_NO = I.SERVICEREQNO
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_SUNRISE_INCIDENTS", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_SUNRISE_INCIDENTS",
    "BK_SUNRISE_INCIDENTS",
    "INCIDENTS_ID",
    "INCIDENT_NAME",
    "INCIDENT_TYPE_KEY",
    "INCIDENT_CATEGORY_KEY",
    "INCIDENT_SUB_CATEGORY_KEY",
    "CATEGORY_1",
    "CATEGORY_2",
    "CATEGORY_3",
    "ACTIVE_TIME_IN_SECONDS",
    "ACTIVE_TIME_IN_HOURS",
    "IMPACTED_COLLEAGUE_REFERENCE",
    "AGE_SLA",
    "ADDED_BY_USER_KEY",
    "ADDED_DATE_KEY",
    "ADDED_HOUR_OF_DAY",
    "ADDED_DATE_TIME",
    "SLA",
    "OWNED_BY_USER_KEY",
    "DEPARTMENT",
    "INCIDENT_PRIORITY_KEY",
    "INCIDENT_ESCULATION_LEVEL_KEY",
    "AFFECTED_DEPARTMENT",
    "INCIDENT_STATE_KEY",
    "QUICK_CLOSE",
    "RESOLVED_DATE_KEY",
    "RESOLVE_DATE_TIME",
    "RESOLVED_HOUR_OF_DATE",
    "RESOLVED_BY_TEAM_KEY",
    "RESOLVED_BY_USER_KEY",
    "TECHNICAL_RESOLUTION",
    "RESOLVE_CAUSE_TYPE_KEY",
    "RESOLVE_TYPE_KEY",
    "LAST_UPDATE_DATE_KEY",
    "LAST_UPDATE_HOUR",
    "LAST_UPDATE_DATE_TIME",
    "RESOLVE_DATE_SHOW",
    "ADDED_DATE_SHOW",
    "COMMS_SLA",
    "TRIAGE_SLA",
    "INCIDENT_SOURCE_KEY",
    "INCIDENT_REQUEST_TYPE_KEY",
    "INCIDENT_FEEDBACK_KEY",
    "INITIAL_TRIAGE_HIT",
    "BREACH_GROUP",
    "BREACH_ASSIGNEE",
    "TECH_AREA_KEY",
    "PLATFORM_KEY",
    "CUSTOMER_ESCALATION_COUNT",
    "BOUNCE_COUNT",
    "CUSTOMER_UPDATE_REQUEST_COUNT",
    "RESOLUTION_TARGET_DATE",
    "RESPONSE_TARGET_DATE",
    "NEW_STARTER_DATE",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME"
    )

    return df
