# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
 
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
  name = "dim_user",
  comment = "list of users responding to itsm incidents"
)
def gold_dim_user():
    # Execute SQL query to select and transform data
    query = f"""
    SELECT 
    SERVICEREQNO AS BK_USER,
    USER_ID,
    USER_LAST_NAME,
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
    ACCOUNT_NAME,
    ADDED_ZONE,
    UPDATED_ZONE,
    EMAIL_ADDRESS,
    TELEPHONE_NUMBER,
    FAX,
    MOBILE_NUMBER,
    `A#112001126`, --need to rename on LBS confirmation
    EXTENSION,
    JOB_TITLE,
    TITLE,
    NETWORK_LOGON,
    ATTACHEMENTS,
    RETURN_DATE_TIME,
    MAILBOX,
    NO_INC,
    GRAPHIC,
    ASSOCIATION,
    TWITTER_ID,
    CHATTER_ID,
    FACEBOOK_ID,
    LINKEDIN_ID,
    TWITTER_TOKEN,
    TWITTER_SECRET,
    USER_FORENAME,
    CONTACT_ID,
    `H#112001185`,  --need to rename on LBS confirmation
    UNIQUE_ID,
    MDP_LOAD_ID,
    MDP_LOAD_DATETIME,
    ROW_START_DATETIME,
    ROW_END_DATETIME,
    ROW_IS_CURRENT
    FROM {env_var}_catalog.silver_con.sunrise_users
    """
    df = spark.sql(query)

    # Add the ROW_IS_CURRENT column and a surrogate key
    df = df.withColumn("PK_USER", monotonically_increasing_id() +1)

    # Select columns explicitly to make PK_PAYMENTS_NAME_VERIFICATION_AUDIT_RECORDS the first column
    df = df.select(
    "PK_USER",
    "BK_USER",
    "USER_ID",
    "USER_LAST_NAME",
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
    "ACCOUNT_NAME",
    "ADDED_ZONE",
    "UPDATED_ZONE",
    "EMAIL_ADDRESS",
    "TELEPHONE_NUMBER",
    "FAX",
    "MOBILE_NUMBER",
    "A#112001126",
    "EXTENSION",
    "JOB_TITLE",
    "TITLE",
    "NETWORK_LOGON",
    "ATTACHEMENTS",
    "RETURN_DATE_TIME",
    "MAILBOX",
    "NO_INC",
    "GRAPHIC",
    "ASSOCIATION",
    "TWITTER_ID",
    "CHATTER_ID",
    "FACEBOOK_ID",
    "LINKEDIN_ID",
    "TWITTER_TOKEN",
    "TWITTER_SECRET",
    "USER_FORENAME",
    "CONTACT_ID",
    "H#112001185",
    "UNIQUE_ID",
    "MDP_LOAD_ID",
    "MDP_LOAD_DATETIME",
    "ROW_START_DATETIME",
    "ROW_END_DATETIME",
    "ROW_IS_CURRENT"
    )

    return df
