# Databricks notebook source
import dlt
import pyspark.sql.functions as F
import pyspark.sql.types as T
import mdp_databricks_common.silver_layer_functions as dlf
from typing import Optional

env_var = dbutils.secrets.get(scope="data-kv-scope", key="data-platform-environment")
source_schema = f"{env_var}_catalog.bronze_power_bi_metadata_int"
target_schema = f"{env_var}_catalog.silver_int"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC |||
# MAGIC |---|---|
# MAGIC | ![asd](https://upload.wikimedia.org/wikipedia/en/d/d8/Windows_11_Clippy_paperclip_emoji.png)    | This set of DLTs uses the Microsoft schema information for Office 365 to try to generally capture all of the flexible (mandatory, non-mandatory) fields possible in the underlying source datasets. That information is available [here](https://learn.microsoft.com/en-us/office/office-365-management-api/office-365-management-activity-api-schema#common-schema), for common schema components, and [here](https://learn.microsoft.com/en-us/office/office-365-management-api/office-365-management-activity-api-schema#power-bi-schema) for Power BI schema addins. <br><br>Simple schema inference isn't enough, as each object in the API response can have a different schema - so we need a viable _superset_ of fields, which every record conforms to, and not a _subset_ corresponding to the footprint of the first (or first few) records search by a schema inference routine. |
# MAGIC |||

# COMMAND ----------

# MAGIC %md
# MAGIC _helpers_
# MAGIC
# MAGIC `handle_json` is a helper function which points at a json-string-valued column, casts to the appropriate Spark complex type, and then spreads the constituent columns.

# COMMAND ----------


def handle_json(
    source_table: str, json_column_name: str, schema: T.StructType
) -> F.DataFrame:
    """
    handle_json
    ===========

    A helper function: takes a source table, json column name and a schema to use to explode and split it.

    Assumes json column holds an array and schema has signature matching that array.

    This is a common libs utility function candidate, if it sees wider usage.
    """

    bare_df = spark.sql(f"SELECT * FROM {source_table}")

    result_df = bare_df.select(
        F.explode(F.from_json(F.col(json_column_name), schema)).alias("root_json"),
        *[
            F.col(f"root_json.{field.name}").alias(field.name.upper())
            for field in schema.elementType.fields
        ],
        "MDP_LOAD_ID",
        "MDP_LOAD_DATETIME",
    ).drop("root_json")

    return result_df


def describe(source_table: str) -> str:
    # simple helper for tables in this source/target schema.
    return f"Data from the {source_table} table from Power BI Metadata in Bronze; exploded and standardised."


def create_dlt(
    source_table_path: str,
    source_json_column_name: str,
    source_json_schema: T.StructType,
    target_table_name: str,
    dict_expects: dict[str, str],
    target_desc: str,
    col_renames: dict[str, str],
    row_is_current_filters: list[str] = [],
    row_is_current_order: str = "MDP_LOAD_ID",
) -> None:
    """
    create_dlt
    ===========

    Standardised creation script: does all of the bespoke transformation within, and handles all of the boilerplate.
    """

    @dlt.table(name=target_table_name, comment=target_desc)
    @dlt.expect_all(dict_expects)
    def get_target_from_json():
        df = handle_json(
            f"{source_table_path}", source_json_column_name, source_json_schema
        )
        for old_name, new_name in col_renames.items():
            df = df.withColumnRenamed(old_name, new_name)
        if len(row_is_current_filters) > 0:
            windowSpec = dlf.get_window_spec(
                row_is_current_filters, row_is_current_order
            )
            df = dlf.get_row_number(df, windowSpec)
        return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Activity Events

# COMMAND ----------

# Microsoft O365 base schema: with all non-mandatory fields set nullable.
common_schema: T.StructType = T.StructType(
    [
        T.StructField("Id", T.StringType(), False),
        T.StructField("RecordType", T.IntegerType(), False),
        T.StructField("CreationTime", T.TimestampType(), False),
        T.StructField("Operation", T.StringType(), False),
        T.StructField("OrganizationId", T.StringType(), False),
        T.StructField("UserType", T.IntegerType(), False),
        T.StructField("UserKey", T.StringType(), False),
        T.StructField("Workload", T.StringType(), False),
        T.StructField("ResultStatus", T.StringType(), True),
        T.StructField("ObjectId", T.StringType(), True),
        T.StructField("UserId", T.StringType(), False),
        T.StructField("ClientIP", T.StringType(), False),
        T.StructField("Scope", T.IntegerType(), True),
        # This is a complex type; don't know if we see it, if we do; need to map
        # T.StructField("AppAccessContext", T.StringType(), False),
    ]
)

# Extensions to the common audit schema for Power BI.
power_bi_audit_schema: T.StructType = T.StructType(
    [
        T.StructField("AppName", T.StringType(), True),
        T.StructField("DashboardName", T.StringType(), True),
        T.StructField("DataClassification", T.StringType(), True),
        T.StructField("DatasetName", T.StringType(), True),
        # T.StructField("MembershipInformation", T.StringType(), True),
        T.StructField("OrgAppPermission", T.StringType(), True),
        T.StructField("ReportName", T.StringType(), True),
        # T.StructField("SharingInformation", T.StringType(), True),
        T.StructField("SwitchState", T.StringType(), True),
        T.StructField("WorkSpaceName", T.StringType(), True),
    ]
)

# The Power BI specific schema for audit events is a combination of the base audit schema plus the Power BI.
power_bi_schema: T.StructType = common_schema
for column in power_bi_audit_schema.fields:
    common_schema.add(column)

# ... and we don't just have one of these - but an array.
power_bi_schema = T.ArrayType(power_bi_schema)

# DQ expectations.
power_bi_expects: dict[str, str] = {
    "ACTIVITY_EVENT_ID NOT NULL": "ACTIVITY_EVENT_ID IS NOT NULL",
    "RECORD_TYPE_VALID": "RECORD_TYPE = 20",
    "USER_TYPE_VALID": "USER_TYPE >= 0",
    "SCOPE_VALUD": "SCOPE IS NULL OR SCOPE >= 0",
}

# Column name conventions.
power_bi_renames: dict[str, str] = {
    "ID": "ACTIVITY_EVENT_ID",
    "RECORDTYPE": "RECORD_TYPE",
    "CREATIONTIME": "CREATION_DATETIME",
    "ORGANIZATIONID": "ORGANIZATION_ID",
    "USERTYPE": "USER_TYPE",
    "USERKEY": "USER_KEY",
    "RESULTSTATUS": "RESULT_STATUS",
    "OBJECTID": "OBJECT_ID",
    "USERID": "USER_ID",
    "CLIENTIP": "CLIENT_IP",
    "APPNAME": "APP_NAME",
    "DASHBOARDNAME": "DASHBOARD_NAME",
    "DATACLASSIFICATION": "DATA_CLASSIFICATION",
    "DATASETNAME": "DATASET_NAME",
    "ORGAPPACCESSCONTEXT": "ORG_APP_ACCESS_CONTEXT",
    "ORGAPPPERMISSION": "ORG_APP_PERMISSION",
    "REPORTNAME": "REPORT_NAME",
    "SWITCHSTATE": "SWITCH_STATE",
    "WORKSPACENAME": "WORKSPACE_NAME",
}

create_dlt(
    source_table_path=f"{source_schema}.activityevents",
    source_json_column_name="activityEventEntities",
    source_json_schema=power_bi_schema,
    target_table_name="power_bi_metadata_activity_events",
    dict_expects=power_bi_expects,
    target_desc=describe("activityevents"),
    col_renames=power_bi_renames,
    row_is_current_filters=["ACTIVITY_EVENT_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apps

# COMMAND ----------

# docs @ https://learn.microsoft.com/en-us/rest/api/power-bi/admin/apps-get-apps-as-admin#adminapp
app_schema: T.StructType = T.ArrayType(
    T.StructType(
        [
            T.StructField("id", T.StringType(), False),
            T.StructField("lastUpdate", T.TimestampType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("publishedBy", T.StringType(), True),
            T.StructField("workspaceId", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
        ]
    )
)

# DQ expectations.
app_expects: dict[str, str] = {
    "APP_ID_NOT_NULL": "APP_ID IS NOT NULL",
}

# Column name conventions.
app_renames: dict[str, str] = {
    "ID": "APP_ID",
    "LASTUPDATE": "LAST_UPDATE_DATETIME",
    "NAME": "APP_NAME",
    "PUBLISHEDBY": "PUBLISHED_BY",
    "WORKSPACEID": "WORKSPACE_ID",
}

create_dlt(
    source_table_path=f"{source_schema}.apps",
    source_json_column_name="value",
    source_json_schema=app_schema,
    target_table_name="power_bi_metadata_apps",
    dict_expects=app_expects,
    target_desc=describe("apps"),
    col_renames=app_renames,
    row_is_current_filters=["APP_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Dashboards

# COMMAND ----------

# docs @ https://learn.microsoft.com/en-us/rest/api/power-bi/admin/dashboards-get-dashboards-as-admin#admindashboard
dashboard_schema: T.StructType = T.ArrayType(
    T.StructType(
        [
            T.StructField("appId", T.StringType(), True),
            T.StructField("displayName", T.StringType(), True),
            T.StructField("embedUrl", T.StringType(), True),
            T.StructField("id", T.StringType(), True),
            T.StructField("isReadOnly", T.StringType(), True),
            # T.StructField("subscriptions", T.StringType(), True),
            # T.StructField("tiles", T.StringType(), True),
            # T.StructField("users", T.StringType(), True),
            T.StructField("webUrl", T.StringType(), True),
            T.StructField("workspaceId", T.StringType(), True),
        ]
    )
)

# DQ expectations.
dashboard_expects: dict[str, str] = {
    "DASHBOARD_ID_NOT_NULL": "DASHBOARD_ID IS NOT NULL",
}

# Column name conventions.
dashboard_renames: dict[str, str] = {
    "ID": "DASHBOARD_ID",
    "APPID": "APP_ID",
    "LASTUPDATE": "LAST_UPDATE_DATETIME",
    "DISPLAYNAME": "DISPLAY_NAME",
    "ISREADONLY": "IS_READ_ONLY",
    "WEBURL": "WEB_URL",
    "EMBEDURL": "EMBED_URL",
    "WORKSPACEID": "WORKSPACE_ID",
}

create_dlt(
    source_table_path=f"{source_schema}.dashboards",
    source_json_column_name="value",
    source_json_schema=dashboard_schema,
    target_table_name="power_bi_metadata_dashboards",
    dict_expects=dashboard_expects,
    target_desc=describe("dashboards"),
    col_renames=dashboard_renames,
    row_is_current_filters=["DASHBOARD_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Datasets

# COMMAND ----------

# docs @ https://learn.microsoft.com/en-us/rest/api/power-bi/admin/datasets-get-datasets-as-admin#admindataset
dataset_schema: T.StructType = T.ArrayType(
    T.StructType(
        [
            T.StructField("ContentProviderType", T.StringType(), True),
            T.StructField("Encryption", T.StringType(), True),
            T.StructField("IsEffectiveIdentityRequired", T.BooleanType(), True),
            T.StructField("IsEffectiveIdentityRolesRequired", T.BooleanType(), True),
            T.StructField("IsInPlaceSharingEnabled", T.BooleanType(), True),
            T.StructField("IsOnPremGatewayRequired", T.BooleanType(), True),
            T.StructField("IsRefreshable", T.BooleanType(), True),
            T.StructField("addRowsAPIEnabled", T.BooleanType(), True),
            T.StructField("configuredBy", T.StringType(), True),
            T.StructField("createReportEmbedURL", T.StringType(), True),
            T.StructField("createdDate", T.TimestampType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("id", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("qnaEmbedURL", T.StringType(), True),
            # T.StructField("queryScaleOutSettings", T.StringType(), True),
            T.StructField("targetStorageMode", T.StringType(), True),
            # T.StructField("upstreamDataflows", T.StringType(), True),
            # T.StructField("users", T.StringType(), True),
            T.StructField("webUrl", T.StringType(), True),
            T.StructField("workspaceId", T.StringType(), True),
        ]
    )
)

# DQ expectations.
dataset_expects: dict[str, str] = {
    "DATASET_ID_NOT_NULL": "DATASET_ID IS NOT NULL",
}

# Column name conventions.
dataset_renames: dict[str, str] = {
    "ID": "DATASET_ID",
    "CONTENTPROVIDERTYPE": "CONTENT_PROVIDER_TYPE",
    "ISEFFECTIVEIDENTITYREQUIRED": "IS_EFFECTIVE_IDENTITY_REQUIRED",
    "ISEFFECTIVEIDENTITYROLESREQUIRED": "IS_EFFECTIVE_IDENTITY_ROLES_REQUIRED",
    "ISINPLACESHARINGENABLED": "IS_IN_PLACE_SHARING_ENABLED",
    "ISONPREMGATEWAYREQUIRED": "IS_ON_PREM_GATEWAY_REQUIRED",
    "ISREFRESHABLE": "IS_REFRESHABLE",
    "ADDROWSAPIENABLED": "ADD_ROWS_API_ENABLED",
    "CONFIGUREDBY": "CONFIGURED_BY",
    "CREATEREPORTEMBEDURL": "CREATE_REPORT_EMBED_URL",
    "CREATEDDATE": "CREATED_DATETIME",
    "QNAEMBEDURL": "QNA_EMBED_URL",
    "TARGETSTORAGEMODE": "TARGET_STORAGE_MODE",
    "WEBURL": "WEB_URL",
    "WORKSPACEID": "WORKSPACE_ID",
}

create_dlt(
    source_table_path=f"{source_schema}.datasets",
    source_json_column_name="value",
    source_json_schema=dataset_schema,
    target_table_name="power_bi_metadata_datasets",
    dict_expects=dataset_expects,
    target_desc=describe("datasets"),
    col_renames=dataset_renames,
    row_is_current_filters=["DATASET_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Groups

# COMMAND ----------

# docs @ https://learn.microsoft.com/en-us/rest/api/power-bi/admin/groups-get-groups-as-admin#admingroup
group_schema: T.StructType = T.ArrayType(
    T.StructType(
        [
            T.StructField("capacityId", T.StringType(), True),
            # T.StructField("dashboards", T.StringType(), True),
            T.StructField("dataflowStorageId", T.StringType(), True),
            # T.StructField("dataflows", T.StringType(), True),
            # T.StructField("datasets", T.StringType(), True),
            # T.StructField("defaultDatasetStorageFormat", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("hasWorkspaceLevelSettings", T.BooleanType(), True),
            T.StructField("id", T.StringType(), True),
            T.StructField("isOnDedicatedCapacity", T.BooleanType(), True),
            T.StructField("isReadOnly", T.BooleanType(), True),
            # T.StructField("logAnalyticsWorkspace", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("isReadOnlypipelineId", T.BooleanType(), True),
            # T.StructField("reports", T.StringType(), True),
            T.StructField("state", T.StringType(), True),
            T.StructField("type", T.StringType(), True),
            # T.StructField("users", T.BooleanType(), True),
            # T.StructField("workbooks", T.BooleanType(), True),
        ]
    )
)

# DQ expectations.
group_expects: dict[str, str] = {
    "GROUP_ID_NOT_NULL": "GROUP_ID IS NOT NULL",
}

# Column name conventions.
group_renames: dict[str, str] = {
    "ID": "GROUP_ID",
    "CAPACITYID": "CAPACITY_ID",
    "DATAFLOWSTORAGEID": "DATAFLOW_STORAGE_ID",
    "HASWORKSPACELEVELSETTINGS": "HAS_WORKSPACE_LEVEL_SETTINGS",
    "ISONDEDICATEDCAPACITY": "IS_ON_DEDICATED_CAPACITY",
    "ISREADONLY": "IS_READ_ONLY",
    "ISREADONLYPIPELINEID": "IS_READ_ONLY_PIPELINE_ID",
}

create_dlt(
    source_table_path=f"{source_schema}.groups",
    source_json_column_name="value",
    source_json_schema=group_schema,
    target_table_name="power_bi_metadata_groups",
    dict_expects=group_expects,
    target_desc=describe("groups"),
    col_renames=group_renames,
    row_is_current_filters=["GROUP_ID"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reports

# COMMAND ----------

# docs @ https://learn.microsoft.com/en-us/rest/api/power-bi/admin/reports-get-reports-as-admin#adminreport
report_schema: T.StructType = T.ArrayType(
    T.StructType(
        [
            T.StructField("appId", T.StringType(), True),
            T.StructField("createdBy", T.StringType(), True),
            T.StructField("createdDateTime", T.TimestampType(), True),
            T.StructField("datasetId", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("embedUrl", T.StringType(), True),
            T.StructField("id", T.StringType(), True),
            T.StructField("isOwnedByMe", T.StringType(), True),
            T.StructField("modifiedBy", T.StringType(), True),
            T.StructField("modifiedDateTime", T.TimestampType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("originalReportId", T.StringType(), True),
            T.StructField("reportType", T.StringType(), True),
            # T.StructField("subscriptions", T.StringType(), True),
            # T.StructField("users", T.StringType(), True),
            T.StructField("webUrl", T.StringType(), True),
            T.StructField("workspaceId", T.StringType(), True),
        ]
    )
)

# DQ expectations.
report_expects: dict[str, str] = {
    "REPORT_ID_NOT_NULL": "REPORT_ID IS NOT NULL",
}

# Column name conventions.
report_renames: dict[str, str] = {
    "ID": "REPORT_ID",
    "APPID": "APP_ID",
    "CREATEDBY": "CREATED_BY",
    "CREATEDDATETIME": "CREATED_DATETIME",
    "DATASETID": "DATASET_ID",
    "DESCRIPTION": "DESCRIPTION",
    "EMBEDURL": "EMBED_URL",
    "ISOWNEDBYME": "IS_OWNED_BY_ME",
    "MODIFIEDBY": "MODIFIED_BY",
    "MODIFIEDDATETIME": "MODIFIED_DATETIME",
    "ORIGINALREPORTID": "ORIGINAL_REPORT_ID",
    "REPORTTYPE": "REPORT_TYPE",
    "WEBURL": "WEB_URL",
    "WORKSPACEID": "WORKSPACE_ID",
}

create_dlt(
    source_table_path=f"{source_schema}.reports",
    source_json_column_name="value",
    source_json_schema=report_schema,
    target_table_name="power_bi_metadata_reports",
    dict_expects=report_expects,
    target_desc=describe("reports"),
    col_renames=report_renames,
    row_is_current_filters=["REPORT_ID"],
)

