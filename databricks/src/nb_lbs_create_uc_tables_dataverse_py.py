# Databricks notebook source
# DBTITLE 1,Widgets
dbutils.widgets.text(name='catalog_name', defaultValue='')
dbutils.widgets.text(name='bronze_sensitive_adls_name', defaultValue='')
dbutils.widgets.text(name='bronze_non_sensitive_adls_name', defaultValue='')
dbutils.widgets.text(name='date_path', defaultValue='')
dbutils.widgets.text(name='child_pipeline', defaultValue='')
dbutils.widgets.text(name='Environment', defaultValue='')
dbutils.widgets.text(name='common_libs_lts_ver', defaultValue='')
dbutils.widgets.text(name='common_libs_lts_ver_underscore', defaultValue='')
dbutils.widgets.text(name='JobName', defaultValue='')
dbutils.widgets.text(name='LoadId', defaultValue='')

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
env_var = str(dbutils.widgets.get("Environment")).lower()

# lts (long-term-support) common-libs version:
common_libs_lts_ver = dbutils.widgets.get("common_libs_lts_ver")
common_libs_lts_ver_underscore = dbutils.widgets.get("common_libs_lts_ver_underscore")

# beta common-libs version:
common_libs_beta_ver = dbutils.widgets.get("common_libs_beta_ver")
common_libs_beta_ver_underscore = dbutils.widgets.get("common_libs_beta_ver_underscore")

# Pointing at beta version for the initial import of dataverse code into common-libs
mdp_databricks_common_library = f"/Volumes/{env_var}_catalog/common_libraries/mdp_databricks_common_{common_libs_beta_ver_underscore}/mdp_databricks_common-{common_libs_beta_ver}-py3-none-any.whl"

mdp_databricks_common_library_wheelhouse = f"--no-index --find-links '/Volumes/{env_var}_catalog/common_libraries/wheelhouse/' '/Volumes/{env_var}_catalog/common_libraries/mdp_databricks_common_{common_libs_beta_ver_underscore}/mdp_databricks_common-{common_libs_beta_ver}-py3-none-any.whl'"

# COMMAND ----------

# MAGIC %pip uninstall $mdp_databricks_common_library -y 
# MAGIC %pip install $mdp_databricks_common_library_wheelhouse

# COMMAND ----------

import mdp_databricks_common.raw_data_load_functions as dlf
from mdp_databricks_common.database.azure_sql_helper import AzureSQLHelper
from mdp_databricks_common.dataverse.dataverse_metadata_manager import DataverseMetadataManager
from mdp_databricks_common.dataverse.dataverse_metadata_merge_query_builder import DataverseMetadataMergeQueryBuilder
import json

# COMMAND ----------

def execute_schema_evolution_merge_query(query: str) -> None:
    try:

        # env or add workflow control db names to vaults / runtime params.
        env = dbutils.secrets.get(
            scope="data-kv-scope",
            key="data-platform-environment"
        )

        tenant_id = dbutils.secrets.get(
            scope="data-kv-scope",
            key="azure-ad-tenant-id"
        )

        client_id = dbutils.secrets.get(
            scope="data-kv-scope",
            key=f"DataBricksForeignCatlog-az-lbs-data-{env[0]}-mdp-workflow-control-sqldb001001-client-id"
        )

        client_secret = dbutils.secrets.get(
            scope="data-kv-scope",
            key=f"DataBricksForeignCatlog-az-lbs-data-{env[0]}-mdp-workflow-control-sqldb001001"
        )

        database = f"az-lbs-data-{env[0]}-mdp-workflow-control-sqldb001001"
        server = f"az-lbs-data-{env[0]}-sql001.database.windows.net"

        helper = AzureSQLHelper(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            database=database,
            server=server,
        )

        with helper.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                cursor.commit()
    except Exception as ex:
        print("Exception thrown in execute_schema_evolution_merge_query() -- inner details (if available) are...")
        if ex.__cause__:
            print("Inner exception (cause):", repr(ex.__cause__))
        elif ex.__context__:
            print("Inner exception (context):", repr(ex.__context__))
        else:
            print("No inner exception")

        raise Exception("Unable to merge Dataverse schema evolution changes.") from ex

# COMMAND ----------

def execute_dataverse_schema_evolution(dbutils):
    error_message = "No exception thrown"
    
    try:
        # Extract Dataverse schema from model file
        dataverse_extracted_schema_df_list = DataverseMetadataManager.extract_dataverse_schema_from_raw_metadata_file(dbutils)

        # Now merge dataverse metadata into the [Control].[DynamicTaskTargetTableSchema]
        # table in the Workflow Control Database.
        dmq = DataverseMetadataMergeQueryBuilder(dataverse_extracted_schema_df_list)
        execute_schema_evolution_merge_query(
            dmq.build_merge_script()
        )

        overall_success = True

    except Exception as ex:
        overall_success = False
        error_message = f"{ex}. {repr(ex)}"

    return json.dumps(dict(
        success=overall_success,
        message=f"Dataverse Schema Evolution update finished",
        error=f"dataverse error (if an exception was thrown): {error_message}",
    ))

# COMMAND ----------

exit_value = execute_dataverse_schema_evolution(dbutils)

# COMMAND ----------

# DBTITLE 1,Exit notebook
# The notebook.exit() function must be run in a separate cell to the previous exception handling
#  and returns the contents of the exit_value variable
dbutils.notebook.exit(exit_value)
