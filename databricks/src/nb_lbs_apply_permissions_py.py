# Databricks notebook source
# DBTITLE 1,Import statements
import json

from pyspark.sql.types import Row 

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("bronze_non_sensitive_adls_name", "")

# COMMAND ----------

# DBTITLE 1,Constants
DEFAULT_CATALOG = dbutils.widgets.get("catalog_name")
DEFAULT_ADLS_NAME = dbutils.widgets.get("bronze_non_sensitive_adls_name")

# COMMAND ----------

def create_etl_table(datalake_name: str, catalog:str) -> None:
    """Creates an external table in Unity Catalog for the ETL metadata.

    Creates an external table named ext_etl_access_control located in the specified
     catalog. The external table contains the metadata required to grant the
     required privileges to the Spark tables in Unity Catalog.

    Args:
        datalake_name (str): The name of the datalake where the file is stored.
        catalog (str): The name of the catalog where the table should be created.
    
    Returns:
        None
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS etl")
    spark.sql("USE SCHEMA etl")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.etl.ext_etl_access_control
              USING PARQUET
              LOCATION 'abfss://metadata@{datalake_name}.dfs.core.windows.net/access_control/access_control.parquet';
              """)
    

    return

# COMMAND ----------

def apply_access_permissions(row_data: Row, catalog_name: str) -> bool:
    """Grants or revokes object level access in Unity Catalog entities.

    Accepts a pyspark.sql.row object (a row from a Spark dataframe) and
     uses the metadata to build f-strings for Spark SQL commands. The 
     commands can grant or revoke access to catalog, schema and table
     entities in Unity Catalog. The function returns a True or False
     value indicating if the operation was successful. If the object
     type is a value other than catalog, schema or table then a value
     of False is returned.

    Args:
        row_data (PySpark Row): A PySpark Row object containing the multiple
            properties required to apply access control.
        catalog_name (str): The name of the Spark catalog to use.
    
    Returns:
        is_success (bool): The success status of the operation.
    """
    try:
        auth_type = row_data.AuthType
        privilege = row_data.PrivilegeType
        object_type = row_data.ObjectType
        schema_name = row_data.ObjectSchema
        object_name = row_data.ObjectTableName
        user_principal = row_data.RelativePrincipalName

        # Construct a Spark SQL command based on the object type
        if object_type == "CATALOG":
            spark.sql(f"{auth_type} {privilege} ON {object_type} {catalog_name} TO {user_principal}")
        elif object_type == "SCHEMA":
            spark.sql(f"{auth_type} {privilege} ON {object_type} {catalog_name}.{schema_name} TO {user_principal}")
        elif object_type == "TABLE":
            spark.sql(f"{auth_type} {privilege} ON {object_type} {catalog_name}.{schema_name}.{object_name} TO {user_principal}")
        else:
            return False
        
        is_success = True
    except Exception as ex:
        is_success = False

    
    return is_success

# COMMAND ----------

# DBTITLE 1,Main workflow
# Declare the exit_value variable outside of the exception handling so it can be accesed
#  in both the try and the except block
exit_value = []

try:
    # Create the ETL table if not exists
    create_etl_table(datalake_name=DEFAULT_ADLS_NAME, catalog=DEFAULT_CATALOG)

    # Get all rows from the ETL metadata which must be processed
    access_control_df = spark.sql(f"SELECT * FROM {DEFAULT_CATALOG}.etl.ext_etl_access_control")
    access_control = access_control_df.collect()

    uc_operations = []

    for row in access_control:
        # Apply any required permissions to the specified entity
        is_success = apply_access_permissions(row_data=row, catalog_name=DEFAULT_CATALOG)

        # Log success status
        uc_operation = {
            "entityID" : row.AccessControlID,
            "success" : is_success
        }

        # Append log to list
        uc_operations.append(uc_operation)
    
    response = {
        "success" : True,
        "message" : f"Notebook nb_lbs_apply_permissions_py succeeded",
        "results" : uc_operations
    }

    # Return response as a JSON object
    exit_value = json.dumps(response)
except Exception as ex:
    response = {
        "success" : False,
        "message" : f"Notebook nb_lbs_apply_permissions_py failed with error: {ex}",
        "results" : []
    }

    exit_value = json.dumps(response)

# COMMAND ----------

# DBTITLE 1,Exit notebook
# The notebook.exit() function must be run in a separate cell to the previous exception handling
#  and returns the contents of the exit_value variable
dbutils.notebook.exit(exit_value)
