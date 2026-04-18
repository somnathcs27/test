# Databricks notebook source
# MAGIC %md
# MAGIC # SECRETS

# COMMAND ----------

#####################
# Returns all secret scopes
#####################

import requests
from requests.auth import HTTPBasicAuth

# Read the Parquet file into a DataFrame
pipeline_df = spark.read.parquet("abfss://metadata@azlbsdataddls01.dfs.core.windows.net/access_control/acl_access_control.parquet")

# Filter the DataFrame for rows where ObjectType is 'secrets'
filtered_pipeline_info = pipeline_df.filter(pipeline_df.ObjectType == 'scopes') \
                                    .select("ObjectName", "Principal", "Permission")

# Define Databricks token
databricks_token = dbutils.widgets.get("token")

scope_name_row = filtered_pipeline_info.select("ObjectName", "Principal", "Permission").limit(1).collect()[0]
scope_name = scope_name_row['ObjectName']
principal_name = scope_name_row['Principal']
permission_name = scope_name_row['Permission']

# Define the Databricks instance and API endpoint for listing secret scopes
databricks_instance = "https://adb-7913207224148365.5.azuredatabricks.net"
api_endpoint = "/api/2.0/secrets/scopes/list"

# Make the API request to list secret scopes
response = requests.get(
    f"{databricks_instance}{api_endpoint}", 
    headers={"Authorization": f"Bearer {databricks_token}"}
)

# Check if the request was successful
if response.status_code == 200:
    scopes = response.json()
    display(scopes)
else:
    print(f"Error: {response.status_code}, {response.text}")

# COMMAND ----------

#####################
# Returns all ACLs for selected scope
#####################

import requests
from requests.auth import HTTPBasicAuth

# Define the Databricks instance and API endpoint
databricks_instance = "https://adb-7913207224148365.5.azuredatabricks.net"
api_endpoint = "/api/2.0/secrets/acls/list"

# Define Databricks token
databricks_token = dbutils.widgets.get("token")

# Make the API request to list ACLs
response = requests.get( f"{databricks_instance}{api_endpoint}", headers={"Authorization": f"Bearer {databricks_token}"}, params={"scope": {scope_name}} )

# Check if the request was successful
if response.status_code == 200:
    acls = response.json()
    display(acls)
else:
    print(f"Error: {response.status_code}, {response.text}")

# COMMAND ----------

import requests

# Define the Databricks instance and API endpoint for updating ACLs
databricks_instance = "https://adb-7913207224148365.5.azuredatabricks.net"
api_endpoint = "/api/2.0/secrets/acls/put"

# Define Databricks token
databricks_token = dbutils.widgets.get("token")

# Make the API request to update ACL for the given principal
response = requests.post(
    f"{databricks_instance}{api_endpoint}",
    headers={"Authorization": f"Bearer {databricks_token}"},
    json={
        "scope": scope_name,
        "principal": principal_name,
        "permission": permission_name  # Ensure this is "READ" if you want to set the permission to READ
    }
)

# Check if the request was successful
if response.status_code == 200:
    print("Permissions updated successfully.")
else:
    print(f"Error: {response.status_code}, {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC # PIPELINES

# COMMAND ----------

from pyspark.sql import DataFrame

# Filter the DataFrame for rows where ObjectType is 'pipelines'
filtered_pipeline_info = pipeline_df.filter(pipeline_df.ObjectType == 'pipelines') \
                                    .select("PipelineID", "Principal", "Permission") \
                                    .collect()

# Define the Databricks instance and API endpoint base
databricks_instance = "https://adb-7913207224148365.5.azuredatabricks.net"
api_endpoint_base = "/api/2.0/permissions/pipelines/"

# Define Databricks token
databricks_token = dbutils.widgets.get("token")

# Loop through each record in the filtered list and update permissions
for row in filtered_pipeline_info:
    pipeline_id = row['PipelineID']
    principal_name = row['Principal']
    permission_level = row['Permission']

    # Define the parameters for the permissions update
    params = {
        "access_control_list": [
            {
                "group_name": principal_name,
                "permission_level": permission_level,
                "inherited": False  # Assuming you want to explicitly set permissions, not inherit them
            }
        ]
    }

    # Make the API request to update permissions for the specific pipeline
    response = requests.patch(
        f"{databricks_instance}{api_endpoint_base}{pipeline_id}",
        headers={"Authorization": f"Bearer {databricks_token}"},
        json=params
    )
    
    # Check if the request was successful
    if response.status_code == 200:
        print(f"Permissions updated successfully for pipeline ID: {pipeline_id}")
    else:
        print(f"Error updating permissions for pipeline ID: {pipeline_id}: {response.status_code}, {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC # CLUSTER POLICIES

# COMMAND ----------

# Filter the DataFrame for rows where ObjectType is 'policies'
filtered_policy_info = pipeline_df.filter(pipeline_df.ObjectType == 'policies') \
                                  .select("PipelineID", "Principal", "Permission") \
                                  .collect()

# Define the API endpoint base for cluster policies permissions
policies_api_endpoint = "/api/2.0/policies/clusters/list"
permissions_api_endpoint = "/api/2.0/permissions/cluster-policies"

# Loop through each record in the filtered list and update permissions for cluster policies
for row in filtered_policy_info:
    policy_id = row['PipelineID']  # Assuming PipelineID is used as PolicyID here
    principal_name = row['Principal']
    permission_level = row['Permission']

    # Define the parameters for the permissions update
    params = {
        "access_control_list": [
            {
                "group_name": principal_name,
                "permission_level": permission_level,
                "inherited": False
            }
        ]
    }

    # Make the API request to update permissions for the specific cluster policy
    response = requests.patch(
        f"{databricks_instance}{permissions_api_endpoint}/{policy_id}",
        headers={"Authorization": f"Bearer {databricks_token}"},
        json=params
    )
    
    # Check if the request was successful
    if response.status_code == 200:
        print(f"Permissions updated successfully for cluster policy ID: {policy_id}")
    else:
        print(f"Error updating permissions for cluster policy ID: {policy_id}: {response.status_code}, {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC # JOBS

# COMMAND ----------

from pyspark.sql import DataFrame

# Assuming pipeline_df has already been read from the Parquet file in a previous cell
# Filter the DataFrame for rows where ObjectType is 'jobs'
filtered_job_info = pipeline_df.filter(pipeline_df.ObjectType == 'jobs') \
                               .select("PipelineID", "Principal", "Permission") \
                               .collect()

# Define the Databricks instance and API endpoint base for jobs permissions
databricks_instance = "https://adb-7913207224148365.5.azuredatabricks.net"
permissions_api_endpoint = "/api/2.0/permissions/jobs"

# Define Databricks token
databricks_token = dbutils.widgets.get("token")

# Loop through each record in the filtered list and update permissions for jobs
for row in filtered_job_info:
    job_id = row['PipelineID']  # Assuming PipelineID is used as JobID
    principal_name = row['Principal']
    permission_level = row['Permission']

    # Define the parameters for the permissions update
    params = {
        "access_control_list": [
            {
                "group_name": principal_name,
                "permission_level": permission_level,
                "inherited": False  # Assuming you want to explicitly set permissions, not inherit them
            }
        ]
    }

    # Make the API request to update permissions for the specific job
    response = requests.patch(
        f"{databricks_instance}{permissions_api_endpoint}/{job_id}",
        headers={"Authorization": f"Bearer {databricks_token}"},
        json=params
    )
    
    # Check if the request was successful
    if response.status_code == 200:
        print(f"Permissions updated successfully for job ID: {job_id}")
    else:
        print(f"Error updating permissions for job ID: {job_id}: {response.status_code}, {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC # CLUSTERS

# COMMAND ----------

# Filter the DataFrame for rows where ObjectType is 'clusters'
filtered_cluster_info = pipeline_df.filter(pipeline_df.ObjectType == 'clusters') \
                                    .select("PipelineID", "Principal", "Permission") \
                                    .collect()

# Define the API endpoint base for cluster permissions
permissions_api_endpoint = "/api/2.0/permissions/clusters"

# Loop through each record in the filtered list and update permissions for clusters
for row in filtered_cluster_info:
    cluster_id = row['PipelineID']
    principal_name = row['Principal']
    permission_level = row['Permission']

    # Define the parameters for the permissions update
    params = {
        "access_control_list": [
            {
                "group_name": principal_name,
                "permission_level": permission_level,
                "inherited": False
            }
        ]
    }

    # Make the API request to update permissions for the specific cluster
    response = requests.patch(
        f"{databricks_instance}{permissions_api_endpoint}/{cluster_id}",
        headers={"Authorization": f"Bearer {databricks_token}"},
        json=params
    )
    
    # Check if the request was successful
    if response.status_code == 200:
        print(f"Permissions updated successfully for cluster ID: {cluster_id}")
    else:
        print(f"Error updating permissions for cluster ID: {cluster_id}: {response.status_code}, {response.text}")
