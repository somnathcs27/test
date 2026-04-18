# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Cluster Mount Points & Delta Database
# MAGIC <p><b>Description: </b>Required to be run once when a cluster is first created</p>
# MAGIC <b>Parent Process: </b>n/a
# MAGIC <table class="table table-striped table-hover">
# MAGIC  <thead>
# MAGIC   <tr>
# MAGIC    <th>Contributor</th>
# MAGIC    <th>Date</th>
# MAGIC    <th>Version</th>
# MAGIC    <th>Comment</th>
# MAGIC    <th>WorkItem No</th>
# MAGIC   </tr>
# MAGIC  </thead>
# MAGIC  <tbody>
# MAGIC   <tr>
# MAGIC    <td>ANS Sigma Team</td>
# MAGIC    <td>2022-12-09</td>
# MAGIC    <td>1.0</td>
# MAGIC    <td>Create initial release</td>
# MAGIC    <td><a href="https://dev.azure.com/my_org/my_project/_workitems/edit/12345">12345</a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC    <td></td>
# MAGIC    <td></td>
# MAGIC    <td>1.1</td>
# MAGIC    <td></td>
# MAGIC    <td><a href="https://dev.azure.com/DwrCymru/Libra%20Replacement/_workitems/edit/67899">67899</a></td>
# MAGIC   </tr>
# MAGIC  </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Shared Parameters / Variables

# COMMAND ----------

# Set secret scope to be used
secret_scope = 'data-kv-scope'

# Aquire keyvault parameters
databricks_sp_clientid = dbutils.secrets.get(scope = secret_scope, key = 'aad-sp-data-databricks-clientid')
databricks_sp_secret = dbutils.secrets.get(scope = secret_scope, key = 'aad-sp-data-databricks-secret')
azure_ad_tenant_id = dbutils.secrets.get(scope = secret_scope, key = 'azure-ad-tenant-id')

datalake_hostname = dbutils.secrets.get(scope = secret_scope, key = 'datalake-dfs-hostname')
datalake_access_key = dbutils.secrets.get(scope = secret_scope, key = 'datalake-access-key')

# Service principal credentials for the azure storage account
spark.conf.set('fs.azure.account.auth.type', 'OAuth')
spark.conf.set('fs.azure.account.oauth.provider.type', 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider')
spark.conf.set('fs.azure.account.oauth2.client.id', databricks_sp_clientid)
spark.conf.set('fs.azure.account.oauth2.client.secret', databricks_sp_secret)
spark.conf.set('fs.azure.account.oauth2.client.endpoint', 'https://login.microsoftonline.com/' + azure_ad_tenant_id + '/oauth2/token')

# Service principal credentials for Data Warehouse (If not defined, the connector will use the Azure storage account credentials)
spark.conf.set('spark.databricks.sqldw.jdbc.service.principal.client.id', databricks_sp_clientid)
spark.conf.set('spark.databricks.sqldw.jdbc.service.principal.client.secret', databricks_sp_secret)

# Notebook session configuration
spark.conf.set('fs.azure.account.key.' + datalake_hostname, datalake_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Notebook Specific Parameters / Variables

# COMMAND ----------

configs = {
  'fs.azure.account.auth.type': 'OAuth',
  'fs.azure.account.oauth.provider.type': 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
  'fs.azure.account.oauth2.client.id': databricks_sp_clientid,
  'fs.azure.account.oauth2.client.secret': databricks_sp_secret,
  'fs.azure.account.oauth2.client.endpoint': 'https://login.microsoftonline.com/' + azure_ad_tenant_id + '/oauth2/token'
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Data Lake Mount Points

# COMMAND ----------

# Create mnt path if it doesn't exist
dbutils.fs.mkdirs('/mnt')

# Get current mount points
mounts = [str(i) for i in dbutils.fs.ls('/mnt/')]

# Mount raw container
mount_name_raw = 'raw'

if "FileInfo(path='dbfs:/mnt/" + mount_name_raw + "/', name='" + mount_name_raw + "/', size=0, modificationTime=0)" in mounts: 
  print(mount_name_raw + " has already been mounted")
else:
  dbutils.fs.mount (
    source = 'abfss://raw@' + datalake_hostname,
    mount_point = '/mnt/raw',
    extra_configs = configs
  )
  print(mount_name_raw + " has been successfully mounted")


# Mount enriched container
mount_name_enriched = 'enriched'

if "FileInfo(path='dbfs:/mnt/" + mount_name_enriched + "/', name='" + mount_name_enriched + "/', size=0, modificationTime=0)" in mounts: 
  print(mount_name_enriched + " has already been mounted")
else:
  dbutils.fs.mount (
    source = 'abfss://enriched@' + datalake_hostname,
    mount_point = '/mnt/enriched',
    extra_configs = configs
  )
  print(mount_name_enriched + " has been successfully mounted")


# Mount curated container
mount_name_curated = 'curated'

if "FileInfo(path='dbfs:/mnt/" + mount_name_curated + "/', name='" + mount_name_curated + "/', size=0, modificationTime=0)" in mounts: 
  print(mount_name_curated + " has already been mounted")
else:
  dbutils.fs.mount (
    source = 'abfss://curated@' + datalake_hostname,
    mount_point = '/mnt/curated',
    extra_configs = configs
  )
  print(mount_name_curated + " has been successfully mounted")


# Mount metadata container
mount_name_meta = 'metadata'

if "FileInfo(path='dbfs:/mnt/" + mount_name_meta + "/', name='" + mount_name_meta + "/', size=0, modificationTime=0)" in mounts: 
  print(mount_name_meta + " has already been mounted")
else:
  dbutils.fs.mount (
    source = 'abfss://metadata@' + datalake_hostname,
    mount_point = '/mnt/metadata',
    extra_configs = configs
  )
  print(mount_name_meta + " has been successfully mounted")


# Mount delta container
mount_name_delta = 'delta'

if "FileInfo(path='dbfs:/mnt/" + mount_name_delta + "/', name='" + mount_name_delta + "/', size=0, modificationTime=0)" in mounts: 
  print(mount_name_delta + " has already been mounted")
else:
  dbutils.fs.mount (
    source = 'abfss://delta@' + datalake_hostname,
    mount_point = '/mnt/delta',
    extra_configs = configs
  )
  print(mount_name_delta + " has been successfully mounted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Mount Points

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Databricks Delta Databases

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS delta LOCATION '/mnt/delta/delta_db'
