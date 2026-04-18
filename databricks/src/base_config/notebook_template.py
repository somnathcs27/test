# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Template
# MAGIC <p><b>Description: </b>Enter notebook decription</p>
# MAGIC <b>Parent Process: </b>Enter name of any parent process that calls this notebook, eg synapse pipeline name
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

# SQL Settings
jdbc_port = '1433'
jdbc_extra_options = ('encrypt=true;trustServerCertificate=true;loginTimeout=30')

sql_user = dbutils.secrets.get(scope = secret_scope, key = 'sql-administrator-username')
sql_pwd = dbutils.secrets.get(scope = secret_scope, key = 'sql-administrator-password')

# Synapse Dedicated SQL
## Uncomment if deployed
# synapse_sql_dedicated_fqdn = dbutils.secrets.get(scope = secret_scope, key = 'synapse-sql-dedicated-server-fqdn')
# synapse_sql_dedicated_pool_name = dbutils.secrets.get(scope = secret_scope, key = 'synapse-sql-dedicated-pool-name')

# # Synapse SQL Dedicated JDBC string for standard JDBC driver com.microsoft.sqlserver.jdbc.SQLServerDriver
# synapse_sql_dedicated = (
#   f"jdbc:sqlserver://{synapse_sql_dedicated_fqdn}:{jdbc_port};database={synapse_sql_dedicated_pool_name};{jdbc_extra_options};authentication=ActiveDirectoryServicePrincipal"
# )

# # Synapse SQL Dedicated JDBC for spark driver com.databricks.spark.sqldw
# synapse_sql_dedicated_spark = (
#   f"jdbc:sqlserver://{synapse_sql_dedicated_fqdn}:{jdbc_port};database={synapse_sql_dedicated_pool_name};user={sql_user};password={sql_pwd};{jdbc_extra_options}"
# )

# Azure SQL Database
## Uncomment if deployed
# azure_sql_server_fqdn = dbutils.secrets.get(scope = secret_scope, key = 'azure-sql-server-fqdn')
# azure_sql_database_name = dbutils.secrets.get(scope = secret_scope, key = 'azure-sql-database-name-001')

# # Azure SQL Database JDBC string for standard JDBC driver com.microsoft.sqlserver.jdbc.SQLServerDriver & com.microsoft.sqlserver.jdbc.spark
# azure_sql_database = (
#   f"jdbc:sqlserver://{azure_sql_server_fqdn}:{jdbc_port};database={azure_sql_database_name};{jdbc_extra_options};authentication=ActiveDirectoryServicePrincipal"
# )

##
## This cell should remain as per notebook_template, please add any extra parameters / variables to section below
##

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Python Libraries

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Specific Functions

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Notebook Specific Parameters / Variables

# COMMAND ----------


