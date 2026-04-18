# Databricks notebook source
# MAGIC %md
# MAGIC ## Example Notebook
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

sql_user = dbutils.secrets.get(scope = secret_scope, key = 'sql-administrator-username')
sql_pwd = dbutils.secrets.get(scope = secret_scope, key = 'sql-administrator-password')

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

# SQL JDBC Settings
jdbc_port = '1433'
jdbc_extra_options = ('encrypt=true;trustServerCertificate=true;loginTimeout=30')

# Synapse Dedicated SQL
# Uncomment if deployed
synapse_sql_dedicated_fqdn = dbutils.secrets.get(scope = secret_scope, key = 'synapse-sql-dedicated-server-fqdn')
synapse_sql_dedicated_pool_name = dbutils.secrets.get(scope = secret_scope, key = 'synapse-sql-dedicated-pool-name')

# Synapse SQL Dedicated JDBC string for standard JDBC driver com.microsoft.sqlserver.jdbc.SQLServerDriver
synapse_sql_dedicated = (
  f"jdbc:sqlserver://{synapse_sql_dedicated_fqdn}:{jdbc_port};database={synapse_sql_dedicated_pool_name};{jdbc_extra_options};authentication=ActiveDirectoryServicePrincipal"
)

# Synapse SQL Dedicated JDBC for spark driver com.databricks.spark.sqldw  or spark._sc._gateway.jvm.java.sql.DriverManager
synapse_sql_dedicated_spark = (
  f"jdbc:sqlserver://{synapse_sql_dedicated_fqdn}:{jdbc_port};database={synapse_sql_dedicated_pool_name};user={sql_user};password={sql_pwd};{jdbc_extra_options}"
)

# Azure SQL Database
# Uncomment if deployed
azure_sql_server_fqdn = dbutils.secrets.get(scope = secret_scope, key = 'azure-sql-server-fqdn')
azure_sql_database_name = dbutils.secrets.get(scope = secret_scope, key = 'azure-sql-database-name-001')

# Azure SQL Database JDBC string for standard JDBC driver com.microsoft.sqlserver.jdbc.SQLServerDriver
azure_sql_database = (
  f"jdbc:sqlserver://{azure_sql_server_fqdn}:{jdbc_port};database={azure_sql_database_name};{jdbc_extra_options};authentication=ActiveDirectoryServicePrincipal"
)

# Azure SQL Database JDBC for spark driver com.databricks.spark.sqldw  or spark._sc._gateway.jvm.java.sql.DriverManager
azure_sql_database_spark = (
  f"jdbc:sqlserver://{azure_sql_server_fqdn}:{jdbc_port};database={azure_sql_database_name};{jdbc_extra_options}"
)

##
## This cell should remain as per notebook_template, please add any extra parameters / variables to section below
##

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Python Libraries

# COMMAND ----------

# Required for Azure AD auth for com.microsoft.sqlserver.jdbc.spark
# Needs to be imported to cluster library via PyPI
import adal

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Specific Functions

# COMMAND ----------

# Write to datalake function without partitioning
def write_to_parquet_no_partition(df_write_to_parquet,directory):
  save_location = directory + datalake_year + '/' + datalake_month + '/' + datalake_day
  
  df_write_to_parquet.repartition(1) \
                     .write \
                     .format('parquet') \
                     .option('compression', 'snappy') \
                     .mode('overwrite') \
                     .save(save_location)

def pad_date_parts(input_date_part):
  string_date_part = str(input_date_part)
  
  if len(string_date_part) == 1 :
     output_date_part = '0' + string_date_part
  else: output_date_part = string_date_part
  return output_date_part

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Notebook Specific Parameters / Variables

# COMMAND ----------

# Setup standard JDBC drive connection properties
connection_properties = {
  'driver' : 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
  'AADSecurePrincipalId' : databricks_sp_clientid,
  'AADSecurePrincipalSecret' : databricks_sp_secret
}

# Get current notebook name
current_notebook_name_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_notebook_name = current_notebook_name_path[current_notebook_name_path.rindex('/')+1:]

# Set temp directory for com.databricks.spark.sqldw driver
temp_dir = 'abfss://metadata@' + datalake_hostname + '/_databricks_tempDir/' + current_notebook_name

# Parse datetimes (for creation of data lake folders)
now = datetime.now()

datalake_year = str(now.year)
datalake_month = pad_date_parts(now.month)
datalake_day = pad_date_parts(now.day)
datalake_hour = pad_date_parts(now.hour)

# Set directories of file feeds
staging_file_directory = '/mnt/rawdata/world_wide_importers/dim_city/'
persist_file_directory = '/mnt/rawdata/world_wide_importers/Raw_Persist/dim_city/' + datalake_year + '/' + datalake_month + '/' + datalake_day + '/'
publish_file_directory = '/mnt/enricheddata/world_wide_importers/dim_city/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Files

# COMMAND ----------

custom_schema = StructType([
  StructField('CityKey', IntegerType()),
  StructField('WWICityID', IntegerType()),
  StructField('StateProvince', StringType()),
  StructField('Country', StringType()),
  StructField('Continent', StringType()),
  StructField('SalesTerritory', StringType()),
  StructField('Region', StringType()),
  StructField('Subregion', StringType()),
  StructField('Location', StringType()),
  StructField('LatestRecordedPopulation', StringType()),
  StructField('ValidFrom', StringType()),
  StructField('ValidTo', TimestampType()),
  StructField('LineageKey', IntegerType()),
])

# COMMAND ----------

df = spark.read \
          .schema(custom_schema) \
          .option('delimiter', '|') \
          .option('recursiveFileLookup', 'true') \
          .csv(staging_file_directory) 

df = df.drop('ValidFrom','LineageKey')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writeout Files To Datalake

# COMMAND ----------

# Write to Datalake
write_to_parquet_no_partition(df,publish_file_directory)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writeout Files To Delta

# COMMAND ----------

df.write.format('delta').option('mergeSchema','true').mode('overwrite').saveAsTable('delta.dim_city')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Synapse Dedicated SQL

# COMMAND ----------

# Set database name and query
database_url = synapse_sql_dedicated
pushdown_query = f"""(

SELECT *
FROM wwi.dim_customer
) queryAlias"""

# Read query
df_dim_customer = spark.read.jdbc(
  url=database_url, properties=connection_properties, table=pushdown_query
)

display(df_dim_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writeout Synapse Dedicated SQL

# COMMAND ----------

# Set database and table names
database_url = synapse_sql_dedicated_spark
table_name = 'wwi.dim_city'

df.write \
  .format('com.databricks.spark.sqldw') \
  .mode('append') \
  .option('tempDir', temp_dir) \
  .option('url', database_url) \
  .option('dbtable', table_name) \
  .option('useAzureMSI', 'true') \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Azure SQL Database

# COMMAND ----------

# Set database name and query
database_url = azure_sql_database
pushdown_query = f"""(

SELECT *
FROM wwi.dim_customer
) queryAlias"""

# Read query
df_dim_customer = spark.read.jdbc(
  url=database_url, properties=connection_properties, table=pushdown_query
)

display(df_dim_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writeout Azure SQL Database

# COMMAND ----------

# Set database and table names
database_url = azure_sql_database
table_name = 'wwi.dim_city'

df.write \
  .format('com.microsoft.sqlserver.jdbc.spark') \
  .mode('overwrite') \
  .option('truncate', 'true') \
  .option('url', database_url) \
  .option('dbtable', table_name) \
  .option('AADSecurePrincipalId', databricks_sp_clientid) \
  .option('AADSecurePrincipalSecret', databricks_sp_secret) \
  .option('enableServicePrincipalAuth', 'true') \
  .option('mssqlIsolationLevel', 'READ_UNCOMMITTED') \
  .option('reliabilityLevel', 'BEST_EFFORT') \
  .option('tableLock', 'true') \
  .option('schemaCheckEnabled', 'false') \
  .save()
