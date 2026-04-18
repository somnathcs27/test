# Databricks notebook source
# DBTITLE 1,Create Raw external locations if they don't exist
# List the environment variables
env_vars = ['dev','sit','prod']

# For each environment
for env_var in env_vars:

# Get the short version of the environment variable, present in storage account names
    if env_var == 'dev':
        env_var_short = 'd'
    elif env_var == 'sit':
        env_var_short = 's'
    elif env_var == 'prod':
        env_var_short = 'p'

# Produce the Azure storage account name for each environment, and each layer that the data goes through
    bronze_non_sensitive_adls = f'azlbsdata{env_var_short}dls01'
    bronze_sensitive_adls = f'azlbsdata{env_var_short}dls02'

    silver_non_sensitive_adls = f'azlbsdata{env_var_short}dls03'
    silver_sensitive_adls = f'azlbsdata{env_var_short}dls04'

    gold_non_sensitive_adls = f'azlbsdata{env_var_short}dls05'
    gold_sensitive_adls = f'azlbsdata{env_var_short}dls06'

# Run the CREATE EXTERNAL LOCATION IF NOT EXISTS command for the metadata external location
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_etl_metadata`
    URL 'abfss://metadata@{bronze_non_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Metadata storage external location'""")

# Run the CREATE EXTERNAL LOCATION IF NOT EXISTS command for the raw-non-sensitive and raw-sensitive external location
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_raw_non_sensitive`
    URL 'abfss://raw-non-sensitive@{bronze_non_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Raw non-sensitive storage external location'""")

    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_raw_sensitive`
    URL 'abfss://raw-sensitive@{bronze_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Raw sensitive storage external location'""")

# Run the CREATE EXTERNAL LOCATION IF NOT EXISTS command for the bronze-non-sensitive and bronze-sensitive external location
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_bronze_non_sensitive`
    URL 'abfss://bronze-non-sensitive@{bronze_non_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Bronze non-sensitive storage external location'""")

    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_bronze_sensitive`
    URL 'abfss://bronze-sensitive@{bronze_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Bronze sensitive storage external location'""")


# Run the CREATE EXTERNAL LOCATION IF NOT EXISTS command for the silver-non-sensitive and silver-sensitive external location
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_silver_non_sensitive`
    URL 'abfss://silver-non-sensitive@{silver_non_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Silver non-sensitive storage external location'""")

    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_silver_sensitive`
    URL 'abfss://silver-sensitive@{silver_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Silver sensitive storage external location'""")

# Run the CREATE EXTERNAL LOCATION IF NOT EXISTS command for the gold-non-sensitive and gold-sensitive external location
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_gold_non_sensitive`
    URL 'abfss://gold-non-sensitive@{gold_non_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Gold non-sensitive storage external location'""")

    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS  `{env_var}_adls_gold_sensitive`
    URL 'abfss://gold-sensitive@{gold_sensitive_adls}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `0184be54-196a-4a6e-b6d6-80cc44244f6f-storage-credential-1694608495592`)
    COMMENT 'Gold sensitive storage external location'""")
