# Databricks notebook source
dbutils.widgets.text("catalog", "")
catalog_name = dbutils.widgets.get("catalog")

dbutils.widgets.text(name='env_var', defaultValue = 'dev')
env_var = dbutils.widgets.get("env_var")

dbutils.widgets.text(name='common_libs_lts_ver', defaultValue = '')
common_libs_lts_ver = dbutils.widgets.get("common_libs_lts_ver")

dbutils.widgets.text(name='common_libs_lts_ver_underscore', defaultValue = '')
common_libs_lts_ver_underscore = dbutils.widgets.get("common_libs_lts_ver_underscore")

mdp_databricks_common_library = f"/Volumes/{env_var}_catalog/common_libraries/mdp_databricks_common_{common_libs_lts_ver_underscore}/mdp_databricks_common-{common_libs_lts_ver}-py3-none-any.whl"

# COMMAND ----------

from mdp_databricks_common.facts.fact_factory import FactFactory
from mdp_databricks_common.enums.enums import FactType

# COMMAND ----------

source_query = f"""
WITH cte_saving_account_total_customers AS
(
  SELECT 
      bpa.FK_ACCOUNT AS FK_SAVING_ACCOUNT
      ,COUNT(DISTINCT bpa.FK_CUSTOMER) AS NUMBER_OF_SAVING_CUSTOMERS
  from {catalog_name}.gold_con.bridge_party_account bpa
  where bpa.RELATIONSHIP_END_DATE IS NULL --only customers still linked to the account
    AND bpa.ACCOUNT_TYPE = 'Saving'
  GROUP BY bpa.FK_ACCOUNT
)

,cte_mortgage_account_total_customers AS
(
  SELECT 
      bpa.FK_ACCOUNT AS FK_MORTGAGE_ACCOUNT
      ,COUNT(DISTINCT bpa.FK_CUSTOMER) AS NUMBER_OF_MORTGAGE_CUSTOMERS
  from {catalog_name}.gold_con.bridge_party_account bpa
  where bpa.RELATIONSHIP_END_DATE IS NULL --only customers still linked to the account
    AND bpa.ACCOUNT_TYPE = 'Mortgage'
  GROUP BY bpa.FK_ACCOUNT
)

,cte_lastest_mortgage_account_close_date AS
(
  SELECT 
      bpa.FK_CUSTOMER
      ,MAX(fma.FK_CLOSED_DATE) AS LATEST_MORTGAGE_ACCOUNT_CLOSE_DATE
  FROM {catalog_name}.gold_con.fact_mortgage_account fma 
  INNER JOIN {catalog_name}.gold_con.bridge_party_account bpa
  ON fma.FK_MORTGAGE_ACCOUNT = bpa.FK_ACCOUNT
  AND bpa.ACCOUNT_TYPE = 'Mortgage'
  GROUP BY bpa.FK_CUSTOMER
)

,cte_customer_address AS
(
  SELECT 
    dca.PK_CUSTOMER_ADDRESS
    ,dca.CUSTOMER_NUMBER
    ,dca.USE
  FROM {catalog_name}.gold_con.dim_customer_address dca
  WHERE dca.ROW_IS_CURRENT = 1
  AND USE IN ('Residential Address','Correspondence address')
  QUALIFY ROW_NUMBER() OVER(PARTITION BY dca.CUSTOMER_NUMBER,dca.USE ORDER BY dca.START_DATE DESC) = 1
)

SELECT
  CAST(xxhash64(dc.BK_CUSTOMER) AS BIGINT) AS PK_FACT_CUSTOMER
  ,bpa.FK_CUSTOMER
  ,MAX(COALESCE(add_cor.PK_CUSTOMER_ADDRESS,-2)) AS FK_CUSTOMER_ADDRESS_CORRESPONDENCE
  ,MAX(COALESCE(add_res.PK_CUSTOMER_ADDRESS,-2)) AS FK_CUSTOMER_ADDRESS_RESIDENTIAL
  ,CAST(MAX(COALESCE(lmacd.LATEST_MORTGAGE_ACCOUNT_CLOSE_DATE,'1900-01-01')) AS DATE) AS FK_LATEST_MORTGAGE_ACCOUNT_CLOSE_DATE
  ,dc.CUSTOMER_NUMBER
  ,CAST(NULL AS INTEGER) AS MORTGAGE_APPLICATIONS --no link between applicants in mso and customers in dataverse at present 
  ,CAST(SUM(
      CASE 
        WHEN (fma.FK_OPEN_DATE <> '1900-01-01' OR fma.FK_OPEN_DATE IS NOT NULL) AND (fma.FK_CLOSED_DATE = '1900-01-01' OR fma.FK_CLOSED_DATE IS NULL) AND bpa.RELATIONSHIP_END_DATE IS NULL THEN 1 
        ELSE NULL 
      END
      ) AS INTEGER) AS OPEN_MORTGAGE_ACCOUNTS
  ,CAST(SUM(
      CASE 
        WHEN (fma.FK_OPEN_DATE <> '1900-01-01' AND fma.FK_OPEN_DATE IS NOT NULL) AND (fma.FK_CLOSED_DATE <> '1900-01-01' AND fma.FK_CLOSED_DATE IS NOT NULL) AND bpa.RELATIONSHIP_END_DATE IS NULL THEN 1 
        ELSE NULL 
      END
      ) AS INTEGER) AS CLOSED_MORTGAGE_ACCOUNTS
  ,CAST(SUM(
      CASE 
        WHEN  bpa.RELATIONSHIP_END_DATE IS NULL THEN fma.CURRENT_BALANCE / mort.NUMBER_OF_MORTGAGE_CUSTOMERS 
        ELSE NULL 
      END
      ) AS DECIMAL(38,2)) AS MORTGAGE_CURRENT_BALANCE_SHARE
  ,CAST(SUM(
      CASE 
        WHEN  bpa.RELATIONSHIP_END_DATE IS NULL THEN fma.ACCRUED_INTEREST / mort.NUMBER_OF_MORTGAGE_CUSTOMERS 
        ELSE NULL 
      END
      ) AS DECIMAL(38,2)) AS MORTGAGE_ACCRUED_INTEREST_SHARE
  ,CAST(SUM(
      CASE 
        WHEN fsa.FK_CLOSED_DATE IS NULL AND fsa.FK_ACTIVATION_DATE IS NOT NULL AND bpa.RELATIONSHIP_END_DATE IS NULL THEN 1 
        ELSE NULL 
      END
      ) AS INTEGER) AS OPEN_SAVING_ACCOUNTS
  ,CAST(SUM(
      CASE 
        WHEN fsa.FK_CLOSED_DATE IS NOT NULL AND fsa.FK_ACTIVATION_DATE IS NOT NULL AND bpa.RELATIONSHIP_END_DATE IS NULL THEN 1 
        ELSE NULL 
      END
      ) AS INTEGER) AS CLOSED_SAVING_ACCOUNTS
  ,CAST(SUM(
      CASE 
        WHEN  bpa.RELATIONSHIP_END_DATE IS NULL THEN fsa.CAPITAL_LEDGER_BALANCE / sav.NUMBER_OF_SAVING_CUSTOMERS 
        ELSE NULL
      END
      ) AS DECIMAL(38,2)) AS SAVING_CAPITAL_LEDGER_BALANCE_SHARE
  ,CAST(SUM(
      CASE 
        WHEN  bpa.RELATIONSHIP_END_DATE IS NULL THEN fsa.CAPITAL_AVAILABLE_BALANCE / sav.NUMBER_OF_SAVING_CUSTOMERS
        ELSE NULL
      END
    ) AS DECIMAL(38,2)) AS SAVING_CAPITAL_AVAILABLE_BALANCE_SHARE
FROM {catalog_name}.gold_con.bridge_party_account bpa
INNER JOIN {catalog_name}.gold_con.dim_customer dc
ON bpa.FK_CUSTOMER = dc.PK_CUSTOMER
LEFT JOIN {catalog_name}.gold_con.fact_saving_account fsa
ON bpa.FK_ACCOUNT = fsa.FK_SAVING_ACCOUNT
AND bpa.ACCOUNT_TYPE = 'Saving'
LEFT JOIN {catalog_name}.gold_con.fact_mortgage_account fma
ON bpa.FK_ACCOUNT = fma.FK_MORTGAGE_ACCOUNT
AND bpa.ACCOUNT_TYPE = 'Mortgage'
LEFT JOIN cte_saving_account_total_customers sav
ON sav.FK_SAVING_ACCOUNT = fsa.FK_SAVING_ACCOUNT
LEFT JOIN cte_mortgage_account_total_customers mort
ON mort.FK_MORTGAGE_ACCOUNT = fma.FK_MORTGAGE_ACCOUNT
LEFT JOIN cte_lastest_mortgage_account_close_date lmacd
ON bpa.FK_CUSTOMER = lmacd.FK_CUSTOMER
LEFT JOIN cte_customer_address add_cor
ON dc.CUSTOMER_NUMBER = add_cor.CUSTOMER_NUMBER
AND add_cor.USE = 'Correspondence address'
LEFT JOIN cte_customer_address add_res
ON dc.CUSTOMER_NUMBER = add_res.CUSTOMER_NUMBER
AND add_res.USE = 'Residential Address'
GROUP BY bpa.FK_CUSTOMER,dc.CUSTOMER_NUMBER,dc.BK_CUSTOMER
"""

try:
  fact =  FactFactory(catalog_name, 'gold_con','fact_customer', source_query).create(FactType.FULL_LOAD)
  fact.load()
except Exception as e:
  dbutils.jobs.taskValues.set("error_detail", str(e))
  raise(e)

