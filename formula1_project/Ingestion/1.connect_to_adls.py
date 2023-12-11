# Databricks notebook source
# MAGIC %md
# MAGIC #### Connect to ADLS to access the formula1 data

# COMMAND ----------

formula1_account_key = dbutils.secrets.get('Azure Key Vault', 'adlsgen2key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net",
    formula1_account_key
    )

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net/formula1_raw/"))
