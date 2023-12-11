# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step -1 Read the CSV file using the spark dataframe reader

# COMMAND ----------

formula1_account_key = dbutils.secrets.get('Azure Key Vault', 'adlsgen2key')

spark.conf.set(
    "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net",
    formula1_account_key
    )

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net/formula1_raw/circuits.csv"))

# COMMAND ----------

circuits_df = spark.read.option("header",True).csv("abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net/formula1_raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------


