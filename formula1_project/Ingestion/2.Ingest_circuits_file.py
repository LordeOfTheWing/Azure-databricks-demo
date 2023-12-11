# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step -1 Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

formula1_account_key = dbutils.secrets.get('Azure Key Vault', 'adlsgen2key')
adls_path = "abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net"
adls_target = "abfss://silver@saprojectmaestrosnd.dfs.core.windows.net"

spark.conf.set(
    "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net",
    formula1_account_key
    )

# COMMAND ----------

# Create the circuits schema

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
    .option("header",True) \
    .schema(circuits_schema) \
    .csv(f"{adls_path}/formula1_raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-2 Selecting only the required columns

# COMMAND ----------

circuits_selected_df =circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3 Renaming the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step-4 Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step-5 Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{adls_target}/processed/circuits")
