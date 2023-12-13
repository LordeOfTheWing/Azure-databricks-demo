# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying json files

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType,StringType

# COMMAND ----------

ADLS_SOURCE = "abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net"
ADLS_TARGET = "abfss://silver@saprojectmaestrosnd.dfs.core.windows.net"
CONN_STRING = "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net"
ACCESS_KEY = dbutils.secrets.get("Azure Key Vault", "adlsgen2key")

spark.conf.set(
CONN_STRING,
ACCESS_KEY
)

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True) \
    .json(f"{ADLS_SOURCE}/formula1_raw/qualifying/qualifying_split*.json")

# COMMAND ----------

qualifying_final_df = qualifying_df \
  .withColumnRenamed("qualifyId", "qualify_id") \
  .withColumnRenamed("raceId", "race_id") \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("constructorId", "constructor_id") \
  .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

qualifying_final_df.write \
    .mode("overwrite") \
    .parquet(f"{ADLS_TARGET}/processed/qualifying")
