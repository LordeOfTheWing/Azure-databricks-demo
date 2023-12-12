# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pit_stops.json file

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

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
    .option("multiLine", True)\
    .schema(pit_stops_schema) \
    .json(f"{ADLS_SOURCE}/formula1_raw/pit_stops.json")

# COMMAND ----------

pit_stops_final_df = pit_stops_df \
  .withColumnRenamed("raceId", "race_id") \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pit_stops_final_df.write \
    .mode("overwrite") \
    .parquet(f"{ADLS_TARGET}/processed/pit_stops")
