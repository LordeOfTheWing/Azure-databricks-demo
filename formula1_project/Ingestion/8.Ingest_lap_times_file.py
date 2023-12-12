# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap times csv files

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

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{ADLS_SOURCE}/formula1_raw/lap_times/lap_times_split*.csv")

display(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_df \
  .withColumnRenamed("raceId", "race_id") \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumn("ingestion_date", current_timestamp())

display(lap_times_final_df)

# COMMAND ----------

lap_times_final_df.write \
    .mode("overwrite") \
    .parquet(f"{ADLS_TARGET}/processed/lap_times")
