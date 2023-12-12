# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Ingest results.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType, StringType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp

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

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(),True),
    StructField("driverId", IntegerType(),True),
    StructField("constructorId", IntegerType(),True),
    StructField("number", IntegerType(),True),
    StructField("grid", IntegerType(),True), 
    StructField("position", IntegerType(),True),
    StructField("positionText", StringType(),True),
    StructField("positionOrder", IntegerType(),True),
    StructField("points", FloatType(),True),
    StructField("laps", IntegerType(),True),
    StructField("time", StringType(),True),
    StructField("milliseconds", IntegerType(),True),
    StructField("fastestLap", IntegerType(),True),
    StructField("rank", IntegerType(),True),
    StructField("fastestLapTime", StringType(),True),
    StructField("fastestLapSpeed", FloatType(),True),
    StructField("statusId", IntegerType(), True)

])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema)\
    .json(f"{ADLS_SOURCE}/formula1_raw/results.json")

# COMMAND ----------

results_renamed_df = results_df \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_final_df = results_renamed_df.drop(col("statusId"))

# COMMAND ----------

results_final_df.write \
    .mode("overwrite") \
    .parquet(f"{ADLS_TARGET}/processed/results")
