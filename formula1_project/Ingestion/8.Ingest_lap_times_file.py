# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap times csv files

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

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
    .csv(f"{BRONZE_LAYER_PATH}/lap_times/lap_times_split*.csv")

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
    .parquet(f"{SILVER_LAYER_PATH}/lap_times")
