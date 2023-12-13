# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

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
    .json(f"{BRONZE_LAYER_PATH}/pit_stops.json")

# COMMAND ----------

pit_stops_final_df = pit_stops_df \
  .withColumnRenamed("raceId", "race_id") \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumn("data_source", lit(v_data_source)) \
  .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pit_stops_final_df.write \
    .mode("overwrite") \
    .parquet(f"{SILVER_LAYER_PATH}/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!")
