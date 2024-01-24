# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap times csv files

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

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

# COMMAND ----------

lap_times_final_df = lap_times_df \
  .withColumnRenamed("raceId", "race_id") \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumn("data_source", lit(v_data_source)) \
  .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

lap_times_final_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("f1_silver.lap_time")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!")
