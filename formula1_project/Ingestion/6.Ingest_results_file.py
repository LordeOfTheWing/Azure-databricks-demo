# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Ingest results.json file

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

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
    .json(f"{BRONZE_LAYER_PATH}/results.json")

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
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_final_df = results_renamed_df.drop(col("statusId"))

# COMMAND ----------

results_final_df.write \
    .mode("overwrite") \
    .parquet(f"{SILVER_LAYER_PATH}/results")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!")
