# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying json files

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

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
    .json(f"{BRONZE_LAYER_PATH}/qualifying/qualifying_split*.json")

# COMMAND ----------

qualifying_final_df = qualifying_df \
  .withColumnRenamed("qualifyId", "qualify_id") \
  .withColumnRenamed("raceId", "race_id") \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("constructorId", "constructor_id") \
  .withColumn("data_source", lit(v_data_source)) \
  .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

qualifying_final_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("f1_silver.qualifying")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!")
