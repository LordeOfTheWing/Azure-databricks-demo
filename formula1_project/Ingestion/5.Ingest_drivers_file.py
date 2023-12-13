# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

names_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True )

])

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), False),
    StructField("dob", DateType(), True),
    StructField("name", names_schema),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),

])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{BRONZE_LAYER_PATH}/drivers.json")

# COMMAND ----------

drivers_renamed_df = drivers_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname"))) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop("url")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{SILVER_LAYER_PATH}/drivers")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!")
