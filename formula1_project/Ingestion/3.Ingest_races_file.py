# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest races.csv file

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# Step1 Read the races CSV file
races_df = spark.read.option("Header", True) \
    .schema(races_schema) \
    .csv(f"{BRONZE_LAYER_PATH}/races.csv")


# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

final_races_df = races_selected_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_races_df.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.race")   

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!")
