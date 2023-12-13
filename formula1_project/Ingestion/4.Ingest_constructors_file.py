# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

constructors_schema = StructType(fields=[
       StructField("constructorId", IntegerType(), False),
       StructField("constructorRef", StringType(), True),
       StructField("name", StringType(), True),
       StructField("nationality", StringType(), True),
       StructField("url", StringType(), True)
])

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{BRONZE_LAYER_PATH}/constructors.json")

# COMMAND ----------

constructors_selected_df = constructors_df.select( 
    col("constructorId").alias("constructor_id")\
    ,col("constructorRef").alias("constructor_ref") \
    ,col("name") \
    ,col("nationality"))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_selected_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet(f"{SILVER_LAYER_PATH}/constructors")
