# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

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

constructors_data_source_df = constructors_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructors_selected_df = constructors_data_source_df.select( 
    col("constructorId").alias("constructor_id")\
    ,col("constructorRef").alias("constructor_ref") \
    ,col("name") \
    ,col("nationality") \
    ,col("data_source"))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_selected_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.constructor")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!")
