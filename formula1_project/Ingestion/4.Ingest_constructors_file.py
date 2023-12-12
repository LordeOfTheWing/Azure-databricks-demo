# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

ADLS_SOURCE = "abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net"
ADLS_TARGET = "abfss://silver@saprojectmaestrosnd.dfs.core.windows.net"
ACCESS_KEY = dbutils.secrets.get("Azure Key Vault", "adlsgen2key")

spark.conf.set(
    "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net",
    ACCESS_KEY
)


# COMMAND ----------

constructors_schema = StructType(fields=[
       StructField("constructorId", IntegerType(), False),
       StructField("constructorRef", StringType(), True),
       StructField("name", StringType(), True),
       StructField("nationality", StringType(), True),
       StructField("url", StringType(), True)
])

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{ADLS_SOURCE}/formula1_raw/constructors.json")

# COMMAND ----------

constructors_selected_df = constructors_df.select( 
    col("constructorId").alias("constructor_id")\
    ,col("constructorRef").alias("constructor_ref") \
    ,col("name") \
    ,col("nationality"))

# COMMAND ----------

constructors_final_df = constructors_selected_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet(f"{ADLS_TARGET}/processed/constructors")
