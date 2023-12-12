# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, current_timestamp, concat, lit

# COMMAND ----------

ADLS_SOURCE = "abfss://bronze@saprojectmaestrosnd.dfs.core.windows.net"
ADLS_TARGET = "abfss://silver@saprojectmaestrosnd.dfs.core.windows.net"
ACCESS_KEY = dbutils.secrets.get("Azure Key Vault", "adlsgen2key")

spark.conf.set(
    "fs.azure.account.key.saprojectmaestrosnd.dfs.core.windows.net",
    ACCESS_KEY
)


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
    .json(f"{ADLS_SOURCE}/formula1_raw/drivers.json")

# COMMAND ----------

drivers_renamed_df = drivers_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname"))) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop("url")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{ADLS_TARGET}/processed/drivers")