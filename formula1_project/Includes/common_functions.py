# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col, lit, concat, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, FloatType

# COMMAND ----------

def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())

