# Databricks notebook source
# MAGIC %run "../ADB/run_variables_and_widgets"
# MAGIC

# COMMAND ----------

raw_folder_path

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
lap_schema = StructType([
    StructField("race_id",IntegerType(),False),
     StructField("driver_id",IntegerType(),False),
     StructField("lap",IntegerType(),True),
     StructField("position",IntegerType(),True),
     StructField("race_time",StringType(),True),
     StructField("milliseconds",FloatType(),True),
    
])
lap_df = spark.read.schema(lap_schema).csv(f"{raw_folder_path}/lap_times")
display(lap_df)
display(lap_df.count())

# COMMAND ----------

parquet = lap_df.write.mode("overwrite").parquet("/mnt/formulad111/processed/lap_times")
display(spark.read.parquet('/mnt/formulad111/processed/lap_times'))
