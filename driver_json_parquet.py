# Databricks notebook source
from pyspark.sql.types import StringType,StructField,StructType,DateType,IntegerType,LongType
from pyspark.sql.functions import current_timestamp
result_schema = StructType([
    StructField("driverId",IntegerType(),False),
    StructField("number",LongType(),True),
    StructField("fastestLapSpeed",DoubleType(),True),
    StructField("fastestLapTime",StringType(),True),
    StructField("grid",IntegerType(),True),
    StructField("laps",IntegerType(),True),
    StructField("milliseconds",IntegerType(),True),
    StructField("number",IntegerType(),True),
    StructField("points",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("positionOrder",IntegerType(),True),
    StructField("positionText",IntegerType(),True),
    StructField("raceId",IntegerType(),False),
    StructField("rank",IntegerType(),True),
    StructField("resultId",IntegerType(),False),
    StructField("statusId",IntegerType(),False),
    StructField("time",StringType(),True),
])

display(spark.read.json('/mnt/formulad111/raw/drivers.json'))

# COMMAND ----------



race_df = spark.read.option("Headers",True).schema(result_schema).json('/mnt/formulad111/raw/results.json')
race_df_final = race_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("positionOrder","position_order").withColumnRenamed("positionText","position_text").withColumnRenamed("raceId","race_id").withColumnRenamed("resultId","result_id").withColumnRenamed("resultId","result_id").withColumn("ingested_date",current_timestamp())
race_df_final.write.mode("overwrite").parquet("/mnt/processed/results")
display(spark.read.parquet("/mnt/processed/results"))
