# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/formulad111/raw/pit_stops.json

# COMMAND ----------

#%fs
#ls /mnt/formulad111/raw/drivers.json
#display(spark.read.json('/mnt/formulad111/raw/drivers.json'))
from pyspark.sql.types import StringType,StructField,StructType,IntegerType,DateType,DoubleType
from pyspark.sql.functions import concat,lit,current_timestamp

driver_schema = StructType([
   StructField("driverId",IntegerType(),True),
   StructField("duration",DoubleType(),True),
   StructField("lap",IntegerType(),False),
   StructField("milliseconds",IntegerType(),True),
   StructField("raceId",IntegerType(),True),
   StructField("nationality",StringType(),True),
   StructField("stop",IntegerType(),True),
   StructField("time",StringType(),True)
])
df = spark.read.option("multiline",True).schema(driver_schema).json("dbfs:/mnt/formulad111/raw/pit_stops.json")
display(df.printSchema())
df_renamed = df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("ingesttion_date",current_timestamp())
display(df_renamed)

# COMMAND ----------

parquet = df_renamed.write.mode("overwrite").parquet("/mnt/processed/pit_stop")
display(spark.read.parquet("/mnt/processed/pit_stop"))
