# Databricks notebook source
#%fs
#ls /mnt/formulad111/raw/drivers.json
#display(spark.read.json('/mnt/formulad111/raw/drivers.json'))
from pyspark.sql.types import StringType,StructField,StructType,IntegerType,DateType
from pyspark.sql.functions import concat,lit,current_timestamp
name_schema = StructType([
    StructField("forename",StringType(),True),
     StructField("surname",StringType(),True)
])
driver_schema = StructType([
   StructField("code",StringType(),True),
   StructField("dob",DateType(),True),
   StructField("driverId",IntegerType(),False),
   StructField("driverRef",StringType(),True),
   StructField("name",name_schema,True),
   StructField("nationality",StringType(),True),
   StructField("number",StringType(),True),
   StructField("url",StringType(),True)
])
driver_df = spark.read.option("Headers",True).schema(driver_schema).json("/mnt/formulad111/raw/drivers.json")
driver_name_df = driver_df.withColumn("name",concat(driver_df.name.forename,lit(" "),driver_df.name.surname)).withColumn("ingestion_time",current_timestamp())
driver_renamed_df = driver_name_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref")
driver_final_df = driver_renamed_df.drop('url')
display(driver_final_df)



# COMMAND ----------

parquet = driver_final_df.write.mode("overwrite").parquet("/mnt/formulad111/processed/driver")
display(spark.read.parquet("/mnt/processed/driver"))
