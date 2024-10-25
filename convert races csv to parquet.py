# Databricks notebook source

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
#race_df = spark.read.csv('/mnt/formulad111/raw/races.csv')
race_schema = StructType([
    StructField("raceId",IntegerType(),False),
    StructField("year",IntegerType(),True),
     StructField("round",IntegerType(),True),
      StructField("ciruitId",IntegerType(),True),
       StructField("name",StringType(),True),
       StructField("date",DateType(),True),
       StructField("time",StringType(),True),
       StructField("url",StringType(),True),
])
race_df = spark.read.option("Header",True).schema(race_schema).csv("/mnt/formulad111/raw/races.csv")


# COMMAND ----------

from pyspark.sql.functions import col,to_timestamp,current_timestamp,concat,lit
race_selected_df = race_df.select('*')
race_selected_df1 = race_selected_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')).withColumn("ingested_date",current_timestamp())
race_renamed_df = race_selected_df1.withColumnRenamed("raceId","race_id").withColumnRenamed("ciruitId","circuit_id")
final_df = race_renamed_df.drop('date').drop('time')


# COMMAND ----------

parquet_file = final_df.write.mode("overwrite").parquet("/mnt/formulad111/processed/race_parquet")
display(spark.read.parquet("/mnt/formulad111/processed/race_parquet"))


# COMMAND ----------


