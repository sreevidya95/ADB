# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import current_timestamp
con_schema = StructType([
               StructField("constructorId",IntegerType(),False),
                StructField("constructorRef",StringType(),True),
                StructField("name",StringType(),True),
                StructField("nationality",StringType(),True),
                StructField("url",StringType(),True)
])
constructor_df = spark.read.option("Headers",True).json("/mnt/formulad111/raw/constructors.json")
final_df = constructor_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").drop("url").withColumn("ingested_date",current_timestamp())
final_df.write.mode("overwrite").parquet("/mnt/formulad111/processed/constructor")
display(spark.read.parquet('/mnt/formulad111/processed/constructor'))
