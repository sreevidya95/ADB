# Databricks notebook source
#%fs
#ls /mnt/formulad111/raw/constructors.json


# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructField,StructType
con_schema = StructType([
  StructField("constructorId",IntegerType(),False),
  StructField("constructorRef",StringType(),True),
  StructField("name",StringType(),False),
  StructField("nationality",StringType(),False),
  StructField("url",StringType(),False)
])
con_df = spark.read.option("Header",True).schema(con_schema).json('/mnt/formulad111/raw/constructors.json')
display(con_df.printSchema())

# COMMAND ----------

# MAGIC %md 
# MAGIC ##dropping url column as we dont want that

# COMMAND ----------

con_drop_df = con_df.drop('url')

# COMMAND ----------

con_final_df = con_drop_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref")

# COMMAND ----------

con_parquet = con_final_df.write.mode("overwrite").parquet("/mnt/processed/constructor")
display(spark.read.parquet('/mnt/processed/constructor'))
