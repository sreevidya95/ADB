# Databricks notebook source
#display(dbutils.fs.mounts())
#display(dbutils.fs.ls('/mnt/formulad111/raw'))
from pyspark.sql.types import DoubleType,StructField,StringType,StructType,IntegerType


# COMMAND ----------

#display(spark.read.csv('dbfs:/mnt/formulad111/raw/circuits.csv'))
circuits_schema = StructType([
    StructField('circuitId',IntegerType(),False),
    StructField('circuitRef',StringType(),True),
     StructField('name',StringType(),True),
      StructField('location',StringType(),True),
       StructField('country',StringType(),True),
        StructField('lat',DoubleType(),True),
         StructField('lng',DoubleType(),True),
          StructField('alt',IntegerType(),True),
           StructField('url',StringType(),True)
])



# COMMAND ----------

circuits_df = spark.read.option("Header",True).schema(circuits_schema).csv('dbfs:/mnt/formulad111/raw/circuits.csv')

# COMMAND ----------

#display(circuits_df.printSchema())
#circuits_df.describe().show()
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting only required columns from dataframe using 3 types
# MAGIC

# COMMAND ----------

#circuits_selected_df=circuits_df.select('circuitId','circuitRef','name','location','country','lat','lng','alt')
#wecant do alias or alter the column name with abpve method

#circuits_selected_df=circuits_df.select(circuits_df['circuitId'].alias('circuit_id'),circuits_df['circuitRef'],
                                        #circuits_df['name'],circuits_df['location'],circuits_df['country'],circuits_df['lat'],circuits_df['lng'],circuits_df['alt'])
#circuits_selected_df=circuits_df.select(circuits_df.circuitId.alias('circuit_id'),circuits_df.circuitRef,
                                        #circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)
#display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df=circuits_df.select(col('circuitId').alias('circuit_id'),col('circuitRef'),
                                        col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))
display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitRef',
                                          'circuit_ref').withColumnRenamed('lat',
                                          'latitude').withColumnRenamed('lng',
                                         'longitude').withColumnRenamed('alt','altitude')


# COMMAND ----------

# MAGIC %md
# MAGIC Add column with current time stamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = circuits_renamed_df.withColumn("ingetion_date",current_timestamp())


# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formulad111/processed

# COMMAND ----------

# MAGIC %md
# MAGIC Writing this data into parquet file and mounting it to data lale in process container
# MAGIC
# MAGIC

# COMMAND ----------

parquet_data = final_df.write.mode('overwrite').parquet('/mnt/formulad111/processed/circuits_parquet')
display(spark.read.parquet('/mnt/formulad111/processed/circuits_parquet'))
