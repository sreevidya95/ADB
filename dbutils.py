# Databricks notebook source
dbutils.fs.ls('/')

# COMMAND ----------

for files in dbutils.fs.ls('/'):
    print(files)

# COMMAND ----------

for file in dbutils.fs.ls('/'):
    if file.path.endswith('/'):
        print(file.name)

# COMMAND ----------

dbutils.help()
