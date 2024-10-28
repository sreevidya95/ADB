# Databricks notebook source


# COMMAND ----------

#display(dbutils.fs.ls(processes_folder_path))
circuits_df = spark.read.parquet("/mnt/formulad111/processed/circuits_parquet/")
circuits_df_final = circuits_df.withColumnRenamed("name","circuits_name")
races_df = spark.read.parquet("/mnt/formulad111/processed/race_parquet/")
races_df_final = races_df.withColumnRenamed("name","race_name")
display(races_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC Inner Join
# MAGIC

# COMMAND ----------

final_set = circuits_df_final.join(races_df_final,races_df_final.circuit_id == races_df_final.circuit_id,"inner")
display(final_set)

# COMMAND ----------

# MAGIC %md
# MAGIC Right Join

# COMMAND ----------

final_set = circuits_df_final.join(races_df_final,races_df_final.circuit_id == races_df_final.circuit_id,"right").select(circuits_df_final.circuits_name,races_df_final.circuit_id,races_df_final.round,races_df_final.race_name)
display(final_set)

# COMMAND ----------

# MAGIC %md
# MAGIC Left Join

# COMMAND ----------

final_set = circuits_df_final.join(races_df_final,races_df_final.circuit_id == races_df_final.circuit_id,"outer")
display(final_set)

# COMMAND ----------

# MAGIC %md
# MAGIC Outer Join
# MAGIC

# COMMAND ----------

final_set = circuits_df_final.join(races_df_final,races_df_final.circuit_id == races_df_final.circuit_id,"outer").select(circuits_df_final.circuits_name,races_df_final.circuit_id,races_df_final.round,races_df_final.race_name)
display(final_set)

# COMMAND ----------

# MAGIC %md
# MAGIC semi Joins(inner join and gives only left table columns)

# COMMAND ----------

final_set = circuits_df_final.join(races_df_final,races_df_final.circuit_id == races_df_final.circuit_id,"semi")
display(final_set)

# COMMAND ----------

# MAGIC %md
# MAGIC ANti JOIN(Gives un matched values  and display only left table columns)

# COMMAND ----------

final_set = circuits_df_final.join(races_df_final,races_df_final.circuit_id == races_df_final.circuit_id,"anti")
display(final_set)

# COMMAND ----------

# MAGIC %md
# MAGIC Cross Join

# COMMAND ----------

final_set = circuits_df_final.crossJoin(races_df_final)
display(final_set.count())
display(circuits_df_final.count() * races_df_final.count())
