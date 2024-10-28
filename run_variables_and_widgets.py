# Databricks notebook source
raw_folder_path='/mnt/formulad111/raw'
processes_folder_path='/mnt/formulad111/processed'
present_folder_path = '/mnt/formulad111/present'

# COMMAND ----------

display(dbutils.widgets.help())
dbutils.widgets.text("environment","","Enter Environment")
