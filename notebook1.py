# Databricks notebook source
# MAGIC %md
# MAGIC #Header1
# MAGIC ##header2
# MAGIC ###header3
# MAGIC **bold**
# MAGIC *italic*
# MAGIC `code`
# MAGIC >blockquote
# MAGIC 1.orderlist1
# MAGIC 2.orderlist2
# MAGIC -ul1
# MAGIC -ul2
# MAGIC [title](https://www.google.com)
# MAGIC ![alt text](image.jpg)

# COMMAND ----------

message = "Welcome sree"

# COMMAND ----------

print(message)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 1

# COMMAND ----------

# MAGIC %scala
# MAGIC val ms = "HI"
# MAGIC print(ms)

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 
