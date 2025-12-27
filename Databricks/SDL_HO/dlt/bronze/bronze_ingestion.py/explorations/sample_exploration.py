# Databricks notebook source
spark.read.table("surakshadb.bronze_stg.br_acxtestdone").limit(5).display()

# COMMAND ----------

spark.read.table("surakshadb.bronze_stg.br_doctorattendance").limit(5).display()

# COMMAND ----------

spark.read.table("surakshadb.bronze_stg.br_inventorymaster").limit(5).display()

# COMMAND ----------

spark.read.table("surakshadb.bronze_stg.br_patientsdata").limit(5).display()

# COMMAND ----------

spark.read.table("surakshadb.bronze_stg.br_routinetests").limit(5).display()
