# Databricks notebook source
spark.read.table('surakshadb.bronze_stg.br_patientsdata').limit(5).display()

# COMMAND ----------

spark.read.table('surakshadb.silver_stg.sil_patientsdata').limit(5).display()
