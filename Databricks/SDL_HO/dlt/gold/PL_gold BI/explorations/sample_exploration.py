# Databricks notebook source
spark.read.table("surakshadb.silver_stg.sil_routinetests").limit(5).display()

# COMMAND ----------

spark.read.table("surakshadb.gold_stg.gold_routinetests").limit(5).display()
