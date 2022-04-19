# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC ### Define Data Paths

# COMMAND ----------

# TODO
try:
  username = 'yan'
except:
  raise NameError("Be sure to define your username in the includes/configuration notebook.")

# COMMAND ----------

projectPath     = f"/dbacademy/{username}/mlmodels/profile/"
landingPath     = projectPath + "landing/"
silverDailyPath = projectPath + "daily/"
dimUserPath     = projectPath + "users/"
goldPath        = projectPath + "gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}");
