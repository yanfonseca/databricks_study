# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# ANSWER
username = "dbacademy"

# COMMAND ----------

projectPath     = f"/dbacademy/{username}/mlmodels/profile/"
landingPath     = projectPath + "landing/"
silverDailyPath = projectPath + "daily/"
dimUserPath     = projectPath + "users/"
goldPath        = projectPath + "gold/"
