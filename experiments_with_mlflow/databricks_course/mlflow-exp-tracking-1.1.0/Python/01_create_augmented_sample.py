# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Augmented Sample

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Spark References to Data
# MAGIC 
# MAGIC In the next cell, we use Apache Spark to define a reference to the data
# MAGIC we will be working with.
# MAGIC 
# MAGIC We need to create references to the following Delta tables:
# MAGIC 
# MAGIC - `user_profile_data`
# MAGIC - `health_profile_data`

# COMMAND ----------

# TODO
# Use spark.read to create references to the two tables as dataframes

user_profile_df = spark.read.table('user_profile_data')
health_profile_df = spark.read.table('health_profile_data')

# COMMAND ----------

from sys import getsizeof

getsizeof(user_profile_df), getsizeof(user_profile_df.toPandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a Sample of Users

# COMMAND ----------

user_profile_sample_df = user_profile_df.sample(0.1)

display(user_profile_sample_df.groupby("lifestyle").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join the User Profile Data to the Health Profile Data
# MAGIC 
# MAGIC 1. Join the two dataframes, `user_profile_sample_df` and `health_profile_df`
# MAGIC 1. Perform the join using the `_id` column.
# MAGIC 
# MAGIC If successful, You should have 365 times as many rows as are in the user sample.

# COMMAND ----------

# TODO
health_profile_sample_df = (user_profile_sample_df
                          .join(health_profile_df, 
                                '_id', 
                                'inner')
)

assert 365 * user_profile_sample_df.count() == health_profile_sample_df.count()

# COMMAND ----------

health_profile_sample_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate the Data to Generate Numerical Features
# MAGIC 
# MAGIC You should perform the following aggregations:
# MAGIC 
# MAGIC - mean `BMI` aliased to `mean_BMI`
# MAGIC - mean `active_heartrate` aliased to `mean_active_heartrate`
# MAGIC - mean `resting_heartrate` aliased to `mean_resting_heartrate`
# MAGIC - mean `VO2_max` aliased to `mean_VO2_max`
# MAGIC - mean `workout_minutes` aliased to `mean_workout_minutes`

# COMMAND ----------

# TODO
from pyspark.sql.functions import mean, col

health_tracker_sample_agg_df = (health_profile_sample_df
                                .groupBy("_id")
                                .agg(
                                    mean(col('BMI')).alias('mean_BMI'),
                                    mean(col('active_heartrate')).alias('mean_active_heartrate'),
                                    mean(col('resting_heartrate')).alias('mean_resting_heartrate'),
                                    mean(col('VO2_max')).alias('mean_VO2_max'),
                                    mean(col('workout_minutes')).alias('mean_workout_minutes')
                                )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join the Aggregate Data to User Data to Augment with Categorical Features
# MAGIC 1. Join the two dataframes, `health_tracker_sample_agg_df` and `user_profile_df`
# MAGIC 1. Perform the join using the `_id` column.

# COMMAND ----------

# TODO
health_tracker_augmented_df = (health_tracker_sample_agg_df
                              .join(user_profile_df,
                                   '_id'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the following features, in this order:
# MAGIC 
# MAGIC - `mean_BMI`
# MAGIC - `mean_active_heartrate`
# MAGIC - `mean_resting_heartrate`
# MAGIC - `mean_VO2_max`
# MAGIC - `mean_workout_minutes`
# MAGIC - `female`
# MAGIC - `country`
# MAGIC - `occupation`
# MAGIC - `lifestyle`

# COMMAND ----------

health_tracker_augmented_df.printSchema()

# COMMAND ----------

# TODO
health_tracker_augmented_df = (
  health_tracker_augmented_df
  .select(
        'mean_BMI',
        'mean_active_heartrate',
        'mean_resting_heartrate',
        'mean_VO2_max',
        'mean_workout_minutes',
        'female',
        'country',
        'occupation',
        'lifestyle')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Run this Assertion to Verify The Schema

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

augmented_schema = """
  mean_BMI double,
  mean_active_heartrate double,
  mean_resting_heartrate double,
  mean_VO2_max double,
  mean_workout_minutes double,
  female boolean,
  country string,
  occupation string,
  lifestyle string
"""

assert health_tracker_augmented_df.schema == _parse_datatype_string(augmented_schema)

# COMMAND ----------

_parse_datatype_string(augmented_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the Augmented Dataframe to a Delta Table
# MAGIC 
# MAGIC Use the following path: `goldPath + "health_tracker_augmented"`

# COMMAND ----------

# TODO
(
  health_tracker_augmented_df.write
  .format("delta")
  .mode("overwrite")
  .save(goldPath + "health_tracker_augmented")
)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------

display(
  dbutils.fs.ls(projectPath)
)