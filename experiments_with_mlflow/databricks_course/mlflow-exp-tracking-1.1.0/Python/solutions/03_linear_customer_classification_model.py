# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Building a Linear Customer Classfication Model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/main/python/preprocessing

# COMMAND ----------

# MAGIC %run ./includes/main/python/experiment

# COMMAND ----------

from sklearn.linear_model import LogisticRegression

# COMMAND ----------

data = (X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grid-Searched Model Fitting
# MAGIC 
# MAGIC The following models were fit using a grid-searched, cross validation with
# MAGIC the respective parameter dictionaries:
# MAGIC 
# MAGIC  - Ridge,
# MAGIC    - `{'alpha' : logspace(-5,5,11)}`
# MAGIC  - Lasso,
# MAGIC    - `{'alpha' : logspace(-5,5,11)}`
# MAGIC  - Elastic Net,
# MAGIC    - `{'alpha' : logspace(-5,5,11), 'l1_ratio' : linspace(0,1,11)}`

# COMMAND ----------

estimator = LogisticRegression(max_iter=10000)
param_grid = {
  'C' : np.logspace(-5,5,11),
  "penalty" : ['l2']
}
mlflow_run(experiment_id, estimator, param_grid, data)

# COMMAND ----------

estimator = LogisticRegression(max_iter=10000)
param_grid = {
  'C' : np.logspace(-5,6,11),
  "penalty" : ['l2']
}
mlflow_run(experiment_id, estimator, param_grid, data)

# COMMAND ----------

estimator = LogisticRegression(max_iter=10000)
param_grid = {
  'C' : np.logspace(-5,5,11),
  "penalty" : ['l1'], "solver" : ['saga']
}
mlflow_run(experiment_id, estimator, param_grid, data)

# COMMAND ----------

estimator = LogisticRegression(max_iter=10000)
param_grid = {
  'C' : np.logspace(-5,5,11),
  "penalty" : ['elasticnet'],
  'l1_ratio' : np.linspace(0,1,11),
  "solver" : ['saga']
}
mlflow_run(experiment_id, estimator, param_grid, data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display Experiment Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display Coefficients Associated with Each Classifier

# COMMAND ----------

prepare_results(experiment_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display Coefficients Associated with Each Classifier

# COMMAND ----------

prepare_coefs(experiment_id, le.classes_, X_train.columns)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>