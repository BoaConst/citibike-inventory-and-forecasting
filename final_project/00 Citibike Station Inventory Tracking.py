# Databricks notebook source
# MAGIC %md
# MAGIC ##DSCC 202 - 402 Final Project Spring 2023
# MAGIC <p>
# MAGIC <img src='https://data-science-at-scale.s3.amazonaws.com/images/fp2023.png'>
# MAGIC </p>
# MAGIC see product description and rubric in repo same directory as this notebook.

# COMMAND ----------

import json

# Run the initialize group environment at the start to refresh all the data directories and database tables
result = dbutils.notebook.run("initialize_group_environment", 3600)

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

from datetime import datetime as dt
from datetime import timedelta

dbutils.widgets.removeAll()

dbutils.widgets.text('01.start_date', "2021-01-01")
dbutils.widgets.text('02.end_date', "2023-12-31")
dbutils.widgets.text('03.hours_to_forecast', '4')
dbutils.widgets.text('04.promote_model', 'Yes')

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = str(dbutils.widgets.get('04.promote_model'))

print(start_date,end_date,hours_to_forecast, promote_model)

# COMMAND ----------

# DBTITLE 1,Run the ETL Notebook
# Run the Data Prepartion note the one hour 3600 second timeout!
result = dbutils.notebook.run("01 etl", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.promote_model":promote_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,Run the EDA Notebook
# Run the Data Prepartion
result = dbutils.notebook.run("02 eda", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.promote_model":promote_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,Run Model Development Notebook
# Run the Data Prepartion
result = dbutils.notebook.run("03 mdl", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.promote_model":promote_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,Run Station Inventory Forecast Notebook
# Run the Data Prepartion
result = dbutils.notebook.run("04 app", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.promote_model":promote_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked
