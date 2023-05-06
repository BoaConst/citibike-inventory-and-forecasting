# Databricks notebook source
pip install folium

# COMMAND ----------

# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# print(start_date,end_date,hours_to_forecast, promote_model)

# print("YOUR CODE HERE...")

# COMMAND ----------

# DBTITLE 1,Current-timestamp when the notebook is run
from pyspark.sql.functions import current_timestamp

currentdate = pd.Timestamp.now(tz='US/Eastern')
fmt = '%Y-%m-%d %H:%M:%S'
currenthour = currentdate.strftime("%Y-%m-%d %H") 
currentdate = currentdate.strftime(fmt) 
print("The current timestamp is:",currentdate)

# COMMAND ----------

client = MlflowClient()

production_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=['Production'])
staging_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=['Staging'])


print("Production Model : ")
print(production_model)


print("Staging Model : ")
print(staging_model)


# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

# Read-in Data from Delta table
table_name = 'Silver_hist_live_Data_Modelling'
Hist_live_df = spark.read.format("delta").load(GROUP_DATA_PATH + table_name)
display(Hist_live_df)

# COMMAND ----------

# Parameter for Prediction and Forcasting in Production
Hours_To_Forecast = 4
Days_To_Predict = 10

# Get max data from the Hist and live data
from pyspark.sql.functions import max, col
max_date = Hist_live_df.select(max("ds")).collect()[0][0]

# Get date from which we will predict
predict_cutoff_date = max_date - datetime.timedelta(days=Days_To_Predict)

# Subset the data that will be used to predict in Production
predict_data = Hist_live_df.filter(col("ds") >= predict_cutoff_date)

display(predict_data)

# COMMAND ----------

predict_data_pd = predict_data.toPandas()
predict_data_pd = predict_data_pd.dropna()

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
from mlflow.tracking.client import MlflowClient
import datetime
from pyspark.sql.functions import *
import mlflow

ARTIFACT_PATH = GROUP_MODEL_NAME
client = MlflowClient()
latest_version_info = client.get_latest_versions(ARTIFACT_PATH, stages=['Production'])
latest_production_version = latest_version_info[0].version
print("The latest production version of the model '%s' is '%s'." %(ARTIFACT_PATH, latest_production_version))

# Predict on the future based on the production model
model_prod_uri = f'models:/{ARTIFACT_PATH}/production'
model_prod = mlflow.prophet.load_model(model_prod_uri)
prod_forecast = model_prod.predict(predict_data_pd)
prod_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]



display(predict_data_pd)

prophet_plot = model_prod.plot(prod_forecast)


# COMMAND ----------

# DBTITLE 1,Residual of Staging Model
predict_data_pd.ds = pd.to_datetime(predict_data_pd.ds)
prod_forecast.ds = pd.to_datetime(prod_forecast.ds)
results = prod_forecast[['ds','yhat']].merge(predict_data_pd,on="ds")
results['residual'] = results['yhat'] - results['y']

# Plot the residuals

fig = px.scatter(
    results, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols'
)
fig.show()


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
