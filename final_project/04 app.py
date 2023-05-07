# Databricks notebook source
pip install folium

# COMMAND ----------

# MAGIC %run ./includes/includes

# COMMAND ----------

import mlflow
import json
import pandas as pd
import numpy as np
from prophet import Prophet, serialize
from prophet.diagnostics import cross_validation, performance_metrics
from mlflow.tracking.client import MlflowClient
import seaborn as sns
import matplotlib.pyplot as plt
sns.set(color_codes=True)
import itertools
from pyspark.sql.functions import max, date_sub,to_timestamp,date_format
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))

print(hours_to_forecast)


# COMMAND ----------

# DBTITLE 1,Current-timestamp when the notebook is run
from pyspark.sql.functions import current_timestamp, dayofweek, to_timestamp

currentdate = pd.Timestamp.now(tz='US/Eastern')
fmt = '%Y-%m-%d %H:%M:%S'
currenthour = currentdate.strftime("%Y-%m-%d %H") 
currentdate = currentdate.strftime(fmt) 
print("The current timestamp is:",currentdate)

# COMMAND ----------

# DBTITLE 1,Production and Staging model Details
client = MlflowClient()

production_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=['Production'])
staging_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=['Staging'])


print("Production Model : ")
print(production_model)


print("Staging Model : ")
print(staging_model)


# COMMAND ----------

# DBTITLE 1,Show the station on map
import folium

print("Assigned Station: ", GROUP_STATION_ASSIGNMENT)

# Create a map centered at the given latitude and longitude

latitude = 40.71754834
longitude = -74.01322069
pointer = folium.Map(location=[latitude, longitude], zoom_start=12)
folium.Marker(location=[latitude, longitude], icon=folium.Icon(color='red')).add_to(pointer)

# Display the map
pointer

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

currentdate = pd.Timestamp.now(tz='US/Eastern').round(freq = 'H')
fmt = '%Y-%m-%d %H:%M:%S'
currentdate = currentdate.strftime(fmt) 

# COMMAND ----------

# DBTITLE 1,Read-in the weather data for showing the current weather
weather = spark.read.format("delta").load(GROUP_DATA_PATH + "Bronze_live_nyc_weather_data")
display(weather)


# COMMAND ----------

from pyspark.sql.functions import col
weather_current = weather.filter(col("time") == currentdate)
display(weather_current)

# COMMAND ----------

# DBTITLE 1,Number of Docks at our station
print(GROUP_STATION_ASSIGNMENT)
info_df = spark.read.format("delta").load(BRONZE_STATION_INFO_PATH)
import pyspark.sql.functions as F
G_02_station_capacity = (info_df.filter(F.col('name')== GROUP_STATION_ASSIGNMENT)).select("capacity").collect()[0][0];
# capacity = station_capacity["capacity"]
print("Total Number of docks at this station : "+str(G_02_station_capacity))

# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

# Read-in Data from Delta table
table_name = 'Silver_hist_live_Data_Modelling'
Hist_live_df = spark.read.format("delta").load(GROUP_DATA_PATH + table_name)
display(Hist_live_df)

# COMMAND ----------

# DBTITLE 1,Predicting for last 10 days in Live data and forecasting for 4 hours in future
# Parameter for Prediction and Forcasting in Production
Hours_To_Forecast = hours_to_forecast
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

# DBTITLE 1,Plot the forecasted values in production
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

##Next four hours Forecasting
from pyspark.sql.functions import col
weather_current = weather.filter(col("time") > currentdate)
weather_next_four = weather_current.orderBy("dt")
weather_next_four = weather_next_four.limit(4)
display(weather_next_four)


# COMMAND ----------

# Create day_of_week column
weather_next_four = weather_next_four.withColumn("day_of_week", dayofweek("time"))
display(weather_next_four)

# COMMAND ----------

weather_next_four_df = weather_next_four.select(to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss").alias('ds'), col("feels_like"), col("clouds"), col("wind_speed"), col("day_of_week"))
# weather_next_four_df = weather_next_four_df.withColumn('y', lit(0))
display(weather_next_four_df)

# Convert to pandas
weather_next_four_pd = weather_next_four_df.toPandas()

# COMMAND ----------

display(predict_data_pd)

# COMMAND ----------

model_prod_uri = f'models:/{ARTIFACT_PATH}/production'
model_prod = mlflow.prophet.load_model(model_prod_uri)

prod_forecast = model_prod.predict(weather_next_four_pd)
prod_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

weather_next_four_pd.ds = pd.to_datetime(weather_next_four_pd.ds)
prod_forecast.ds = pd.to_datetime(prod_forecast.ds)
results_forecast = prod_forecast[['ds','yhat']].merge(weather_next_four_pd,on="ds")
display(results_forecast)
results_forecast = spark.createDataFrame(results_forecast)


# COMMAND ----------

# DBTITLE 1,Write the inferences to Gold Table
results = spark.createDataFrame(results)
MODEL_INFERENCING_INFO = GROUP_DATA_PATH + "Gold_Inferencing_tables"

#results = spark.createDataFrame(results)
results.write.format("delta").option("path", MODEL_INFERENCING_INFO).mode("overwrite").save()

# COMMAND ----------


MODEL_FORECASTING_INFO = GROUP_DATA_PATH + "Gold_Forecasting_tables"

results_forecast.write.format("delta").option("path", MODEL_FORECASTING_INFO).mode("overwrite").save()


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
