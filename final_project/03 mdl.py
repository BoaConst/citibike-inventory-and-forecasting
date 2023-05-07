# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# Using the promote_model widget to promote to production
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)
#promote_model = True

print(promote_model)

# COMMAND ----------

# Importing all the relavant libraries and functions required for modelling
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
from pyspark.sql.functions import desc
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error,mean_absolute_error
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# COMMAND ----------

# Read-in the silver delta table to be used for modelling

SOURCE_DATA = GROUP_DATA_PATH + "Silver_G02_modelling_data/"
ARTIFACT_PATH = GROUP_MODEL_NAME
np.random.seed(1234)

data_hist = (spark.read.format("delta").load(SOURCE_DATA))

## Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# Select the features for modelling Prophet model
new_df_hist = data_hist.select(col("timestamp").alias('ds'), col("feels_like"), col("clouds"), col("wind_speed"), col("day_of_week"),col("net_change").alias('y'))

# Get the last date timestamp in historic data
latest_end_timestamp_in_historic_data = new_df_hist.select("ds").sort(desc("ds")).head(1)[0][0]
print(latest_end_timestamp_in_historic_data)

# Read-in the Gold table that includes the live Delta tables 
SOURCE_DATA = GROUP_DATA_PATH + "Gold_G02_modelling_data/"
ARTIFACT_PATH = GROUP_MODEL_NAME
np.random.seed(1234)

data_live = (spark.read.format("delta").load(SOURCE_DATA))

## Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# Select the features for modelling Prophet model
new_df_live = data_live.select(col("timestamp").alias('ds'), col("feels_like"), col("clouds"), col("wind_speed"), col("day_of_week"),col("net_change").alias('y'))

# Filter the live data for data after the historic data
new_df_live = new_df_live.filter(col("ds") > latest_end_timestamp_in_historic_data) 

# Combine the historic and live data
data = new_df_hist.union(new_df_live)
display(data.orderBy("ds", ascending = False))


# COMMAND ----------

# Write the data to Silver Delta Table to be used again
data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH+'Silver_hist_live_Data_Modelling').saveAsTable('Silver_hist_live_Data_Modelling')

# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

# Calculate the max date in the dataframe
max_date = data.select(max('ds')).collect()[0][0]

# Offset the date by 40 days that will be used for training the model
filter_date = max_date - datetime.timedelta(days=40)

# Filter the data that will be used for modeling 
data = data.filter(col("ds") < filter_date) 

# COMMAND ----------

# Paramters to be used for modeling
numberofDaysToPredict = 10 #last 10 days will be used for model evaluation
numberofHoursToForecast = 4 # 4 Hours will be used for forcasting

# Calculate the max date in the dataframe
max_date = data.select(max("ds")).collect()[0][0]

# Offset the date to create filter criteria
number_of_days_to_predict_date = max_date - datetime.timedelta(days=numberofDaysToPredict)

# Based on filter, create test and train data
test_data = data.filter(col("ds") > number_of_days_to_predict_date) 
train_data = data.filter(col("ds") <= number_of_days_to_predict_date) 

# Convert to pandas dataframe
train_data_pd = train_data.toPandas()
test_data_pd  = test_data.toPandas()

train_data_pd = train_data_pd.dropna()
test_data_pd= test_data_pd.dropna()
display(train_data)

# COMMAND ----------

# Plot the Net bike change in plotly
data_pd =data.toPandas()
import plotly.express as px

fig = px.line(data.toPandas(), x="ds", y="y", title='Net bike change')
fig.show()

# COMMAND ----------

# Set up parameter grid
param_grid = {  
    'changepoint_prior_scale': [0.01, 0.005],
    'seasonality_prior_scale': [4, 8],
    'seasonality_mode': ['additive'],
    'yearly_seasonality' : [True],
    'weekly_seasonality': [True],
    'daily_seasonality': [True]
}

# Generate all combinations of parameters
all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]

print(f"Total training runs {len(all_params)}")

# Create a list to store MAEs values for each combination
maes = [] 

# Use cross validation to evaluate all parameters
for params in all_params:
    with mlflow.start_run(): 
        # Fit a model using one parameter combination + holidays
        m = Prophet(**params) 
        holidays = pd.DataFrame({"ds": [], "holiday": []})
        m.add_country_holidays(country_name='US')
        m.add_regressor('feels_like')
        m.add_regressor('wind_speed')
        m.add_regressor('clouds')
        m.add_regressor('day_of_week')
        m.fit(train_data_pd)

        # make prediction on the test data
        y_pred = m.predict(test_data_pd)

        # evaluate model performance
        rmse = np.sqrt(mean_squared_error(test_data_pd['y'], y_pred['yhat'])) # Updated line

        # Log model, perameters, metrics using mlflow
        mlflow.prophet.log_model(m, artifact_path=ARTIFACT_PATH)
        mlflow.log_params(params)
        mlflow.log_metrics({'rmse': rmse}) # Updated line
        model_uri = mlflow.get_artifact_uri(ARTIFACT_PATH)
        print(f"Model artifact logged to: {model_uri}")

        # Save model performance metrics for this combination of hyper parameters
        maes.append((rmse, model_uri))


# COMMAND ----------

# Tuning results
tuning_results = pd.DataFrame(all_params)
tuning_results['rmse'] = list(zip(*maes))[0]
tuning_results['model']= list(zip(*maes))[1]

# Store the best parameters after tuning the model
best_params = dict(tuning_results.iloc[tuning_results[['rmse']].idxmin().values[0]]) 
best_params


# COMMAND ----------

# Load the model for best parameters
loaded_model = mlflow.prophet.load_model(best_params['model'])

# predict the values on test data
forecast = loaded_model.predict(test_data_pd)
print(f"forecast:\n${forecast.tail(40)}")

# COMMAND ----------

# Plot the forcaseted values
prophet_plot = loaded_model.plot(forecast)

# COMMAND ----------

# Plot the Prophet componenets
prophet_plot2 = loaded_model.plot_components(forecast)

# COMMAND ----------

# Finding residuals
test_data.ds = pd.to_datetime(test_data_pd.ds)
forecast.ds = pd.to_datetime(forecast.ds)
results = forecast[['ds','yhat']].merge(test_data_pd,on="ds")
results['residual'] = results['yhat'] - results['y']

# COMMAND ----------

# Plot the residuals
fig = px.scatter(
    results, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols'
)
fig.show()

# COMMAND ----------

# Register Model to MLFlow
model_details = mlflow.register_model(model_uri=best_params['model'], name=ARTIFACT_PATH)

# COMMAND ----------

# Check for the best model using rmse and store the details of model in Gold table 'Gold_best_model_result_data'

model_result_schema = StructType([
        StructField("ModelName", StringType(), True),
        StructField("model", StringType(), True),
        StructField("version", IntegerType(), True),
        StructField("rmse", DoubleType(), True)
    ])
delta_table_path = GROUP_DATA_PATH + "Gold_best_model_result_data"
changeStaging = False
try:
    df = spark.read.format("delta").load(delta_table_path)
    rmse = df.first()['rmse']
    print(f"RMSE from delta table: {rmse}")
    if rmse > best_params['rmse']:
        changeStaging= True
        df = spark.createDataFrame([(model_details.name, best_params['model'], int(model_details.version), float(best_params['rmse']))], model_result_schema)

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)

except:
    # create new delta table if it doesn't exist
    
    print("Delta table does not exist yet.")
    df = spark.createDataFrame([(model_details.name, best_params['model'], int(model_details.version), float(best_params['rmse']))], model_result_schema)

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)
    
print(best_params['rmse'])
df = spark.read.format("delta") \
    .load(delta_table_path)

display(df)

# COMMAND ----------

# Call MLFlow Client
client = MlflowClient()

# Move to production based on the promode_model widget value
if promote_model:
    client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage='Production')
else:
    if changeStaging:
        client.transition_model_version_stage(
        name=model_details.name,
        version=model_details.version,
        stage='Staging'
)

# COMMAND ----------

# Load the model version in stage
model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

# Load the model version in production
latest_version_info = client.get_latest_versions(model_details.name, stages=["Staging"])
latest_production_version = latest_version_info[0].version
print("The latest production version of the model '%s' is '%s'." % (model_details.name, latest_production_version))

# COMMAND ----------

model_staging_uri = "models:/{model_name}/staging".format(model_name=ARTIFACT_PATH)
print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_staging_uri))
model_staging = mlflow.prophet.load_model(model_staging_uri)

# COMMAND ----------

# Plot the residuals
model_staging.plot(model_staging.predict(test_data_pd))

# COMMAND ----------

import json

# Return Success Code
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
