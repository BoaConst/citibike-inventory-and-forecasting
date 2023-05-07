# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

promote_model = True

# COMMAND ----------

# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)
# promote_model = True

# print(start_date,end_date,hours_to_forecast, promote_model)
# print("YOUR CODE HERE...")

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

from pyspark.sql.functions import desc

SOURCE_DATA = GROUP_DATA_PATH + "Silver_G02_modelling_data/"
ARTIFACT_PATH = GROUP_MODEL_NAME
np.random.seed(1234)

data_hist = (spark.read.format("delta").load(SOURCE_DATA))

# display(data)
## Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# data.printSchema()

new_df_hist = data_hist.select(col("timestamp").alias('ds'), col("feels_like"), col("clouds"), col("wind_speed"), col("day_of_week"),col("net_change").alias('y'))

# display(new_df.orderBy("ds"))


latest_end_timestamp_in_historic_data = new_df_hist.select("ds").sort(desc("ds")).head(1)[0][0]

print(latest_end_timestamp_in_historic_data)

SOURCE_DATA = GROUP_DATA_PATH + "Gold_G02_modelling_data/"
ARTIFACT_PATH = GROUP_MODEL_NAME
np.random.seed(1234)


data_live = (spark.read.format("delta").load(SOURCE_DATA))

# display(data)
## Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# data.printSchema()

new_df_live = data_live.select(col("timestamp").alias('ds'), col("feels_like"), col("clouds"), col("wind_speed"), col("day_of_week"),col("net_change").alias('y'))

new_df_live = new_df_live.filter(col("ds") > latest_end_timestamp_in_historic_data) 

display(new_df_live.orderBy("ds"))

data = new_df_hist.union(new_df_live)

display(data.orderBy("ds", ascending = False))


# COMMAND ----------

# Write the data to Delta Table
data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", GROUP_DATA_PATH+'Silver_hist_live_Data_Modelling').saveAsTable('Silver_hist_live_Data_Modelling')

# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

data.printSchema()

# COMMAND ----------

# Assuming the date column is named "date_col"
max_date = data.select(max('ds')).collect()[0][0]

filter_date = max_date - datetime.timedelta(days=40)

data = data.filter(col("ds") < filter_date) 

# COMMAND ----------

data.printSchema()
numberofDaysToPredict = 10
numberofHoursToForecast = 4

# Assuming the date column is named "date_col"
max_date = data.select(max("ds")).collect()[0][0]

print(max_date)

number_of_days_to_predict_date = max_date - datetime.timedelta(days=numberofDaysToPredict)

test_data = data.filter(col("ds") > number_of_days_to_predict_date) 
train_data = data.filter(col("ds") <= number_of_days_to_predict_date) 

train_data_pd = train_data.toPandas()
test_data_pd  = test_data.toPandas()


train_data_pd = train_data_pd.dropna()
test_data_pd= test_data_pd.dropna()
display(train_data)



# Subtracting 10 days from max date
# numberofDaysToPredictDate = date_sub(max_date, numberofDaysToPredict)

# test_data = new_df_live.filter(col("ds") > numberofDaysToPredictDate) 
# train_data = new_df_live.filter(col("ds") <= numberofDaysToPredictDate) 

# display(test_data)
# df = new_df.toPandas()
# train_data = df.sample(frac=0.9, random_state=42)
# test_data = df.drop(train_data.index)
# x_train, y_train, x_test, y_test = train_data["ds"], train_data["y"], test_data["ds"], test_data["y"]


# COMMAND ----------

data_pd =data.toPandas()
import plotly.express as px

fig = px.line(data.toPandas(), x="ds", y="y", title='Net bike change')
fig.show()

# COMMAND ----------

# train_data.printSchema()
# from pyspark.sql import functions as F

# # Loop through all columns in the dataframe and filter out rows with null values
# train_data = train_data.dropna(subset=['y'])
# for col_name in train_data.columns:
#     null_count = train_data.filter(F.col(col_name).isNull()).count()
#     if null_count > 0:
#         print("Column '{}' has {} null values".format(col_name, null_count))
#     else:
#         print("Column '{}' has no null values".format(col_name))

# train_data =train_data.printSchema()
# test_data =test_data.printSchema()


# COMMAND ----------

from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error,mean_absolute_error

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

# Create a list to store MAPE values for each combination
maes = [] 

#for commmit
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

        # Cross-validation
        # df_cv = cross_validation(model=m, initial='710 days', period='180 days', horizon = '365 days', parallel="threads")
        # Model performance
        # df_p = performance_metrics(m, rolling_window=1)

        # try:
        #     metric_keys = ["mse", "rmse", "mae", "mape", "mdape", "smape", "coverage"]
        #     metrics = {k: df_p[k].mean() for k in metric_keys}
        #     params = extract_params(m)
        # except:
        #     pass

        # print(f"Logged Metrics: \n{json.dumps(metrics, indent=2)}")
        # print(f"Logged Params: \n{json.dumps(params, indent=2)}")
        y_pred = m.predict(test_data_pd)
        rmse = np.sqrt(mean_squared_error(test_data_pd['y'], y_pred['yhat'])) # Updated line

        mlflow.prophet.log_model(m, artifact_path=ARTIFACT_PATH)
        mlflow.log_params(params)
        mlflow.log_metrics({'rmse': rmse}) # Updated line

        model_uri = mlflow.get_artifact_uri(ARTIFACT_PATH)
        print(f"Model artifact logged to: {model_uri}")

        # Save model performance metrics for this combination of hyper parameters
        maes.append((rmse, model_uri))


# COMMAND ----------

# # Tuning results


tuning_results = pd.DataFrame(all_params)
tuning_results['rmse'] = list(zip(*maes))[0] # Updated line
tuning_results['model']= list(zip(*maes))[1]

best_params = dict(tuning_results.iloc[tuning_results[['rmse']].idxmin().values[0]]) # Updated line

best_params


# COMMAND ----------

loaded_model = mlflow.prophet.load_model(best_params['model'])

forecast = loaded_model.predict(test_data_pd)

print(f"forecast:\n${forecast.tail(40)}")

# COMMAND ----------

prophet_plot = loaded_model.plot(forecast)

# COMMAND ----------

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
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
    
# display(df)

print(best_params['rmse'])
df = spark.read.format("delta") \
    .load(delta_table_path)

display(df)

# COMMAND ----------

# Call MLFlow Client
client = MlflowClient()
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


model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

latest_version_info = client.get_latest_versions(model_details.name, stages=["Staging"])
latest_production_version = latest_version_info[0].version
print("The latest production version of the model '%s' is '%s'." % (model_details.name, latest_production_version))

# COMMAND ----------

model_staging_uri = "models:/{model_name}/staging".format(model_name=ARTIFACT_PATH)

print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_staging_uri))

model_staging = mlflow.prophet.load_model(model_staging_uri)

# COMMAND ----------

model_staging.plot(model_staging.predict(test_data_pd))

