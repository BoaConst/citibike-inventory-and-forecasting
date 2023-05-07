# Databricks notebook source
# MAGIC  %run ./includes/includes

# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

DataforModelling = spark.read \
                        .format('delta') \
                        .option("header", "true") \
                        .load('dbfs:/FileStore/tables/G02/DataforModelling/')


# COMMAND ----------



# COMMAND ----------

import numpy as np
import pandas as pd
# import pickle
import random
import datetime as dt
from datetime import datetime
from datetime import timedelta
import patsy
from tqdm import tqdm
import matplotlib.pyplot as plt
%matplotlib inline
import seaborn as sns
sns.set()
sns.set_style("whitegrid", {'axes.grid' : False})

from sklearn import preprocessing
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error

from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation
from fbprophet.diagnostics import performance_metrics
from fbprophet.plot import plot_cross_validation_metric
from fbprophet.plot import add_changepoints_to_plot

# from helper_functions import mae, mape, rmse, plot_forecast, is_weekend, is_warm

# COMMAND ----------

df = DataforModelling.toPandas()

# COMMAND ----------

df['date'].unique()

# COMMAND ----------

df.head()

# COMMAND ----------

df['not_weekend'] = [False if (x == 5 or x == 6) else True for x in df.day_of_week]

# COMMAND ----------

df['is_weekend'] = [True if (x == 5 or x == 6) else False for x in df.day_of_week]

# COMMAND ----------

df['month'] = df['date'].apply(lambda x: x.month)

# COMMAND ----------

# df = df.drop(['end_ride_count'], axis=1)
# df = df.reset_index(drop=True)

# COMMAND ----------

# add column for whether day is in a warm month (may-sept)
df['is_warm'] = [True if x in [5,6,7,8,9] else False for x in df.month]
df['not_warm'] = [False if x in [5,6,7,8,9] else True for x in df.month]

# COMMAND ----------

df['is_warm']

# COMMAND ----------

# create dataframe for notable days including summer streets events and observance days
summer_streets = pd.DataFrame({
  'holiday': 'summer_streets',
  'ds': pd.to_datetime(['2022-08-06', '2022-08-07', '2022-08-08', '2022-08-09',
                        '2022-08-10', '2022-08-11', '2022-08-12', '2022-08-13', 
                        '2022-08-14', '2022-08-15', '2022-08-16', '2022-08-17',
                        '2022-08-18', '2022-08-19','2022-08-20', '2017-08-19',]),
  'lower_window': 0,
  'upper_window': 0,
})

mothers_day = pd.DataFrame({
  'holiday': 'mothers_day',
  'ds': pd.to_datetime(['2022-05-08']),
  'lower_window': 0,
  'upper_window': 0,
})

fathers_day = pd.DataFrame({
  'holiday': 'fathers_day',
  'ds': pd.to_datetime(['2022-06-19']),
  'lower_window': 0,
  'upper_window': 0,
})

holidays = pd.concat((summer_streets, mothers_day, fathers_day))

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10,6))
ax.plot(df.date, df.start_ride_count)
ax.set_facecolor('whitesmoke')
ax.title.set_text('Daily volume of Citibike rides from West St & Chambers St')
ax.set_xlabel('Date')
ax.set_ylabel('# of Citibike rides');

# COMMAND ----------

# # split train and test for a select station 491
# citibike_station = df[['date', 'is_weekend', 'not_weekend','is_warm', 'not_warm', 'start_ride_count']]

# last_date = pd.Timestamp(2022, 12, 31) 
# delta = timedelta(days = 364)
# test_start_date = last_date - delta # test date begins 1/01/2023

# station_cv = citibike_station.loc[(citibike_station.date < test_start_date), 
#                                       ['date', 'is_weekend', 'not_weekend', 
#                                        'is_warm', 'not_warm', 'start_ride_count']]

# station_test = citibike_station.loc[(citibike_station.date >= test_start_date), 
#                                   ['date', 'is_weekend', 'not_weekend', 
#                                    'is_warm', 'not_warm', 'start_ride_count']]

# station_cv.columns = ['ds', 'is_weekend', 'not_weekend', 'is_warm', 'not_warm', 'y']
# station_test.columns = ['ds', 'is_weekend', 'not_weekend', 'is_warm', 'not_warm', 'y']

# COMMAND ----------

# Select the relevant rows from the original DataFrame
train_data = df.loc[df['date'] <= pd.Timestamp(2022, 12, 31)]
test_data = df.loc[(df['date'] >= pd.Timestamp(2023, 1, 1)) & (df['date'] <= pd.Timestamp(2023, 3, 31))]

# Extract the columns you need for modeling
train_data = train_data[['date', 'is_weekend', 'not_weekend', 'is_warm', 'not_warm', 'start_ride_count']]
test_data = test_data[['date', 'is_weekend', 'not_weekend', 'is_warm', 'not_warm', 'start_ride_count']]

# Rename the columns for consistency with your original code
train_data.columns = ['ds', 'is_weekend', 'not_weekend', 'is_warm', 'not_warm', 'y']
test_data.columns = ['ds', 'is_weekend', 'not_weekend', 'is_warm', 'not_warm', 'y']


# COMMAND ----------


# # create prophet object with parameters, fit with train data and do cross validation 
# m = Prophet(daily_seasonality = 10, # True 5 20 100
#             weekly_seasonality = 5, # True 10 100
#             yearly_seasonality = 5, # True 10 20 100!            
#             holidays = holidays,
#             seasonality_mode = 'multiplicative',
#             seasonality_prior_scale = 10, #100
#             holidays_prior_scale = 10, #) #100
#             changepoint_prior_scale = 0.20) # .20 .15 .25

# m.add_seasonality(name='monthly', period=30, fourier_order=10) # 5 100
# m.add_seasonality(name='is_weekend', period=7, fourier_order=10, condition_name='is_weekend')
# m.add_seasonality(name='not_weekend', period=7, fourier_order=3, condition_name='not_weekend')
# m.add_seasonality(name='is_warm', period=30, fourier_order=3, condition_name='is_warm')
# m.add_seasonality(name='not_warm', period=30, fourier_order=3, condition_name='not_warm')

# m.add_country_holidays(country_name='US')

# m.fit(station_cv)

# cv_results = cross_validation(m, initial='1095 days', horizon='365 days')
# print(cv_results)
     

# COMMAND ----------

# Import Prophet and related functions
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation

# Create Prophet object with desired parameters
m = Prophet(daily_seasonality=10, 
            weekly_seasonality=5, 
            yearly_seasonality=5, 
            holidays=holidays,
            seasonality_mode='multiplicative',
            seasonality_prior_scale=10, 
            holidays_prior_scale=10, 
            changepoint_prior_scale=0.20)

m.add_seasonality(name='monthly', period=30, fourier_order=10)
m.add_seasonality(name='is_weekend', period=7, fourier_order=10, condition_name='is_weekend')
m.add_seasonality(name='not_weekend', period=7, fourier_order=3, condition_name='not_weekend')
m.add_seasonality(name='is_warm', period=30, fourier_order=3, condition_name='is_warm')
m.add_seasonality(name='not_warm', period=30, fourier_order=3, condition_name='not_warm')
m.add_country_holidays(country_name='US')

# Fit the model to the training data
m.fit(train_data)

# Set up cross-validation with the desired initial and horizon parameters
cv_results = cross_validation(m, 
                              initial=pd.Timedelta(days=365), 
                              period=pd.Timedelta(days=30), 
                              horizon=pd.Timedelta(days=90))


# COMMAND ----------

# Calculate available bikes column
df["available_bikes"] = df["start_ride_count"] - df["end_ride_count"]

# Convert date_hour column to datetime format
df["date_hour"] = pd.to_datetime(df["date_hour"])

# Prepare the dataset for Prophet model
prophet_data = df[["date_hour", "available_bikes"]]
prophet_data.columns = ["ds", "y"]
