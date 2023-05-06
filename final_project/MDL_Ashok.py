# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------


# Loading weather trip data from the delta tables
info_df = spark.read.format("delta").load(BRONZE_STATION_INFO_PATH)
status_df = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH)
nyc_weather_df = spark.read.format("delta").load(BRONZE_NYC_WEATHER_PATH)

# COMMAND ----------

display(info_df)
display(status_df)
display(nyc_weather_df)


# COMMAND ----------

import pyspark.sql.functions as F
print(GROUP_STATION_ASSIGNMENT)

G_02_station_capacity = (info_df.filter(F.col('name')== GROUP_STATION_ASSIGNMENT)).select("capacity").collect()[0][0];
# capacity = station_capacity["capacity"]
display(G_02_station_capacity)

# COMMAND ----------

status_df.count()

# COMMAND ----------

from pyspark.sql.functions import max

# Loading weather trip data from the delta tables
weather_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_nyc_weather_ashok/')
bike_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_bike_trip_ashok/')
starting_bike_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_bike_trip_starting_g02_ashok/')
ending_bike_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_bike_trip_ending_g02_ashok/')



# COMMAND ----------

starting_bike_df = starting_bike_df.dropDuplicates(["ride_id"])
ending_bike_df = ending_bike_df.dropDuplicates(["ride_id"])
weather_df = weather_df.dropDuplicates(["dt"])
display(weather_df.orderBy("dt"))
display(starting_bike_df)
display(ending_bike_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, from_unixtime
weather_df_with_date = weather_df.withColumn("timestamp", to_timestamp(from_unixtime(weather_df["dt"])))
display(weather_df_with_date.orderBy("timestamp"))
display(starting_bike_df.orderBy("started_at",ascending = False))
display(ending_bike_df.orderBy("ended_at",ascending = False))

# COMMAND ----------

from pyspark.sql.functions import hour, date_format
starting_bike_df = starting_bike_df.withColumn("date", date_format("started_at", "yyyy-MM-dd"))
ending_bike_df = ending_bike_df.withColumn("date", date_format("ended_at", "yyyy-MM-dd"))
starting_rides_per_hour = starting_bike_df.groupBy("date", hour("started_at").alias("hour") ).count()
ending_rides_per_hour = ending_bike_df.groupBy("date", hour("ended_at").alias("hour") ).count()
# rides_per_hour = bike_df.groupBy(("started_at"), hour("started_at")).count()


starting_rides_per_hour = starting_rides_per_hour.withColumnRenamed("count", "start_ride_count")

ending_rides_per_hour = ending_rides_per_hour.withColumnRenamed("count", "end_ride_count")

last_date = starting_rides_per_hour.orderBy("date","hour", ascending = False).first()
# display()
display(ending_rides_per_hour.orderBy("date","hour",ascending = False))



# COMMAND ----------

from pyspark.sql.functions import hour, expr

# Define the range of dates
# start_date = "2021-11-01"
# end_date = "2023-03-31"
from pyspark.sql.functions import min,max

start_date = starting_rides_per_hour.agg(min("date")).collect()[0][0]
end_date = ending_rides_per_hour.agg(max("date")).collect()[0][0]


display(start_date)
display(end_date)

# Create a DataFrame with all the possible date-hour combinations
dates_df = spark.range(0, (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1) \
               .withColumn("date", expr("date_add('{}', CAST(id AS int))".format(start_date)))
hours_df = spark.range(0, 24).withColumn("hour_of_day", hour(expr("timestamp('2000-01-01 ' || id || ':00:00')")))
all_hours_df = dates_df.crossJoin(hours_df) \
                       .withColumn("date", expr("date_format(date, 'yyyy-MM-dd')"))

# Display the result
all_hours_df = all_hours_df.drop("id")
all_hours_df = all_hours_df.withColumnRenamed("hour_of_day", "hour")

display(all_hours_df)


# COMMAND ----------

result_df = all_hours_df.join(starting_rides_per_hour, ["date", "hour"], "left_outer") \
                        .fillna(0, subset=["start_ride_count"]) \
                        .orderBy("date", "hour")
# display(result_df)
result_df = result_df.join(ending_rides_per_hour, ["date", "hour"], "left_outer") \
                        .fillna(0, subset=["end_ride_count"]) \
                        .orderBy("date", "hour")
result_df = result_df.withColumn("net_change", result_df["end_ride_count"] - result_df["start_ride_count"])
display(result_df)



# COMMAND ----------


from pyspark.sql.functions import hour, to_date

weather_df_with_date = weather_df_with_date.withColumn("hour", hour("timestamp"))
weather_df_with_date = weather_df_with_date.withColumn("date", to_date("timestamp"))

display(weather_df_with_date.orderBy("date","hour",ascending=True))
weather_df_with_date.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, avg, mode
from pyspark.sql.functions import date_format, dayofweek,when

# Select columns with integer and string data types
int_cols = ["temp","feels_like","pressure","humidity","dew_point","uvi","clouds","visibility","wind_speed","wind_deg","pop","snow_1h","rain_1h"]
str_cols = ["main","description"]

# Group by date and hour and compute average and mode of columns
grouped_weather_df = weather_df_with_date.groupBy("date", "hour").agg(
    *[avg(col).alias(col) for col in int_cols],
    *[mode(col).alias(col) for col in str_cols]
)

# Show the resulting dataframe
grouped_weather_df = grouped_weather_df.withColumn("day_of_week", date_format("date", "E")) \
                                       .withColumn("is_weekend", when(dayofweek("date").isin([7,1]), 1).otherwise(0))

display(grouped_weather_df.orderBy("date","hour"))


# COMMAND ----------

from pyspark.sql.functions import col

joined_df = grouped_weather_df.join(result_df, ["date", "hour"], "left_outer")
display(joined_df)
joined_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

# Loop through all columns in the dataframe and filter out rows with null values
joined_df = joined_df.dropna(subset=['net_change'])
for col_name in joined_df.columns:
    null_count = joined_df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print("Column '{}' has {} null values".format(col_name, null_count))
    else:
        print("Column '{}' has no null values".format(col_name))


# COMMAND ----------

from pyspark.sql.functions import col, count, desc
mode = joined_df.groupBy("visibility").agg(count("*").alias("count")).orderBy(desc("count")).first()[0]
# joined_df = joined_df.fillna(mode, subset=["col1"])

joined_df = joined_df.fillna(mode, subset=["visibility"])


# COMMAND ----------



from pyspark.sql.functions import concat, lit, to_timestamp,col,lpad

joined_df = joined_df.withColumn(
    "hour", lpad(col("hour").cast("string"), 2, "0")
)

joined_df = joined_df.withColumn('date_hour', concat('date', lit(' '), 'hour', lit(':00')))
# joined_df = joined_df.withColumn("date_hour", concat(col("date"), " ", col("hour").cast("string").rpad(2, '0')))


joined_df = joined_df.withColumn('timestamp', to_timestamp('date_hour', 'yyyy-MM-dd HH:mm'))

# joined_df = joined_df.withColumn("ds", to_timestamp(concat(col("date"), col("hour"))))

display(joined_df)

joined_df.printSchema()

# COMMAND ----------

# joined_df.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH+"DataforModelling/")
# pip install fbprophet
!pip install fbprophet


# COMMAND ----------

from fbprophet import Prophet
import pyspark.sql.functions as F
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error


target_col = "net_change"
prophet_df = joined_df.selectExpr("date_hour as ds", target_col + " as y", "temp", "feels_like", "pressure", "humidity", "wind_speed", "rain_1h")
prophet_df = prophet_df.toPandas()
prophet_df['ds'] = pd.to_datetime(prophet_df['ds'])

train_data, test_data = train_test_split(prophet_df, test_size=0.05, shuffle=False)

m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True, seasonality_mode="multiplicative")
# m.add_regressor("temp")
# m.add_regressor("feels_like")
# m.add_regressor("pressure")
# m.add_regressor("humidity")
# m.add_regressor("wind_speed")
# m.add_regressor("rain_1h")
# Add regressors
m.add_regressor("temp", standardize=True)
m.add_regressor("feels_like", standardize=True)
m.add_regressor("pressure", standardize=True)
m.add_regressor("humidity", standardize=True)
m.add_regressor("wind_speed", standardize=True)
m.add_regressor("rain_1h", standardize=True)

m.fit(train_data)

future = pd.DataFrame({
    'ds': test_data['ds'],
    'temp': test_data['temp'],
    'feels_like': test_data['feels_like'],
    'pressure': test_data['pressure'],
    'humidity': test_data['humidity'],
    'wind_speed': test_data['wind_speed'],
    'rain_1h': test_data['rain_1h']
})
forecast = m.predict(future)

# future = pd.concat([future, test_data.drop("y", axis=1)], axis=1)
# print(future)

forecast = m.predict(future)

print(forecast)


print(test_data['y'])

mape = mean_absolute_percentage_error(test_data['y'], forecast['yhat'])
mse = mean_squared_error(test_data['y'], forecast['yhat'])

print("MAPE: {:.2f}%".format(mape * 100))
print("MSE: {:.2f}".format(mse))



# COMMAND ----------

from fbprophet import Prophet
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
from fbprophet.diagnostics import cross_validation, performance_metrics
from fbprophet.plot import plot_cross_validation_metric
from hyperopt import fmin, tpe, hp

# ... (your existing code for loading and preprocessing the data)
target_col = "net_change"
prophet_df = joined_df.selectExpr("date_hour as ds", target_col + " as y", "temp", "pressure", "humidity", "wind_speed")
prophet_df = prophet_df.toPandas()
prophet_df['ds'] = pd.to_datetime(prophet_df['ds'])

train_data, test_data = train_test_split(prophet_df, test_size=0.10, shuffle=False)


def train_and_evaluate(params):
    m = Prophet(
        daily_seasonality=params["daily_seasonality"],
        weekly_seasonality=params["weekly_seasonality"],
        yearly_seasonality=params["yearly_seasonality"],
        seasonality_mode=params["seasonality_mode"],
        changepoint_prior_scale=params["changepoint_prior_scale"],
        seasonality_prior_scale=params["seasonality_prior_scale"],
    )

    # Add regressors
    m.add_regressor("temp", standardize=True)
    m.add_regressor("pressure", standardize=True)
    m.add_regressor("humidity", standardize=True)
    m.add_regressor("wind_speed", standardize=True)

    m.fit(train_data)

    df_cv = cross_validation(m, initial='200 days', period='180 days', horizon='100 days')
    df_p = performance_metrics(df_cv, rolling_window=1)
    print(df_p)
    rmse = df_p['rmse'].mean()

    return rmse

space = {
    "daily_seasonality": hp.choice("daily_seasonality", [True, False]),
    "weekly_seasonality": hp.choice("weekly_seasonality", [True, False]),
    "yearly_seasonality": hp.choice("yearly_seasonality", [True, False]),
    "seasonality_mode": hp.choice("seasonality_mode", ["additive", "multiplicative"]),
    "changepoint_prior_scale": hp.loguniform("changepoint_prior_scale", -5, 0),
    "seasonality_prior_scale": hp.loguniform("seasonality_prior_scale", -5, 0),
}

best_params = fmin(
    fn=train_and_evaluate,
    space=space,
    algo=tpe.suggest,
    max_evals=10,
    rstate=np.random.default_rng(42),
    verbose=2,
)

best_params["daily_seasonality"] = bool(best_params["daily_seasonality"])
best_params["weekly_seasonality"] = bool(best_params["weekly_seasonality"])
best_params["yearly_seasonality"] = bool(best_params["yearly_seasonality"])
best_params["seasonality_mode"] = ["additive", "multiplicative"][best_params["seasonality_mode"]]




# COMMAND ----------

# Add regressors

final_model = Prophet(**best_params)
final_model.add_regressor("temp", standardize=True)
# final_model.add_regressor("feels_like", standardize=True)
final_model.add_regressor("pressure", standardize=True)
final_model.add_regressor("humidity", standardize=True)
final_model.add_regressor("wind_speed", standardize=True)
# final_model.add_regressor("rain_1h", standardize=True)

final_model.fit(train_data)
future = pd.DataFrame({
    'ds': test_data['ds'],
    'temp': test_data['temp'],
    # 'feels_like': test_data['feels_like'],
    'pressure': test_data['pressure'],
    'humidity': test_data['humidity'],
    'wind_speed': test_data['wind_speed']
    # 'rain_1h': test_data['rain_1h']
})


forecast = final_model.predict(future)


mape = mean_absolute_percentage_error(test_data['y'], forecast['yhat'])
mse = mean_squared_error(test_data['y'], forecast['yhat'])



print("MSE: {:.2f}".format(mse))

print(forecast['yhat'])

#Plot the data
import matplotlib.pyplot as plt
indices = [i for i in range(0,250)]
plt.plot(indices, test_data['y'][:250], 'g-', label='Distance thumb_finger')
plt.plot(indices, forecast['yhat'][:250], 'b-', label='Cam Distance')
#plt.plot(indices, robust_dist, 'c-', label='Distance thumb_finger')


# Add axis labels and title
plt.xlabel('Predicted')
plt.ylabel('Velocity in pixel/sec')
plt.title('Robust Lengths between thumb and index finger (4, 2, 0)')
plt.legend()
# Display the plot
plt.show()

# COMMAND ----------

test_data

# COMMAND ----------


!pip install --upgrade fbprophet

# COMMAND ----------

# DBTITLE 1,Prophet Modelling
from fbprophet import Prophet
import pyspark.sql.functions as F

target_col = "net_change"
prophet_df = joined_df.selectExpr("timestamp as ds", target_col + " as y", "temp", "feels_like", "pressure", "humidity", "wind_speed", "rain_1h")

display(prophet_df)
prophet_df.printSchema()
m = Prophet(seasonality_mode="multiplicative", yearly_seasonality=True)

# Add seasonality components
m.add_seasonality(name='daily', period=1, fourier_order=7)
m.add_seasonality(name='weekly', period=7, fourier_order=14)

# Add regressors
m.add_regressor("temp", standardize=True)
m.add_regressor("feels_like", standardize=True)
m.add_regressor("pressure", standardize=True)
m.add_regressor("humidity", standardize=True)
m.add_regressor("wind_speed", standardize=True)
m.add_regressor("rain_1h", standardize=True)

m.fit(prophet_df)

# Make future dataframe
future = m.make_future_dataframe(periods=24*7, freq="H")
future = future.join(prophet_df.drop("y"), on="ds", how="left")

# Predict
forecast = m.predict(future)

fig = m.plot(forecast)


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import corr

# Define the input features and the target column
input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h', 'rain_1h']
target_col = 'net_'
import pyspark.sql.functions as F

# Convert string hour column to integer
joined_df = joined_df.withColumn("hour", F.regexp_replace("hour", "[^0-9]", "").cast("integer"))


# Assemble the input features into a single vector column
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

# Apply the assembler to the DataFrame to get a new DataFrame with a "features" column
df_with_features = assembler.transform(joined_df)

# Calculate the correlation matrix
corr_matrix = Correlation.corr(df_with_features, 'features').head()[0]

# Print the correlation matrix
print('Correlation Matrix:')
print(corr_matrix)

# Extract the correlations for the target column
correlations = df_with_features.select([corr(c, target_col).alias(c) for c in input_cols]).first()

# Print the correlations for the target column
print('Correlations with target column:')
print(correlations)


# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Convert categorical columns to numerical using StringIndexer and OneHotEncoder
main_indexer = StringIndexer(inputCol='main', outputCol='main_index')
# description_indexer = StringIndexer(inputCol='description', outputCol='description_index')
main_encoder = OneHotEncoder(inputCols=['main_index'], outputCols=['main_vec'])
# description_encoder = OneHotEncoder(inputCols=['description_index'], outputCols=['description_vec'])

# Define the input features and the target column
input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h', 'rain_1h', 'main_vec', 'description_vec']
# input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h', 'rain_1h']
target_col = 'count'

# Assemble the input features into a single vector column
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

# Split the data into training and test sets
train_data, test_data = joined_df.randomSplit([0.7, 0.3], seed=42)

# Define the linear regression model
lr = LinearRegression(featuresCol='features', labelCol=target_col)

# Chain the feature transformers and model together in a pipeline
pipeline = Pipeline(stages=[main_indexer, description_indexer, main_encoder, description_encoder, assembler, lr])
# pipeline = Pipeline(stages=[assembler, lr])

# Fit the pipeline on the training data
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model using RMSE
evaluator = RegressionEvaluator(labelCol=target_col, predictionCol='prediction', metricName='rmse')
rmse = evaluator.evaluate(predictions)
print('Root Mean Squared Error (RMSE) on test data = {:.2f}'.format(rmse))

# Get the R-squared value of the model on the test data
r2 = model.stages[-1].summary.r2
print('R-squared on test data = {:.2f}'.format(r2))


# COMMAND ----------

display(test_data)
display(predictions)

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# Define the input features and the target column
input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'pop', 'snow_1h', 'rain_1h']
target_col = 'net_change'

# Assemble the input features into a single vector column
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

# Split the data into training and test sets
train_data, test_data = joined_df.randomSplit([0.8, 0.1], seed=42)

# Define the Random Forest regression model
rf = RandomForestRegressor(featuresCol='features', labelCol=target_col)

# Chain the feature transformers and model together in a pipeline
pipeline = Pipeline(stages=[assembler, rf])

# Fit the pipeline on the training data
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model using RMSE
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='net_change', metricName='rmse')
rmse = evaluator.evaluate(predictions)
print('Root Mean Squared Error (RMSE) on test data = {:.2f}'.format(rmse))

# Get the R-squared value of the model on the test data
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print('R-squared on test data = {:.2f}'.format(r2))




# COMMAND ----------

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# Define the input features and the target column
input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h', 'rain_1h']
target_col = 'count'

# Assemble the input features into a single vector column
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

# Split the data into training and test sets
train_data, test_data = joined_df.randomSplit([0.7, 0.3], seed=42)

# Define the decision tree regression model
dt = DecisionTreeRegressor(featuresCol='features', labelCol=target_col)

# Chain the feature transformers and model together in a pipeline
pipeline = Pipeline(stages=[assembler, dt])

# Fit the pipeline on the training data
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model using RMSE
evaluator = RegressionEvaluator(labelCol=target_col, predictionCol='prediction', metricName='rmse')
rmse = evaluator.evaluate(predictions)
print('Root Mean Squared Error (RMSE) on test data = {:.2f}'.format(rmse))

# Get the R-squared value of the model on the test data
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print('R-squared on test data = {:.2f}'.format(r2))


# COMMAND ----------

# DBTITLE 1,GBT REGRESSOR
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# Define the input features and the target column
# input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'pop', 'snow_1h', 'rain_1h']
input_cols = ['hour', 'temp', 'pressure', 'humidity', 'dew_point', ]
target_col = 'net_change'

# Assemble the input features into a single vector column
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

# Split the data into training and test sets
train_data, test_data = joined_df.randomSplit([0.9, 0.1], seed=42)

# Define the Gradient-Boosted Tree regression model
gbt = GBTRegressor(featuresCol='features', labelCol=target_col)

# Chain the feature transformers and model together in a pipeline
pipeline = Pipeline(stages=[assembler, gbt])

# Fit the pipeline on the training data
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model using RMSE
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='net_change', metricName='rmse')
rmse = evaluator.evaluate(predictions)
print('Root Mean Squared Error (RMSE) on test data = {:.2f}'.format(rmse))

# Get the R-squared value of the model on the test data
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print('R-squared on test data = {:.2f}'.format(r2))





# COMMAND ----------

import matplotlib.pyplot as plt

# Extract the first 500 data points from the predictions dataframe

net_change = predictions.select('net_change').collect()
predicted = predictions.select('prediction').collect()
indices = [i for i in range(len(predicted))]
# Plot the data
plt.plot(indices, net_change, 'g-', label='Net Change')
plt.plot(indices, predicted, 'b-', label='Predictions')

# Add axis labels and title
plt.xlabel('Index')
plt.ylabel('Net Change')
plt.title('Predictions vs Net Change (First 500 Data Points)')

# Add legend
plt.legend()

# Display the plot
plt.show()


# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Define the input features and the target column
input_cols = ['hour', 'temp', 'pressure', 'humidity', 'dew_point']
target_col = 'net_change'

# Assemble the input features into a single vector column
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

# Define the Gradient-Boosted Tree regression model
gbt = GBTRegressor(featuresCol='features', labelCol=target_col)

# Chain the feature transformers and model together in a pipeline
pipeline = Pipeline(stages=[assembler, gbt])

# Define the hyperparameter grid to search over
param_grid = (ParamGridBuilder()
              .addGrid(gbt.maxDepth, [2, 4, 6])
              .addGrid(gbt.maxBins, [10, 20, 30])
              .addGrid(gbt.maxIter, [10, 20, 30])
              .build())

# Define the cross-validation object
cv = CrossValidator(estimator=pipeline, evaluator=evaluator, estimatorParamMaps=param_grid, numFolds=3)

# Fit the pipeline on the training data
cv_model = cv.fit(train_data)

# Make predictions on the test data
predictions = cv_model.transform(test_data)

# Evaluate the model using RMSE
rmse = evaluator.evaluate(predictions)
print('Root Mean Squared Error (RMSE) on test data = {:.2f}'.format(rmse))

# Get the R-squared value of the model on the test data
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print('R-squared on test data = {:.2f}'.format(r2))


# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Convert categorical columns to numerical using StringIndexer and OneHotEncoder
main_indexer = StringIndexer(inputCol='main', outputCol='main_index')
# description_indexer = StringIndexer(inputCol='description', outputCol='description_index')
main_encoder = OneHotEncoder(inputCols=['main_index'], outputCols=['main_vec'])
# description_encoder = OneHotEncoder(inputCols=['description_index'], outputCols=['description_vec'])

# Define the input features and the target column
input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h', 'rain_1h', 'main_vec']

target_col = 'count'

# Assemble the input features into a single vector column
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

# Split the data into training and test sets
train_data, test_data = joined_df.randomSplit([0.7, 0.3], seed=42)

# Define the linear regression model
gbt = GBTRegressor(featuresCol='features', labelCol=target_col)
# Chain the feature transformers and model together in a pipeline
pipeline = Pipeline(stages=[main_indexer, description_indexer, main_encoder, description_encoder, assembler, gbt])
# pipeline = Pipeline(stages=[assembler, lr])

# Fit the pipeline on the training data
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)


# Evaluate the model using RMSE
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='count', metricName='rmse')
rmse = evaluator.evaluate(predictions)
print('Root Mean Squared Error (RMSE) on test data = {:.2f}'.format(rmse))

# Get the R-squared value of the model on the test data
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print('R-squared on test data = {:.2f}'.format(r2))


# COMMAND ----------

from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import VectorAssembler

# Define the input features and the target column
# input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h', 'rain_1h']
input_cols = ['hour', 'temp', 'feels_like', 'pressure', 'humidity','wind_speed','rain_1h']
target_col = 'count'

# Assemble the input features into a single vector column
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

# Split the data into training and test sets
train_data, test_data = joined_df.randomSplit([0.7, 0.3], seed=42)

# Define the GBT regression model
gbt = GBTRegressor(featuresCol='features', labelCol=target_col, seed=42)

# Define the parameter grid to search over
param_grid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [2, 4, 6]) \
    .addGrid(gbt.maxBins, [20, 40, 60]) \
    .addGrid(gbt.minInstancesPerNode, [1, 3, 5]) \
    .addGrid(gbt.stepSize, [0.1, 0.01]) \
    .build()

# Define the evaluator
evaluator = RegressionEvaluator(labelCol=target_col, predictionCol='prediction', metricName='rmse')

# Define the cross validator
cv = CrossValidator(estimator=gbt, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=5, seed=42)

# Chain the feature transformers and model together in a pipeline
pipeline = Pipeline(stages=[assembler, cv])

# Fit the pipeline on the training data
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model using RMSE
rmse = evaluator.evaluate(predictions)
print('Root Mean Squared Error (RMSE) on test data = {:.2f}'.format(rmse))

# Get the best model from the cross validator
best_model = model.stages[-1].bestModel

# Get the best hyperparameters from the cross validator
best_maxDepth = best_model.getMaxDepth()
best_maxBins = best_model.getMaxBins()
best_minInstancesPerNode = best_model.getMinInstancesPerNode()
best_stepSize = best_model.getStepSize()

print('Best hyperparameters:')
print('maxDepth = {}'.format(best_maxDepth))
print('maxBins = {}'.format(best_maxBins))
print('minInstancesPerNode = {}'.format(best_minInstancesPerNode))
print('stepSize = {}'.format(best_stepSize))


# COMMAND ----------


