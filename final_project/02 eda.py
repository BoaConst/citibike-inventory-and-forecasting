# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

  # Read the filtered data for visualization
delta_path = "dbfs:/FileStore/tables/G02/historic_bike_trip_g02/"

# Create df from the delta table
df_monthly_trips = spark.read.format("delta").load(delta_path)
display(df_monthly_trips.limit(10))

# COMMAND ----------

# Create month and year columns for analysis
from pyspark.sql.functions import year, month

# Assuming 'date_col' is the name of your date column
df_monthly_trips = df_monthly_trips.withColumn("year", year("started_at")).withColumn("month", month("started_at"))
display(df_monthly_trips.limit(10))

# COMMAND ----------

from pyspark.sql.functions import concat, count, lit
import matplotlib.pyplot as plt

# Group the data by year_month and count the number of rows in each group
df_monthly_trips = df_monthly_trips.groupBy("year", "month").agg(count("*").alias("Number of Bike Trips")).orderBy("year", "month")

# Concatenate the year and month columns to create the 'year_month' column
df_monthly_trips = df_monthly_trips.withColumn("year_month", 
                    concat(df_monthly_trips["year"], lit("-"), df_monthly_trips["month"]))


# Convert the DataFrame to a Pandas DataFrame for plotting
df_monthly_trips_pd = df_monthly_trips.toPandas()

# Plot the data as a line chart
plt.plot(df_monthly_trips_pd["year_month"], df_monthly_trips_pd["Number of Bike Trips"])
plt.xticks(rotation=90)
plt.xlabel("Year-Month")
plt.ylabel("Number of Bike Trips")
plt.title("Monthly Trend of Number of Bike Trips")
plt.show()


# COMMAND ----------

# Read the filtered data for visualization
delta_path = "dbfs:/FileStore/tables/G02/historic_bike_trip_g02/"

# Create df from the delta table
df_daily_trips = spark.read.format("delta").load(delta_path)
display(df_daily_trips.limit(10))

# COMMAND ----------

from pyspark.sql.functions import date_format

# Convert 'started_at' to date format
df_daily_trips = df_daily_trips.withColumn("start_date", date_format("started_at", "yyyy-MM-dd").cast("date"))
df_daily_trips = df_daily_trips.withColumn("year", year("start_date")).withColumn("month", month("start_date"))
display(df_daily_trips.limit(10))

# COMMAND ----------

# Group the data by date and count the number of rows in each group
df_daily_trips_line = df_daily_trips.groupBy("start_date").agg(count("*").alias("Number of Bike Trips")).orderBy("start_date")

df_daily_trips_line_pd = df_daily_trips_line.toPandas()
df_daily_trips_pd = df_daily_trips.toPandas()

import plotly.express as px

# Create line chart of daily trips
fig = px.line(df_daily_trips_line_pd, x="start_date", y="Number of Bike Trips", title="Daily Bike Trips Trend")
fig.show()

# COMMAND ----------

import holidays

# Get list of US holidays for the year
us_holidays = holidays.US(years=2022)

# Create new column indicating whether or not each day is a holiday
df_daily_trips_pd['is_holiday'] = df_daily_trips_pd['start_date'].apply(lambda x: x in us_holidays)

# COMMAND ----------

df_daily_trips_pd.head()

# COMMAND ----------

import pandas as pd

pivot = pd.pivot_table(
    df_daily_trips_pd,
    values='ride_id',
    index=['year', 'month'],
    columns=['is_holiday'],
    aggfunc='count',
    fill_value=0
)

# calculate the mean number of rides for each month and year
pivot['mean_rides'] = pivot.mean(axis=1)

# calculate the mean number of rides for each month and year, separated by holiday and non-holiday
pivot['mean_holiday_rides'] = pivot[True] / pivot[True].sum()
pivot['mean_non_holiday_rides'] = pivot[False] / pivot[False].sum()

# create a stacked bar chart to compare the average number of rides per month and year, by holiday and non-holiday
pivot[['mean_non_holiday_rides', 'mean_holiday_rides']].plot(kind='bar', stacked=False)



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


