# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# print(start_date,end_date,hours_to_forecast, promote_model)
# print("YOUR CODE HERE...")

# COMMAND ----------

# DBTITLE 1,Helper Functions for EDA
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

import plotly.express as px
import pandas as pd

def plot_spark_dataframe(df, x_col, y_col, plot_type, title):
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = df.select(x_col, y_col).toPandas()

    # Plot the Pandas DataFrame using Plotly
    fig = px.bar(pandas_df, x=x_col, y=y_col) if plot_type == "bar" else \
          px.line(pandas_df, x=x_col, y=y_col) if plot_type == "line" else \
          px.scatter(pandas_df, x=x_col, y=y_col)

    # Set the title
    fig.update_layout(title=title)
        
    # Display the plot
    fig.show()

def plot_spark_dataframe_with_color(df, x_col, y_col, color_col, plot_type, title):
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = df.select(x_col, y_col, color_col).toPandas()

    # Plot the Pandas DataFrame using Plotly
    fig = px.bar(pandas_df, x=x_col, y=y_col, color=color_col) if plot_type == "bar" else \
          px.line(pandas_df, x=x_col, y=y_col, color=color_col) if plot_type == "line" else \
          px.scatter(pandas_df, x=x_col, y=y_col, color=color_col)

    # Set the title
    fig.update_layout(title=title)
        
    # Display the plot
    fig.show()

def readDeltaTable(delta_table_path: str, isReadStream: bool) -> DataFrame:
    df = spark.createDataFrame([], StructType([]))
    if isReadStream:
        df = spark.readStream.format("delta") \
            .load(delta_table_path)
    else:
        df = spark.read.format("delta") \
            .load(delta_table_path)

    display(df)
    return df

# COMMAND ----------

# Check Delta tables in the Group Data Path
display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

# DBTITLE 1,Load Delta Tables need for EDA
# Load G02 historic bike trips data
table_name = 'Silver_G02_modelling_data'
G02_bike_trip_df = spark.read.format("delta").load(GROUP_DATA_PATH + table_name)
display(G02_bike_trip_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Monthly Trip Trends for G02: West St & Chambers St	</h1>

# COMMAND ----------

# Create month and year columns for analysis
from pyspark.sql.functions import year, month
G02_bike_trip_df = G02_bike_trip_df.withColumn("year", year("date")).withColumn("month", month("date"))
display(G02_bike_trip_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import concat, count, lit, sum
import matplotlib.pyplot as plt

# Group the data by year_month and count the number of rows in each group
df_monthly_trips = G02_bike_trip_df.groupBy("year", "month").agg(sum("start_ride_count").alias("Number of Bike Trips started"), sum("end_ride_count").alias("Number of Bike Trips ended")).orderBy("year", "month")

# Concatenate the year and month columns to create the 'year_month' column
df_monthly_trips = df_monthly_trips.withColumn("year_month", 
                    concat(df_monthly_trips["year"], lit("-"), df_monthly_trips["month"]))


# COMMAND ----------

plot_spark_dataframe(df_monthly_trips, "year_month", "Number of Bike Trips started", "bar", "Monthly Trends for the Outgoing Bikes")
plot_spark_dataframe(df_monthly_trips, "year_month", "Number of Bike Trips ended", "bar", "Monthly Trends for the Incoming Bikes")
display(df_monthly_trips)

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Daily Trip Trends for G02: West St & Chambers St	</h1>

# COMMAND ----------

from pyspark.sql.functions import date_format

df_daily_trips = G02_bike_trip_df.withColumn("date", date_format("date", "yyyy-MM-dd").cast("date"))
df_daily_trips = df_daily_trips.withColumn("year", year("date")).withColumn("month", month("date"))
display(df_daily_trips.limit(10))

# COMMAND ----------

# Group the data by date and count the number of rows in each group
df_daily_trips_line = df_daily_trips.groupBy("date").agg(sum("start_ride_count").alias("Number of Bike Trips started"), sum("end_ride_count").alias("Number of Bike Trips ended")).orderBy("date")

# COMMAND ----------

plot_spark_dataframe(df_daily_trips_line, "date", "Number of Bike Trips started", "line", "Daily Trends for the Outgoing Bikes")
plot_spark_dataframe(df_daily_trips_line, "date", "Number of Bike Trips ended", "line", "Daily Trends for the Incoming Bikes")
display(df_daily_trips_line)

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Holiday Trends for G02: West St & Chambers St	</h1>

# COMMAND ----------

import holidays

# Generate a list of US holidays for 2022 and 2023
us_holidays_2021 = holidays.US(years=2021)
us_holidays_2022 = holidays.US(years=2022)
us_holidays_2023 = holidays.US(years=2023)

# Combine the holiday dictionaries for 2022 and 2023
us_holidays = {**us_holidays_2021, **us_holidays_2022, **us_holidays_2023}

us_holidays

# COMMAND ----------

# Convert the list of holidays into a Spark DataFrame
holiday_df = spark.createDataFrame([(str(date), True) for date in us_holidays.keys()], ["date", "is_holiday"])

# Join the holiday DataFrame with your main DataFrame to add holiday information
df_daily_trips_line = df_daily_trips_line.join(holiday_df, ["date"], "left_outer")

# fill null with False in is_holiday column
df_daily_trips_line = df_daily_trips_line.fillna(False)

df_daily_trips_line_pd = df_daily_trips_line.toPandas()

display(df_daily_trips_line)

# COMMAND ----------

from pyspark.sql.functions import date_format, col, when
import holidays
import matplotlib.pyplot as plt


# Group by holiday and calculate the average number of trips on each type of day
df_holiday_trips = df_daily_trips_line.groupBy("is_holiday").agg({"Number of Bike Trips started": "avg", "Number of Bike Trips ended": "avg"}) \
                                 .orderBy("is_holiday")

plot_spark_dataframe(df_holiday_trips, 'is_holiday', 'avg(Number of Bike Trips started)', "bar", "Aggregated Holiday trends for the Outgoing Bikes (Historical)")
plot_spark_dataframe(df_holiday_trips, 'is_holiday', 'avg(Number of Bike Trips ended)', "bar", "Aggregated Holiday trends for the Incoming Bikes (Historical)")
display(df_holiday_trips)

# COMMAND ----------

# Create month and year from date column
df_daily_trips_line = df_daily_trips_line.orderBy("date")
df_daily_trips_line = df_daily_trips_line.withColumn("year", year("date")).withColumn("month", month("date"))
df_daily_trips_line = df_daily_trips_line.withColumn("year_month", 
                    concat(df_daily_trips_line["year"], lit("-"), df_daily_trips_line["month"]))


plot_spark_dataframe_with_color(df_daily_trips_line, 'date', 'Number of Bike Trips started', "is_holiday", "line", "Holiday trends for the Outgoing Bikes (Historical)")
plot_spark_dataframe_with_color(df_daily_trips_line, 'date', 'Number of Bike Trips ended', "is_holiday", "line", "Holiday trends for the Incoming Bikes (Historical)")

# COMMAND ----------

from pyspark.sql.functions import date_format, max, col, ntile, when, to_timestamp, from_unixtime
from pyspark.sql.window import Window


df_weather = readDeltaTable(GROUP_DATA_PATH+'Silver_G02_modelling_data', False)

display(df_weather)

# COMMAND ----------

# Define the WindowSpec object by partitioning and ordering the data
window_spec = Window.partitionBy().orderBy("temp")

# Filter `df_daily_weather` to include only data from 2022
df_daily_weather_2022 = df_weather.filter(year(col("date")) == 2022)

# Divide the temperatures into three groups
df_weather_2022 = df_daily_weather_2022.withColumn("temp_group", ntile(3).over(window_spec))

# Get max temp for each temp_group
max_temps = df_weather_2022.groupBy("temp_group").agg(max(col("temp")).alias("max_temp"))

# Print the max temperatures for each group
max_temps.show()

low_temp_2022 = max_temps.select("max_temp").collect()[0][0]
mod_temp_2022 = max_temps.select("max_temp").collect()[1][0]
high_temp_2022 = max_temps.select("max_temp").collect()[2][0]


# COMMAND ----------

from pyspark.sql.functions import when

# Define the temperature groups

# Add the temperature group column
df_weather = df_weather.withColumn("temp_group", when(col("temp") < low_temp_2022, "Cold")
                                                         .when((col("temp") >= low_temp_2022) & (col("temp") < mod_temp_2022), "Moderate")
                                                         .when(col("temp") >= mod_temp_2022, "High"))


# COMMAND ----------

import pyspark.sql.functions as F
import matplotlib.pyplot as plt

# Group by temp_group and count the number of rows
df_grouped = df_weather.groupBy('temp_group').count()

plot_spark_dataframe(df_grouped, 'temp_group', 'count', "bar", "Distribution of Temperatures across the data")

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
