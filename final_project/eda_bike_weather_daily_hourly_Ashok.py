# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# Imports
from pyspark.sql.functions import year, month, sum, count, col, current_date, date_format, date_add
import matplotlib.pyplot as plt
from datetime import datetime

# COMMAND ----------


# Loading weather trip data from the delta tables
weather_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_nyc_weather_ashok/')
bike_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_bike_trip_g02_ashok/')

# COMMAND ----------

display(weather_df)

# COMMAND ----------

display(bike_df.orderBy("started_at",ascending=False))

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, from_unixtime


# Convert the date_long column to a date format
weather_df_with_date = weather_df.withColumn("date", to_timestamp(from_unixtime(weather_df["dt"])))

# Show the result
display(weather_df_with_date)

# COMMAND ----------

# Define the start date and end date for the last year
end_date = current_date()
start_date = date_add(end_date, (-12 * 30) + 5)

# COMMAND ----------

# Iterate over each month in the last year and plot the trends for each unique bike type
for i in range(12):
    # Generate the start and end dates for the current month
    current_start_date = date_add(start_date, i * 30)
    current_end_date = date_add(current_start_date, 30) if i < 11 else end_date
    
    # Make a date range query for the current month
    month_query = bike_df.filter((col("started_at") >= current_start_date) & (col("ended_at") < current_end_date))   
    month_query_count = month_query.count()
    
    # Group by rideable type
    grouped_data = month_query.groupBy("rideable_type").agg(count("ride_id").alias("total_riders"))
    
    # Convert to pandas dataframe
    monthly_data_pd = grouped_data.toPandas()
    
    # Plot the pandas dataframe using matplotlib
    fig, ax = plt.subplots()
    ax.bar(monthly_data_pd["rideable_type"], monthly_data_pd["total_riders"])
    ax.set_xticks(range(0, 3))
    ax.set_xticklabels(["Docked","Electric", "Classic"])
    ax.set_xlabel("Bike Type")
    ax.set_ylabel("Total Rides For Type")
    ax.set_title(f"Month {i+1}: {month_query_count} total rides")
    plt.show()

# COMMAND ----------

from pyspark.sql.functions import year, month

# Group by year and month and count the number of rides in each group
df_monthly_trips = bike_df.groupBy(year("started_at").alias("year"), 
                                                month("started_at").alias("month")) \
                                      .count() \
                                      .orderBy("year", "month")

display(df_monthly_trips)


# COMMAND ----------

import matplotlib.pyplot as plt

from pyspark.sql.functions import year, month

import numpy as np
# Group by year and month and count the number of rides in each group
monthly_trips_by_year = bike_df.groupBy(year("started_at").alias("year"), 
                                                month("started_at").alias("month")) \
                                      .count() \
                                      .orderBy("year", "month")
# extract the x axis data (years)
years = [row["year"] for row in monthly_trips_by_year.collect()]

# extract the y axis data (monthly trip counts for each year)
monthly_trip_counts = [list(row) for row in monthly_trips_by_year.toPandas().iloc[:, 1:].values]

print(years)

print(monthly_trip_counts)



# Extract the data for each year into separate arrays
year_data = {}
for year in set(years):
    year_data[year] = np.zeros(12)
for year, count in zip(years, monthly_trip_counts):
    month, trips = count
    year_data[year][month-1] = trips

# Plot the data for each year separately
fig, ax = plt.subplots()
for year in sorted(set(years)):
    ax.plot(range(1, 13), year_data[year], label=str(year))
plt.xlabel("Month")
plt.ylabel("Number of Trips")
plt.title("Monthly Bike Trip Trends by Year")
plt.legend(title="Year", loc="upper left")
plt.show()



# COMMAND ----------

from pyspark.sql.functions import date_format, dayofweek
from pyspark.sql.functions import when


# Group by day of the week and count the number of rides in each group
df_weekly_trips = bike_df.groupBy(dayofweek("started_at").alias("day_of_week")) \
                                      .count() \
                                      .orderBy("day_of_week")

# display(df_weekly_trips)

# Create a new column with the text of the day of the week
df_weekly_trips = df_weekly_trips.withColumn("day_of_week_text", when(df_weekly_trips.day_of_week == 1, "Sunday") \
                                                                .when(df_weekly_trips.day_of_week == 2, "Monday") \
                                                                .when(df_weekly_trips.day_of_week == 3, "Tuesday") \
                                                                .when(df_weekly_trips.day_of_week == 4, "Wednesday") \
                                                                .when(df_weekly_trips.day_of_week == 5, "Thursday") \
                                                                .when(df_weekly_trips.day_of_week == 6, "Friday") \
                                                                .when(df_weekly_trips.day_of_week == 7, "Saturday") \
                                                                .otherwise("Unknown"))

# Reorder the columns and select only the columns you want to keep
df_weekly_trips = df_weekly_trips.select("day_of_week_text", "count").orderBy("day_of_week")

display(df_weekly_trips)


# Extract the data from the DataFrame
# day_of_week = [row.day_of_week for row in df_weekly_trips.collect()]
day_of_week_text = [row.day_of_week_text for row in df_weekly_trips.collect()]
count = [row["count"] for row in df_weekly_trips.collect()]

# Create a bar chart of the weekly trip trends
plt.bar(day_of_week_text, count)
plt.xlabel("Day of the Week")
plt.ylabel("Number of Trips")
plt.title("Weekly Bike Trip Trends")
plt.show()


# COMMAND ----------

# Group by end station ID and count the number of rides that ended at each station
df_popular_stations = bike_df.groupBy("end_station_name") \
                                          .count() \
                                          .orderBy("count", ascending=False) \
                                          .limit(10)

display(df_popular_stations)


# COMMAND ----------

import matplotlib.pyplot as plt

# Get the data from the DataFrame
end_station_names = [row["end_station_name"] for row in df_popular_stations.collect()]
counts = [row["count"] for row in df_popular_stations.collect()]

# Create a horizontal bar graph
fig, ax = plt.subplots(figsize=(8, 6))
ax.barh(end_station_names, counts, color="blue")
ax.invert_yaxis()  # Invert the y-axis to show the most popular station on top
ax.set_xlabel("Number of Rides")
ax.set_ylabel("End Station")
ax.set_title("Top 10 End Stations by Number of Rides")
plt.show()


# COMMAND ----------

from pyspark.sql.functions import date_format
import matplotlib.pyplot as plt

# Group by date and count the number of rides on each date
df_daily_trips = bike_df.groupBy(date_format("started_at", "yyyy-MM-dd").alias("date")) \
                       .count() \
                       .orderBy("date")

# Convert the DataFrame to a Pandas DataFrame
df_daily_trips_pd = df_daily_trips.toPandas()

# Plot a line chart of the daily trip trends
plt.plot(df_daily_trips_pd['date'], df_daily_trips_pd['count'])

# Set the x-axis label and rotate the tick labels for better readability
plt.xlabel('Date')
plt.xticks(df_daily_trips_pd['date'][::30], rotation=45)

# Set the y-axis label
plt.ylabel('Number of trips')

# Set the title of the chart
plt.title('Daily Bike Trip Trends')

# Display the chart
plt.show()


# COMMAND ----------

from pyspark.sql.functions import date_format, col, when
import holidays
import matplotlib.pyplot as plt

# Define the list of holidays in the US for 2021, 2022, and 2023
us_holidays = holidays.US(years=[2021, 2022, 2023])

# Group by date and count the number of rides on each date
df_daily_trips = bike_df.groupBy(date_format("started_at", "yyyy-MM-dd").alias("date")) \
                       .agg({"started_at": "count"}) \
                       .withColumnRenamed("count(started_at)", "num_trips") \
                       .orderBy("date")

# Add a new column to indicate whether each date is a holiday
df_daily_trips = df_daily_trips.withColumn("is_holiday", when(col("date").isin(list(us_holidays)), "Yes").otherwise("No"))

# Group by holiday and calculate the average number of trips on each type of day
df_holiday_trips = df_daily_trips.groupBy("is_holiday").agg({"num_trips": "avg"}) \
                                 .withColumnRenamed("avg(num_trips)", "avg_trips") \
                                 .orderBy("is_holiday")

# Convert the DataFrame to a Pandas DataFrame
df_holiday_trips_pd = df_holiday_trips.toPandas()

# Plot a bar chart comparing the average number of trips on holidays and non-holidays
plt.bar(df_holiday_trips_pd['is_holiday'], df_holiday_trips_pd['avg_trips'])
plt.xlabel('Holiday')
plt.ylabel('Average Number of Trips')
plt.title('Average Daily Bike Trips on Holidays vs. Non-Holidays')
plt.show()


# COMMAND ----------

df_holiday_trips_pd

# COMMAND ----------

from pyspark.sql.functions import date_format, col, when

# Define schema for weather data
from pyspark.sql.types import StructType, StructField, StringType
schema_weather = StructType([
    StructField("dt", StringType(), True),
    StructField("temp", StringType(), True),
    StructField("feels_like", StringType(), True),
    StructField("pressure", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("dew_point", StringType(), True),
    StructField("uvi", StringType(), True),
    StructField("clouds", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("wind_deg", StringType(), True),
    StructField("pop", StringType(), True),
    StructField("snow_1h", StringType(), True),
    StructField("id", StringType(), True),
    StructField("main", StringType(), True),
    StructField("description", StringType(), True),
    StructField("icon", StringType(), True),
    StructField("loc", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("lon", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_offset", StringType(), True),
    StructField("rain_1h", StringType(), True),
])

# Read in the weather data
df_weather = spark.read.format("csv").option("header", True).schema(schema_weather).load(NYC_WEATHER_FILE_PATH)

# Convert the 'dt' column from a string to a timestamp
# df_weather = df_weather.withColumn("timestamp", col("dt").cast("timestamp"))
df_weather = df_weather.withColumn("timestamp", to_timestamp(from_unixtime(df_weather["dt"])))

# Extract the date from the timestamp column
df_weather = df_weather.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))

# Group by date and calculate the average temperature, humidity, and wind speed for each date
df_daily_weather = df_weather.groupBy("date") \
                             .agg({"temp": "avg", "humidity": "avg", "wind_speed": "avg"}) \
                             .withColumnRenamed("avg(temp)", "avg_temp") \
                             .withColumnRenamed("avg(humidity)", "avg_humidity") \
                             .withColumnRenamed("avg(wind_speed)", "avg_wind_speed")

df_daily_weather = df_daily_weather.withColumnRenamed("date","weather_date")


# COMMAND ----------

# Convert timestamp column to timestamp and extract date
df_bike = bike_df.withColumn("timestamp", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss"))
df_bike = df_bike.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))


# COMMAND ----------

display(df_bike.orderBy("date"))

# COMMAND ----------

display(df_daily_weather.orderBy("weather_date"))

# COMMAND ----------


df_trips_weather = df_bike.join(df_daily_weather, df_bike.date == df_daily_weather.weather_date, "left_outer") \
                          .withColumnRenamed("timestamp", "trip_timestamp") \
                          .withColumnRenamed("date", "trip_date") \
                          .withColumnRenamed("avg_temp", "weather_avg_temp") \
                          .withColumnRenamed("avg_humidity", "weather_avg_humidity") \
                          .withColumnRenamed("avg_wind_speed", "weather_avg_wind_speed")


# COMMAND ----------

display(df_trips_weather.orderBy("trip_date"))

# COMMAND ----------

display(df_weather)

# COMMAND ----------

# Filter the weather data to include only the data from the year 2022
from pyspark.sql.functions import year
df_weather_2022 = df_weather.filter(year(col("date")) == 2022)

# Group by date and calculate the average temperature, humidity, and wind speed for each date
weather_2022  = df_weather_2022.agg({"temp": "avg", "humidity": "avg", "wind_speed": "avg"}) \
                                       .withColumnRenamed("avg(temp)", "avg_temp") \
                                       .withColumnRenamed("avg(humidity)", "avg_humidity") \
                                       .withColumnRenamed("avg(wind_speed)", "avg_wind_speed")


# COMMAND ----------

avg_temp = weather_2022.select("avg_temp").collect()[0][0]
avg_humidity = weather_2022.select("avg_humidity").collect()[0][0]
avg_wind_speed = weather_2022.select("avg_wind_speed").collect()[0][0]


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import max, col, ntile
from pyspark.sql.window import Window

# Define the WindowSpec object by partitioning and ordering the data
window_spec = Window.partitionBy().orderBy("avg_temp")

# Filter `df_daily_weather` to include only data from 2022
df_daily_weather_2022 = df_daily_weather.filter(year(col("weather_date")) == 2022)

# Divide the temperatures into three groups
df_weather_2022 = df_daily_weather_2022.withColumn("temp_group", ntile(3).over(window_spec))

# Get max temp for each temp_group
max_temps = df_weather_2022.groupBy("temp_group").agg(max(col("avg_temp")).alias("max_temp"))

# Print the max temperatures for each group
max_temps.show()

low_temp_2022 = max_temps.select("max_temp").collect()[0][0]
mod_temp_2022 = max_temps.select("max_temp").collect()[1][0]
high_temp_2022 = max_temps.select("max_temp").collect()[2][0]


# COMMAND ----------



# COMMAND ----------

df_daily_weather.show()

# COMMAND ----------

from pyspark.sql.functions import when

# Define the temperature groups

# Add the temperature group column
df_daily_weather = df_daily_weather.withColumn("temp_group", when(col("avg_temp") < low_temp_2022, "Cold")
                                                         .when((col("avg_temp") >= low_temp_2022) & (col("avg_temp") < mod_temp_2022), "Moderate")
                                                         .when(col("avg_temp") >= mod_temp_2022, "High")
                                                         .otherwise("null"))


# COMMAND ----------

display(df_daily_weather)

# COMMAND ----------


df_trips_weather = df_bike.join(df_daily_weather, df_bike.date == df_daily_weather.weather_date, "left_outer") \
                          .withColumnRenamed("timestamp", "trip_timestamp") \
                          .withColumnRenamed("date", "trip_date") \
                          .withColumnRenamed("avg_temp", "weather_avg_temp") \
                          .withColumnRenamed("avg_humidity", "weather_avg_humidity") \
                          .withColumnRenamed("avg_wind_speed", "weather_avg_wind_speed")


# COMMAND ----------

(df_trips_weather.orderBy("trip_date", ascending = False))

# COMMAND ----------

import pyspark.sql.functions as F
import matplotlib.pyplot as plt

# Group by temp_group and count the number of rows
df_grouped = df_trips_weather.groupBy('temp_group').count()

# Convert to Pandas DataFrame and plot a bar graph
df_grouped.toPandas().plot(kind='bar', x='temp_group', y='count', rot=0)
plt.xlabel('Temperature Group')
plt.ylabel('Count')
plt.title('Distribution of Temperatures across the data')
plt.show()


# COMMAND ----------

display(df_trips_weather.orderBy("trip_date"))

# COMMAND ----------

unique_values = df_trips_weather.select("temp_group").distinct().rdd.map(lambda row: row[0]).collect()


# COMMAND ----------

unique_values

# COMMAND ----------


