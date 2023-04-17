# Databricks notebook source
# MAGIC %run "/Repos/sroy22@ur.rochester.edu/G02-final-project/final_project/includes/includes"

# COMMAND ----------

# Imports
from pyspark.sql.functions import year, month, sum, count, col, current_date, date_format, date_add
import matplotlib.pyplot as plt
from datetime import datetime

# COMMAND ----------

# Loading weather trip data from the delta tables
weather_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historical_weather_data_g02/')
bike_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_bike_trip_g02/')

# COMMAND ----------

display(weather_df)

# COMMAND ----------

display(bike_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, from_unixtime


# Convert the date_long column to a date format
weather_df_with_date = weather_df.withColumn("date", to_timestamp(from_unixtime(weather_df["dt"])))

# Show the result
weather_df_with_date.show()

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
    month_query = delta_df.filter((col("started_at") >= current_start_date) & (col("ended_at") < current_end_date))   
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
