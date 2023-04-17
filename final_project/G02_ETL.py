# Databricks notebook source
# MAGIC %run "/Repos/sroy22@ur.rochester.edu/G02-final-project/final_project/includes/includes"

# COMMAND ----------

delta_df = spark.read.format("delta").load("dbfs:/FileStore/tables/G02/historic_bike_trip/")

# COMMAND ----------

delta_df.printSchema()


# COMMAND ----------

import os
from pyspark.sql.functions import col

# Filter the rows based on the start_station_name
G02_df = delta_df.filter((col("start_station_name") == "West St & Chambers St"))


# Write the processed data to a DeltaTable
output_path = GROUP_DATA_PATH + "historical_bike_trips_G02"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

    
G02_df.write.format("delta").mode("append").option("path", output_path).saveAsTable("historical_bike_trips_G02")    
display(G02_df)
G02_df.count()

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G02/historic_bike_trip_g02/'))

# COMMAND ----------

from pyspark.sql.functions import count, date_add, last_day, col, current_date

delta_df = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_bike_trip_g02/')

# Define the start date and end date for the last year
end_date = current_date()
start_date = date_add(end_date, -12 * 30)
# start_date = date_add(last_day(date_add(end_date, "-12 months")), 1)

# Iterate over each month in the last year
for i in range(12):
    # Generate the start and end dates for the current month
    current_start_date = date_add(start_date, i * 30)
    current_end_date = date_add(current_start_date, 30) if i < 11 else end_date
    
    # Make a date range query for the current month
    month_query = delta_df.filter((col("started_at") >= current_start_date) & (col("ended_at") < current_end_date))
    
    # Do something with the query result, e.g. show the count
    month_query_count = month_query.count()
    print(f"Month {i+1}: {month_query_count} rows")
    
    grouped_data = month_query.groupBy("rideable_type").agg(count("ride_id"))
    display(grouped_data)

# COMMAND ----------

# df = spark.read.format("csv").option("header", "true").load(BIKE_TRIP_DATA_PATH)

# output_path = GROUP_DATA_PATH + "historical_bike_trips"

# if not os.path.isdir(output_path):
#     dbutils.fs.mkdirs(output_path)

# history_bike_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .save(output_path)

# history_bike_df.write.format("delta").mode("overwrite").saveAsTable("history_bike_trips")
