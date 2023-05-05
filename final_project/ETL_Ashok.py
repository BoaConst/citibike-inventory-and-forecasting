# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

GROUP_DATA_PATH

# COMMAND ----------

# Check for files in weather data path
import os
dbutils.fs.ls(NYC_WEATHER_FILE_PATH)


# COMMAND ----------

BIKE_TRIP_DATA_PATH

# COMMAND ----------

# Check for files in bike trip data path
files = dbutils.fs.ls("dbfs:/FileStore/tables/raw/bike_trips/")
num_files = len(files)
print(f"Total number of bike trip files: {num_files}")

# COMMAND ----------

# Load the data file into a DataFrame
df = spark.read.format("csv").option("header", True).load(NYC_WEATHER_FILE_PATH)

# Print the schema of the DataFrame
df.printSchema()

# COMMAND ----------

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

# Read in the history weather data
df_weather_history = spark.read.format("csv").option("header", True).schema(schema_weather).load(NYC_WEATHER_FILE_PATH)

display(df_weather_history)


# COMMAND ----------

# Write the dataframe to bronze delta table
delta_table_name = 'historic_nyc_weather_ashok'
df_weather_history.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

# Check if the delta table is available at the Group data path
display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

# Check the schema for bike trip data
df = spark.read.format("csv").option("header", True).load(BIKE_TRIP_DATA_PATH)

# Print the schema of the DataFrame
df.printSchema()

# COMMAND ----------

# Define the schema for brik trip history data
schema_bike_trip = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", StringType(), True),
    StructField("start_lng", StringType(), True),
    StructField("end_lat", StringType(), True),
    StructField("end_lng", StringType(), True),
    StructField("member_casual", StringType(), True)
])

df_bike_trip_history = spark.read.format("csv").option("header", True).schema(schema_bike_trip).load(BIKE_TRIP_DATA_PATH)

df_bike_trip_history.count()

# COMMAND ----------

# Write the dataframe to bronze delta table
delta_table_name = 'historic_bike_trip_ashok'
df_bike_trip_history.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

# Check if the delta table is available at the Group data path
display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

# Filter the delta table for G02 station

delta_path = "dbfs:/FileStore/tables/G02/historic_bike_trip_ashok/"

# Register Delta table as temporary view
spark.read.format("delta").load(delta_path).createOrReplaceTempView("bike_trip_history_delta")

# Filter data using SQL query
starting_df_g02 = spark.sql("""
  SELECT * 
  FROM bike_trip_history_delta 
  WHERE start_station_name = {}
""".format("'{}'".format(GROUP_STATION_ASSIGNMENT)))

# Display filtered data
display(starting_df_g02)  

# Display count of dataframe
starting_df_g02.count()



# Filter data using SQL query
ending_df_g02 = spark.sql("""
  SELECT * 
  FROM bike_trip_history_delta 
  WHERE end_station_name = {}
""".format("'{}'".format(GROUP_STATION_ASSIGNMENT)))

# Display filtered data
display(ending_df_g02)  

# Display count of dataframe
ending_df_g02.count()

# COMMAND ----------

# Write the dataframe to bronze delta table
delta_table_name = 'historic_bike_trip_starting_g02_ashok'
starting_df_g02.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# Write the dataframe to bronze delta table
delta_table_name = 'historic_bike_trip_ending_g02_ashok'
ending_df_g02.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

from pyspark.sql.functions import col

# Read from delta tables using readStream and apply transformations
station_info_df = (
  spark.readStream.format("delta")
    .load(BRONZE_STATION_INFO_PATH)
    .select(col("*"))
)

station_status_df = (
  spark.readStream.format("delta")
    .load(BRONZE_STATION_STATUS_PATH)
    .select(col("*"))
)

nyc_weather_df = (
  spark.readStream.format("delta")
    .load(BRONZE_NYC_WEATHER_PATH)
    .select(col("*"))
)

# COMMAND ----------

bronze_station_info_stream = (
  station_info_df.writeStream
    .format("delta")
    .option("checkpointLocation", GROUP_DATA_PATH + "/checkpoint/bronze_station_info")
    .outputMode("append")
    .queryName("stream_bronze_station_info")
    .start(GROUP_DATA_PATH	 + "/bronze_station_info")
)

bronze_station_status_stream = (
  station_status_df.writeStream
    .format("delta")
    .option("checkpointLocation", GROUP_DATA_PATH + "/checkpoint/bronze_station_status")
    .outputMode("append")
    .queryName("stream_bronze_station_status")
    .start(GROUP_DATA_PATH	 + "/bronze_station_status")
)

bronze_nyc_weather_stream = (
  nyc_weather_df.writeStream
    .format("delta")
    .option("checkpointLocation", GROUP_DATA_PATH + "/checkpoint/bronze_nyc_weather")
    .outputMode("append")
    .queryName("stream_bronze_nyc_weather")
    .start(GROUP_DATA_PATH	 + "/bronze_nyc_weather")
)

# COMMAND ----------

station_info_df = (
  spark.readStream.format("delta")
    .load(BRONZE_STATION_INFO_PATH)
    .select(col("*"))
)

bronze_station_info_stream = (
  station_info_df.writeStream
    .format("delta")
    .option("checkpointLocation", GROUP_DATA_PATH + "/checkpoint/bronze_station_info")
    .outputMode("append")
    .queryName("stream_bronze_station_info")
    .start(GROUP_DATA_PATH	 + "/bronze_station_info")
)

# COMMAND ----------



# COMMAND ----------

# Check if the delta table is available at the Group data path
display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

DELTA_TABLE_PATH = GROUP_DATA_PATH + "/bronze_nyc_weather"

# Read the Delta table into a DataFrame
df = spark.read.format("delta").load(DELTA_TABLE_PATH)

# Display the contents of the DataFrame
display(df)


# COMMAND ----------


