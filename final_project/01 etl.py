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

print("At the high-level, there are {} and {} no. of files containing the raw data for New York Weather Data & Bike Trip Data respectively.".format(len(dbutils.fs.ls(NYC_WEATHER_FILE_PATH)), len(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))))

# COMMAND ----------

print("Starting the Streaming ETL Process for New York Weather Data and Bike Trip Data.")

# COMMAND ----------

# Create a static DataFrame to infer the schema from existing CSV files
weather_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(NYC_WEATHER_FILE_PATH)

schema = weather_df.schema

# Create a streaming DataFrame to read data from the CSV files
weather_df_stream = spark.readStream.format("csv") \
    .option("header", "true") \
    .option("path", NYC_WEATHER_FILE_PATH) \
    .schema(schema) \
    .load()

weather_df_stream.printSchema()

# COMMAND ----------

weather_df_stream.isStreaming

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
delta_table_name = 'historic_nyc_weather'
df_weather_history.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

# Check if the delta table is available at the Group data path
display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G02/historic_nyc_weather/'))

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
delta_table_name = 'historic_bike_trip'
df_bike_trip_history.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G02/historic_bike_trip/'))

# COMMAND ----------

# Filter the delta table for G02 station

delta_path = "dbfs:/FileStore/tables/G02/historic_bike_trip/"

# Register Delta table as temporary view
spark.read.format("delta").load(delta_path).createOrReplaceTempView("bike_trip_history_delta")

# Filter data using SQL query
filtered_df_g02 = spark.sql("""
  SELECT * 
  FROM bike_trip_history_delta 
  WHERE start_station_name = 'West St & Chambers St'
""")

# Display filtered data
display(filtered_df_g02)  

# Display count of dataframe
filtered_df_g02.count()

# COMMAND ----------

# Write the dataframe to bronze delta table
delta_table_name = 'historic_bike_trip_g02'
filtered_df_g02.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G02/historic_bike_trip_g02/'))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
