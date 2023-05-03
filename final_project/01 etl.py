# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

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

# Check if the data stream is still streaming
weather_df_stream.isStreaming

# COMMAND ----------

# Write the dataframe to bronze delta table
delta_table_name = 'nyc_historical_weather_data'
weather_df_stream.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G02/nyc_historical_weather_data/'))

# COMMAND ----------

# Create a static DataFrame to infer the schema from existing CSV files
bike_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(BIKE_TRIP_DATA_PATH)

bike_schema = bike_df.schema

# Create a streaming DataFrame to read data from the CSV files
bike_df_stream = spark.readStream.format("csv") \
    .option("header", "true") \
    .option("path", BIKE_TRIP_DATA_PATH) \
    .schema(bike_schema) \
    .load()

bike_df_stream.printSchema()

# COMMAND ----------

# Write the dataframe to bronze delta table
delta_table_name = 'nyc_historical_bike_trip_data'
df_bike_trip_history.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

# Check if the delta table is available at the Group data path
display(dbutils.fs.ls('dbfs:/FileStore/tables/G02/nyc_historical_bike_trip_data/'))

# COMMAND ----------

# Filter the delta table for G02 station
delta_path = "dbfs:/FileStore/tables/G02/nyc_historical_bike_trip_data/"

# Register Delta table as temporary view
spark.read.format("delta").load(delta_path).createOrReplaceTempView("nyc_historical_bike_trip_delta")

# Filter data using SQL query
filtered_df_g02 = spark.sql("""
  SELECT * 
  FROM nyc_historical_bike_trip_delta 
  WHERE start_station_name = 'West St & Chambers St'
""")

# Display filtered data
display(filtered_df_g02)  

# Display count of dataframe
filtered_df_g02.count()

# COMMAND ----------

# # Write the dataframe to bronze delta table
# delta_table_name = 'historic_bike_trip_g02'
# filtered_df_g02.write.format("delta").mode("append").option("path", GROUP_DATA_PATH + delta_table_name).saveAsTable(delta_table_name)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
