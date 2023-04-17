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

NYC_WEATHER_FILE_PATH

# COMMAND ----------

# Check for files in weather data path
import os
dbutils.fs.ls("dbfs:/FileStore/tables/raw/weather/")


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
schema = StructType([
    StructField("dt", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])

schema_weather = StructType([
    StructField("dt", StringType(), True),
    StructField("temp", StringType(), True),
    StructField("feels_like", StringType(), True),
    StructField("pressure", StringType(), True),
])

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
