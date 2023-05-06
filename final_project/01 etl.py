# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 1,Parsing the parameters provided by the main notebook
# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# print(start_date,end_date,hours_to_forecast, promote_model)

# COMMAND ----------

# DBTITLE 1,Introductory information About Historic Data
print("Starting the ETL Process for New York Weather Data and Bike Trip Data.")

print("At a high-level, "
        f"there are {len(dbutils.fs.ls(NYC_WEATHER_FILE_PATH))} and "
            f"{len(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))} raw historic data files "
                "for New York Weather Data & "
                    "Bike Trip Data respectively.")

# COMMAND ----------

# DBTITLE 1,Helper functions required as part of ETL
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

def readDataFromSourceWithInferredSchema(path: str, file_format: str, df: DataFrame) -> DataFrame:
    schema = df.schema

    # Replace the DataFrame with the inferred schema
    df = spark.read.format(file_format) \
        .option("header", "true") \
            .option("path", path) \
                .schema(schema) \
                    .load()
    
    display(df)
    return df

def readDataFrameFromSource(path: str, file_format: str) -> DataFrame:
    """
    Creates a Spark DataFrame from a raw data file.

    :param path: The location of the raw data file.
    :param format: The format of the raw data file.
    :return: A Spark DataFrame
    """
    # Create a static DataFrame to infer the schema from existing CSV files
    df = spark.read.format(file_format) \
        .option("header", "true") \
            .option("inferSchema", "true") \
                .load(path)

    return readDataFromSourceWithInferredSchema(path, file_format, df)

def writeDataFrameToDeltaTable(df: DataFrame, delta_table_name: str):
    delta_table_path = GROUP_DATA_PATH + delta_table_name
    df.write.format("delta") \
        .mode("overwrite") \
            .option("overwriteSchema", "true") \
                .option("path", delta_table_path) \
                    .saveAsTable(delta_table_name)

    # Check if the Delta Tables were correctly created or not
    try:
        if len(dbutils.fs.ls(delta_table_path)) >= 1: 
            print("Directory created : ", delta_table_path)    
            display(dbutils.fs.ls(delta_table_path))  
    except FileNotFoundError as e:
        print("Oops! Directory not found:", e)

def writeDataFrameToDeltaTableOptimized(df: DataFrame, delta_table_name: str, partitionByColumnName: str, zOrderColumnNames: str):
    """
    Writes a Spark DataFrame into a delta table

    :param df: Spark DataFrame
    :param delta_table_name: The name of the delta table
    """
    temp_table_name = "temp_table"
    temp_table_path = GROUP_DATA_PATH + temp_table_name
    
    # Write the dataframe to temp table so that the schema can be inferred while Zordering and Partitioning
    df.write.format("delta") \
        .mode("overwrite") \
            .option("overwriteSchema", "true") \
                .option("path", temp_table_path) \
                    .saveAsTable(temp_table_name)

    # Populate the Delta Table from the delta table path
    delta_table_path = GROUP_DATA_PATH + delta_table_name
    checkpoints_dir = GROUP_DATA_PATH + "_checkpoints/"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {delta_table_name}
        USING delta
        LOCATION '{delta_table_path}'
        PARTITIONED BY ({partitionByColumnName})
        OPTIONS ('checkpointLocation' = '{checkpoints_dir}')
        AS SELECT * FROM {temp_table_name}
    """)

    spark.sql(f"""
        OPTIMIZE {delta_table_name}
        ZORDER BY {zOrderColumnNames}
    """)


    # Delete the temp table
    spark.sql(f"DROP TABLE {temp_table_name}")

    # Check if the Delta Tables were correctly created or not
    try:
        if len(dbutils.fs.ls(delta_table_path)) >= 1:
            print("Delta tables were successfully created!") 
            print("Directory created : ", delta_table_path)    
            display(dbutils.fs.ls(delta_table_path))  
    except FileNotFoundError as e:
        print("Oops! Directory not found:", e)

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

def registerDeltaTablesAsTemporaryView(delta_table_path: str, temporary_view_name: str):
    """
    Loads a Delta Table as a temporary view 

    :param delta_table_path: The path of the delta table
    :param temporary_view_name: The name of the view
    """
    spark.read.format("delta") \
        .load(delta_table_path) \
            .createOrReplaceTempView(temporary_view_name)

def extractDateHourFromDataFrame(df: DataFrame, dateColName: str, isUnixTime: bool) -> DataFrame:
    if(isUnixTime):
        df = df.withColumn("timestamp", to_timestamp(from_unixtime(df[dateColName])))
    else:
        df = df.withColumn("timestamp", to_timestamp(df[dateColName]))
    df = df \
            .withColumn("full_date", date_format("timestamp", "yyyy-MM-dd HH:mm:ss")) \
                .withColumn("date", to_date("full_date")) \
                    .withColumn("hour", hour("full_date")) \
                        .withColumn("month", month("full_date")) \
                            .drop(dateColName) \
                                .drop("timestamp") \
                                    .drop("full_date")
            
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Bronze Data ETL Pipeline <h1>

# COMMAND ----------

# DBTITLE 1,Bronze Tables for Historical Weather and Bike Trip Data
from pyspark.sql.functions import to_timestamp, to_date, from_unixtime, date_format, hour, month

# Read Historical Weather Data
weather_df = readDataFrameFromSource(NYC_WEATHER_FILE_PATH, "csv")

# COMMAND ----------

# Print the total number of row in the raw data file
print("Historic Weather data files read-in was successful! "
        f"There are a total of {(weather_df.count())} lines ")

# COMMAND ----------

# Write raw historic weather data to Bronze table
weather_delta_table_name = 'Bronze_nyc_historical_weather_data'
writeDataFrameToDeltaTable(weather_df, weather_delta_table_name) 

# COMMAND ----------

# Read Historical Bike Trip Data
bike_df = readDataFrameFromSource(BIKE_TRIP_DATA_PATH, "csv")

# COMMAND ----------

# Print the total number of row in the raw data file
print("Historic Bike Trip data files read-in was successful! "
        f"There are a total of {(bike_df.count())} lines ")

# COMMAND ----------

# Write raw bike trips data to Bronze table
nyc_historical_bike_delta_table_name = 'Bronze_nyc_historical_bike_trip_data'
writeDataFrameToDeltaTable(bike_df, nyc_historical_bike_delta_table_name) 

# COMMAND ----------

# DBTITLE 1,Bronze Tables for Live Delta Tables updated every 30 mins
# Load the Delta table into a DataFrame
bronze_station_info_df = readDeltaTable(BRONZE_STATION_INFO_PATH, False)
bronze_station_status_df = readDeltaTable(BRONZE_STATION_STATUS_PATH, False)
bronze_nyc_weather_df = readDeltaTable(BRONZE_NYC_WEATHER_PATH, False)

# COMMAND ----------

# Print the total number of row in the raw data file
print("Bronze Station info delta files read-in was successful! "
        f"There are a total of {(bronze_station_info_df.count())} lines ")

print("Bronze Station Status delta files read-in was successful! "
        f"There are a total of {(bronze_station_status_df.count())} lines ")

print("Bronze NYC Weather delta files read-in was successful! "
        f"There are a total of {(bronze_nyc_weather_df.count())} lines ")

# COMMAND ----------

# # Write raw data files to Bronze Tables
# station_info_delta_table_name = 'Bronze_station_info_data'
# writeDataFrameToDeltaTable(bronze_station_info_df, station_info_delta_table_name)

# station_status_delta_table_name = 'Bronze_station_status_data'
# writeDataFrameToDeltaTable(bronze_station_status_df, station_status_delta_table_name)

# nyc_weather_delta_table_name = 'Bronze_live_nyc_weather_data'
# writeDataFrameToDeltaTable(bronze_nyc_weather_df, nyc_weather_delta_table_name)

# COMMAND ----------

# Check if all the bronze tables are present
display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Silver Data ETL Pipeline <h1>

# COMMAND ----------

# DBTITLE 1,Transformations on Historic Weather Data
# Transform the dt field so as to have independent date and hour columns. We plan to partition the delta table by month to have a manageable directory size.
weather_df = extractDateHourFromDataFrame(weather_df, "dt", True)
display(weather_df)

# Write to a delta table partitioned by the month and z-ordered by date and hour
weather_delta_table_name = 'Silver_nyc_historical_weather_data'
writeDataFrameToDeltaTableOptimized(weather_df, weather_delta_table_name, "month", "date, hour")

# COMMAND ----------

# DBTITLE 1,Transformations and filtering on Historic Bike Trips Data
import pyspark.sql.functions as F

# Creating G02 station start and end datafranes
historic_bike_trips_for_starting_station_df = bike_df.filter(F.col('start_station_name')== GROUP_STATION_ASSIGNMENT)
historic_bike_trips_for_ending_station_df = bike_df.filter(F.col('end_station_name')== GROUP_STATION_ASSIGNMENT)

# COMMAND ----------

# Transform the started_at for starting_df. This will ensure we have independent date and hour columns to z-order. We plan to partition the delta table by month to have a manageable directory size.
historic_bike_trips_for_starting_station_df = extractDateHourFromDataFrame(historic_bike_trips_for_starting_station_df, "started_at", False)
display(historic_bike_trips_for_starting_station_df)

nyc_historical_starting_bike_delta_table_name = 'Silver_nyc_historical_G02_starting_bike_trip_data'
writeDataFrameToDeltaTableOptimized(historic_bike_trips_for_starting_station_df, nyc_historical_starting_bike_delta_table_name, "month", "date, hour")

# Transform the ended_at field ending_df. This will ensure we have independent date and hour columns to z-order. We plan to partition the delta table by month to have a manageable directory size.
historic_bike_trips_for_ending_station_df = extractDateHourFromDataFrame(historic_bike_trips_for_ending_station_df, "ended_at", False)
display(historic_bike_trips_for_ending_station_df)

nyc_historical_ending_bike_delta_table_name = 'Silver_nyc_historical_G02_ending_bike_trip_data'
writeDataFrameToDeltaTableOptimized(historic_bike_trips_for_ending_station_df, nyc_historical_ending_bike_delta_table_name, "month", "date, hour")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from g02_db

# COMMAND ----------

# DBTITLE 1,Transformations and filtering of Live Station info and status Data
from pyspark.sql.functions import col

# Filter the live tables for the assigned station
bronze_station_info_df = bronze_station_info_df.filter(col("short_name") == '5329.03')
bronze_station_status_df = bronze_station_status_df.filter(col("station_id") == '66dc0e99-0aca-11e7-82f6-3863bb44ef7c')
bronze_station_status_df = extractDateHourFromDataFrame(bronze_station_status_df, "last_reported", False)

# COMMAND ----------

from pyspark.sql.functions import lag
from pyspark.sql.window import Window

bronze_station_status_df = bronze_station_status_df.withColumn("net_change", lag(col("num_bikes_available") - col("num_bikes_available")).over(Window.orderBy("date")))
display(bronze_station_status_df)

# COMMAND ----------

# Write raw data files to Silver Tables
station_info_delta_table_name = 'Silver_G02_station_info_data'
writeDataFrameToDeltaTable(bronze_station_info_df, station_info_delta_table_name)

station_status_delta_table_name = 'Silver_G02_station_status_data'
writeDataFrameToDeltaTableOptimized(bronze_station_status_df, station_status_delta_table_name, "month", "date, hour")

# COMMAND ----------

# DBTITLE 1,Combining Historic Weather and Bike Trips Data for EDA


# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

import json

# Return Success Code
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
