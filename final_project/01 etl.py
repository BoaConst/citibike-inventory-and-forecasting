# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 1,Parsing the parameters provided by the main notebook
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

# COMMAND ----------

# DBTITLE 1,Introductory information About Historic Data
print("Starting the ETL Process for New York Weather Data and Bike Trip Data.")

print("At a high-level, "
        f"there are {len(dbutils.fs.ls(NYC_WEATHER_FILE_PATH))} and "
            f"{len(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))} no. of files "
                "containing the raw data for New York Weather Data & "
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
        CREATE TABLE {delta_table_name}
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

def extractDateHourFromDataFrame(df: DataFrame, dateColName: str) -> DataFrame:
    df = df.withColumn("timestamp", to_timestamp(from_unixtime(df[dateColName]))) \
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

# DBTITLE 1,ETL for Historical Weather and Bike Trip Data
from pyspark.sql.functions import to_timestamp, to_date, from_unixtime, date_format, hour, month

# ETL for Historical Weather Data
weather_df = readDataFrameFromSource(NYC_WEATHER_FILE_PATH, "csv")

# COMMAND ----------

# Transform the dt field so as to have independent date and hour columns. We plan to partition the delta table by month to have a manageable directory size.
weather_df = extractDateHourFromDataFrame(weather_df, "dt")
display(weather_df)

# Write to a delta table partitioned by the month and z-ordered by date and hour
weather_delta_table_name = 'nyc_historical_weather_data'
writeDataFrameToDeltaTableOptimized(weather_df, weather_delta_table_name, "month", "date, hour")

# COMMAND ----------

import pyspark.sql.functions as F

# ETL for Historical Bike Trip Data
bike_df = readDataFrameFromSource(BIKE_TRIP_DATA_PATH, "csv")
historic_bike_trips_for_starting_station_df = bike_df.filter(F.col('start_station_name')== GROUP_STATION_ASSIGNMENT)
historic_bike_trips_for_ending_station_df = bike_df.filter(F.col('end_station_name')== GROUP_STATION_ASSIGNMENT)

# COMMAND ----------

# Transform the started_at for starting_df. This will ensure we have independent date and hour columns to z-order. We plan to partition the delta table by month to have a manageable directory size.
historic_bike_trips_for_starting_station_df = extractDateHourFromDataFrame(historic_bike_trips_for_starting_station_df, "started_at")
display(historic_bike_trips_for_starting_station_df)

nyc_historical_starting_bike_delta_table_name = 'nyc_historical_starting_bike_trip_data'
writeDataFrameToDeltaTableOptimized(historic_bike_trips_for_starting_station_df, nyc_historical_starting_bike_delta_table_name, "month", "date, hour")

# Transform the ended_at field ending_df. This will ensure we have independent date and hour columns to z-order. We plan to partition the delta table by month to have a manageable directory size.
historic_bike_trips_for_ending_station_df = extractDateHourFromDataFrame(historic_bike_trips_for_ending_station_df, "ended_at")
display(historic_bike_trips_for_ending_station_df)

nyc_historical_ending_bike_delta_table_name = 'nyc_historical_ending_bike_trip_data'
writeDataFrameToDeltaTableOptimized(historic_bike_trips_for_ending_station_df, nyc_historical_ending_bike_delta_table_name, "month", "date, hour")

# COMMAND ----------

# DBTITLE 1,ETL for Live Bronze Tables updated every 30 mins
# Load the Delta table into a DataFrame
bronze_station_info_df = readDeltaTable(BRONZE_STATION_INFO_PATH, True)
bronze_station_status_df = readDeltaTable(BRONZE_STATION_STATUS_PATH, True)
bronze_nyc_weather_df = readDeltaTable(BRONZE_NYC_WEATHER_PATH, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from g02_db

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
