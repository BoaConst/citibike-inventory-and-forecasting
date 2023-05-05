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
            f"{len(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))} no. of files "
                "containing the raw data for New York Weather Data & "
                    "Bike Trip Data respectively.")

# COMMAND ----------

# DBTITLE 1,Helper functions required as part of ETL
from pyspark.sql import DataFrame

def readDataFromSourceWithInferredSchema(path: str, file_format: str, df: DataFrame) -> DataFrame:
    schema = df.schema

    # Replace the DataFrame with the inferred schema
    df = spark.read.format(file_format) \
        .option("header", "true") \
            .option("path", path) \
                .schema(schema) \
                    .load()
    
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
    """
    Writes a Spark DataFrame into a delta table

    :param df: Spark DataFrame
    :param delta_table_name: The name of the delta table
    """
    delta_table_path = GROUP_DATA_PATH + delta_table_name
    # Write the dataframe to delta table
    df.write.format("delta") \
        .mode("append") \
            .option("path", delta_table_path) \
                .option("checkpointLocation", GROUP_DATA_PATH + "_checkpoints/") \
                    .saveAsTable(delta_table_name)
    # Check if the directory is correctly created or not
    try:
        if len(dbutils.fs.ls(delta_table_path)) >= 1: 
            print("Directory created : ", delta_table_path)    
            display(dbutils.fs.ls(delta_table_path))  
    except FileNotFoundError as e:
        print("Oops! Directory not found:", e)


def readDeltaTable(delta_table_path: str) -> DataFrame:
    df = spark.read.format("delta") \
            .load(delta_table_path)
    return df

def registerDeltaTablesAsGlobalTemporaryView(delta_table_path: str, temporary_view_name: str):
    """
    Loads a Delta Table as a temporary view 

    :param delta_table_path: The path of the delta table
    :param temporary_view_name: The name of the view
    """
    spark.read.format("delta") \
        .load(delta_table_path) \
            .createOrReplaceGlobalTempView(temporary_view_name)

# COMMAND ----------

# DBTITLE 1,Cells [5-8]: Bronze Tables Processing
# ETL for Historical Weather Data
weather_df = readDataFrameFromSource(NYC_WEATHER_FILE_PATH, "csv")
weather_df.printSchema()

weather_delta_table_name = 'nyc_historical_weather_data'
writeDataFrameToDeltaTable(weather_df, weather_delta_table_name)

# COMMAND ----------

# ETL for Historical Bike Trip Data
bike_df = readDataFrameFromSource(BIKE_TRIP_DATA_PATH, "csv")
bike_df.printSchema()

bike_delta_table_name = 'nyc_historical_bike_trip_data'
writeDataFrameToDeltaTable(bike_df, bike_delta_table_name)

# COMMAND ----------

# DBTITLE 1,ETL for Live Bronze Tables updated every 30 mins
# Load the Delta table into a DataFrame
bronze_station_info_df = readDeltaTable(BRONZE_STATION_INFO_PATH) 
bronze_station_info_df.printSchema()

bronze_station_status_df = readDeltaTable(BRONZE_STATION_STATUS_PATH)
bronze_station_status_df.printSchema()

bronze_nyc_weather_df = readDeltaTable(BRONZE_NYC_WEATHER_PATH)
bronze_nyc_weather_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from g02_db

# COMMAND ----------

# DBTITLE 1,Silver Tables Processing

# Register Delta tables For Historic Data as Temporary Views
registerDeltaTablesAsTemporaryView(GROUP_DATA_PATH + weather_delta_table_name, 'historic_weather_trip_data_view')
registerDeltaTablesAsTemporaryView(GROUP_DATA_PATH + bike_delta_table_name, 'historic_bike_trip_data_view')


# Register CitiBike Bronze Data Tables as Temporary Views
registerDeltaTablesAsTemporaryView(BRONZE_STATION_INFO_PATH, 'bronze_station_info_view')
registerDeltaTablesAsTemporaryView(BRONZE_STATION_STATUS_PATH, 'bronze_station_status_view')
registerDeltaTablesAsTemporaryView(BRONZE_NYC_WEATHER_PATH, 'bronze_nyc_weather_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from g02_db

# COMMAND ----------

# DBTITLE 1,Remaining Cells : Gold Tables Processing
# Filter data using SQL query
filtered_df_g02 = spark.sql("""
  SELECT * 
  FROM historic_bike_trip_data_view 
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
