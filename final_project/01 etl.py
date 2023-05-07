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
            f"{len(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))} raw historic data files "
                "for New York Weather Data & "
                    "Bike Trip Data respectively.")

# COMMAND ----------

# DBTITLE 1,Helper functions required as part of ETL
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_timestamp, to_date, from_unixtime, date_format, hour, month

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
                                .drop("timestamp") \
                                    .drop("full_date")
            
    return df

def extractDateHourFromDataFrame1(df: DataFrame, dateColName: str) -> DataFrame:
    df = df.withColumn("full_date", date_format(dateColName, "yyyy-MM-dd HH:mm:ss")) \
                .withColumn("date", to_date("full_date")) \
                    .withColumn("hour", hour("full_date")) \
                        .withColumn("month", month("full_date")) \
                                .drop("timestamp") \
                                    .drop("full_date")
            
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Bronze Data ETL Pipeline <h1>

# COMMAND ----------

# DBTITLE 1,Bronze Tables for Historical Weather and Bike Trip Data
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

# DBTITLE 1,Combining Historic Weather and Bike Trips Data for EDA
# Drop duplicates from historic files
historic_bike_trips_for_starting_station_df = historic_bike_trips_for_starting_station_df.dropDuplicates(["ride_id"])
historic_bike_trips_for_ending_station_df = historic_bike_trips_for_ending_station_df.dropDuplicates(["ride_id"])
weather_df = weather_df.dropDuplicates(["date", "hour"])

# COMMAND ----------

# Aggregate the starting and ending historic bike trip 
starting_rides_per_hour = historic_bike_trips_for_starting_station_df.groupBy("date", "hour").count()
ending_rides_per_hour = historic_bike_trips_for_ending_station_df.groupBy("date", "hour").count()

starting_rides_per_hour = starting_rides_per_hour.withColumnRenamed("count", "start_ride_count")
ending_rides_per_hour = ending_rides_per_hour.withColumnRenamed("count", "end_ride_count")

# COMMAND ----------

display(starting_rides_per_hour.orderBy("date", "hour"))

# COMMAND ----------

from pyspark.sql.functions import hour, expr

# Define the range of dates
from pyspark.sql.functions import min,max

start_date = starting_rides_per_hour.agg(min("date")).collect()[0][0]
end_date = ending_rides_per_hour.agg(max("date")).collect()[0][0]

display(start_date)
display(end_date)

# Create a DataFrame with all the possible date-hour combinations
dates_df = spark.range(0, (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1) \
               .withColumn("date", expr("date_add('{}', CAST(id AS int))".format(start_date)))
hours_df = spark.range(0, 24).withColumn("hour_of_day", hour(expr("timestamp('2000-01-01 ' || id || ':00:00')")))
all_hours_df = dates_df.crossJoin(hours_df) \
                       .withColumn("date", expr("date_format(date, 'yyyy-MM-dd')"))

# Display the result
all_hours_df = all_hours_df.drop("id")
all_hours_df = all_hours_df.withColumnRenamed("hour_of_day", "hour")

display(all_hours_df)

# COMMAND ----------

display(all_hours_df.orderBy("date", "hour"))

# COMMAND ----------

# Join weather and bike trips aggregated data with all_hours_df
Data_modelling_df = all_hours_df.join(starting_rides_per_hour, ["date", "hour"], "left_outer") \
                        .fillna(0, subset=["start_ride_count"]) \
                        .orderBy("date", "hour")

Data_modelling_df = Data_modelling_df.join(ending_rides_per_hour, ["date", "hour"], "left_outer") \
                        .fillna(0, subset=["end_ride_count"]) \
                        .orderBy("date", "hour")

Data_modelling_df = Data_modelling_df.withColumn("net_change", Data_modelling_df["end_ride_count"] - Data_modelling_df["start_ride_count"])

display(Data_modelling_df.orderBy("date", "hour"))


# COMMAND ----------

from pyspark.sql.functions import col, avg, mode
from pyspark.sql.functions import date_format, dayofweek,when

# Select columns with integer and string data types
int_cols = ["temp","feels_like","pressure","humidity","dew_point","uvi","clouds","visibility","wind_speed","wind_deg","pop","snow_1h","rain_1h"]
str_cols = ["main","description"]

# Group by date and hour and compute average and mode for integer and string columns respectively
grouped_weather_df = weather_df.groupBy("date", "hour").agg(
    *[avg(col).alias(col) for col in int_cols],
    *[mode(col).alias(col) for col in str_cols]
)

# Show the resulting dataframe
grouped_weather_df = grouped_weather_df.withColumn("day_of_week", dayofweek("date")) \
                                       .withColumn("is_weekend", when(dayofweek("date").isin([7,1]), 1).otherwise(0))

display(grouped_weather_df.orderBy("date","hour"))

# COMMAND ----------

from pyspark.sql.functions import col

# Joining with grouped weather data for final dataframe for modelling
Data_modelling_df = grouped_weather_df.join(Data_modelling_df, ["date", "hour"], "left_outer")
display(Data_modelling_df.orderBy("date","hour"))
Data_modelling_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, desc

# Loop through all columns in the dataframe and filter out rows with null values
Data_modelling_df = Data_modelling_df.dropna(subset=['net_change'])
for col_name in Data_modelling_df.columns:
    null_count = Data_modelling_df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print("Column '{}' has {} null values".format(col_name, null_count))
    else:
        print("Column '{}' has no null values".format(col_name))

# Replace the null with its mode value
mode = Data_modelling_df.groupBy("visibility").agg(count("*").alias("count")).orderBy(desc("count")).first()[0]

Data_modelling_df = Data_modelling_df.fillna(mode, subset=["visibility"])

# COMMAND ----------

# Making the hour column consistent length
from pyspark.sql.functions import concat, lit, to_timestamp,col,lpad

Data_modelling_df = Data_modelling_df.withColumn(
    "hour", lpad(col("hour").cast("string"), 2, "0")
)

Data_modelling_df = Data_modelling_df.withColumn('date_hour', concat('date', lit(' '), 'hour', lit(':00')))
Data_modelling_df = Data_modelling_df.withColumn('timestamp', to_timestamp('date_hour', 'yyyy-MM-dd HH:mm'))
Data_modelling_df = Data_modelling_df.drop("date_hour")

# From our model we got to know that snow_1h, main, description, start_ride_count and end_ride_count are not really relevant from a modelling perspective. These fields don't appear in the gold table for live data. Thus we drop it!
columns_to_drop = ["snow_1h", "main", "description"]

# Transforming Weather Live Data
Data_modelling_df = Data_modelling_df \
                            .drop(*columns_to_drop) \

display(Data_modelling_df)

Data_modelling_df.printSchema()

# COMMAND ----------

display(Data_modelling_df.orderBy("date","hour"))

# COMMAND ----------

# Write final dataset for modelling to Delta table
data_for_modelling_table_name = 'Silver_G02_modelling_data'
writeDataFrameToDeltaTable(Data_modelling_df, data_for_modelling_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Gold Data ETL Pipeline <h1>

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

# DBTITLE 1,Transformations and filtering of Live Station info and status Data
from pyspark.sql.functions import col, lead, asc
from pyspark.sql.window import Window

# Filter the live tables for the assigned station
bronze_station_info_df = bronze_station_info_df.filter(col("short_name") == '5329.03')
bronze_station_status_df = bronze_station_status_df.filter(col("station_id") == '66dc0e99-0aca-11e7-82f6-3863bb44ef7c')
bronze_station_status_df = extractDateHourFromDataFrame(bronze_station_status_df, "last_reported", False)
bronze_nyc_weather_df = extractDateHourFromDataFrame(bronze_nyc_weather_df, "time", False)

# COMMAND ----------

# Transforming station status table
# Sort the DataFrame by the date column in descending order
bronze_station_status_df = bronze_station_status_df.orderBy(asc('last_reported'))

# Use drop_duplicates to keep only the first occurrence of each unique hour
bronze_station_status_df = bronze_station_status_df.dropDuplicates(['date', 'hour'])

bronze_station_status_df = bronze_station_status_df.withColumn("net_change", lead(col("num_bikes_available")).over(Window.orderBy("date")) - col("num_bikes_available"))
bronze_station_status_df = bronze_station_status_df.fillna(0, subset=["net_change"])

display(bronze_station_status_df)
# Write the refined dataframe to a gold table
silver_station_status_delta_table_name = 'Silver_G02_station_status_data'
writeDataFrameToDeltaTableOptimized(bronze_station_status_df, silver_station_status_delta_table_name, "month", "date, hour")

bronze_station_status_df = bronze_station_status_df.select("date", "hour", "net_change")

columns_to_drop = ["dt", "weather"]

# Transforming Weather Live Data
bronze_nyc_weather_df = bronze_nyc_weather_df \
                            .drop(*columns_to_drop) \
                                .withColumnRenamed("rain.1h", "rain_1h") \
                                    

# COMMAND ----------

from pyspark.sql.functions import col, avg, mode
from pyspark.sql.functions import date_format, dayofweek,when

# Select columns with integer and string data types
int_cols = ["temp","feels_like","pressure","humidity","dew_point","uvi","clouds","visibility","wind_speed","wind_deg","pop","rain_1h"]

# Group by date and hour and compute average and mode for integer and string columns respectively
grouped_bronze_nyc_weather_df = bronze_nyc_weather_df.groupBy("date", "hour").agg(*[avg(col).alias(col) for col in int_cols])

# Show the resulting dataframe
grouped_bronze_nyc_weather_df = grouped_bronze_nyc_weather_df.withColumn("day_of_week", dayofweek("date")) \
                                       .withColumn("is_weekend", when(dayofweek("date").isin([7,1]), 1).otherwise(0))

# COMMAND ----------

from pyspark.sql.functions import concat, lit, to_timestamp,col,lpad

# Joining with grouped weather data for final dataframe for modelling
Data_modelling_live_df = bronze_station_status_df.join(grouped_bronze_nyc_weather_df, ["date", "hour"], "left_outer")

# Making the hour column consistent length
Data_modelling_live_df = Data_modelling_live_df.withColumn(
    "hour", lpad(col("hour").cast("string"), 2, "0")
)

Data_modelling_live_df = Data_modelling_live_df.withColumn('date_hour', concat('date', lit(' '), 'hour', lit(':00')))
Data_modelling_live_df = Data_modelling_live_df.withColumn('timestamp', to_timestamp('date_hour', 'yyyy-MM-dd HH:mm'))
Data_modelling_live_df = Data_modelling_live_df.drop("date_hour")

Data_modelling_live_df.printSchema()
display(Data_modelling_live_df)

# COMMAND ----------

# Write the refined dataframe to a gold table
live_data_for_modelling_table_name = 'Gold_G02_modelling_data'
writeDataFrameToDeltaTableOptimized(Data_modelling_live_df, live_data_for_modelling_table_name, "date", "hour")

# COMMAND ----------

# DBTITLE 1,Gold Table for Storing Model Results
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

model_result_schema = StructType([
  StructField("model_name", StringType(), True),
  StructField("version", IntegerType(), True),
  StructField("rmse", DoubleType(), True)
])

model_result_df = spark.createDataFrame([], model_result_schema)

gold_model_result_data_delta_table_name = 'Gold_model_result_data'
writeDataFrameToDeltaTable(model_result_df, gold_model_result_data_delta_table_name) 

# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from g02_db

# COMMAND ----------

import json

# Return Success Code
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
