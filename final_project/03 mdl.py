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

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G02/bronze_station_info/'))

# COMMAND ----------

nyc_weather = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/bronze_nyc_weather/')

# COMMAND ----------

display(nyc_weather)

# COMMAND ----------

bike_station_info = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/bronze_station_info/')

# COMMAND ----------

display(bike_station_info)

# COMMAND ----------

bike_station_status = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/bronze_station_status/')

# COMMAND ----------

display(bike_station_status)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G02/historic_bike_trip_ashok/'))

# COMMAND ----------

historic_bike_trip_g02_ashok = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_bike_trip_g02_ashok/')

# COMMAND ----------

display(historic_bike_trip_g02_ashok)

# COMMAND ----------

historic_nyc_weather_ashok = spark.read.format("delta").load('dbfs:/FileStore/tables/G02/historic_nyc_weather_ashok/')

# COMMAND ----------

display(historic_nyc_weather_ashok)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, date_format

historic_nyc_weather_ashok.select(
    date_format(from_unixtime('timestamp_col'), 'yyyy-MM-dd').alias('date'),
    date_format(from_unixtime('timestamp_col'), 'HH:mm:ss').alias('time')
)

display(historic_nyc_weather_ashok)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
