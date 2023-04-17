# Databricks notebook source
# MAGIC %run "/Repos/sroy22@ur.rochester.edu/G02-final-project/final_project/includes/includes"

# COMMAND ----------

display(dbutils.fs.ls(NYC_WEATHER_FILE_PATH))

# COMMAND ----------

weather_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(NYC_WEATHER_FILE_PATH)

# COMMAND ----------

import os 

delta_table_name = "historical_weather_data_g02"
output_path = GROUP_DATA_PATH + delta_table_name

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)
    
weather_df.write.format("delta").mode("append").option("path", output_path).saveAsTable(delta_table_name)

# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))
