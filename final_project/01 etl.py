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

# Databricks notebook source
# MAGIC %run "/Shared/final-project/includes"

# COMMAND ----------

csv_files = dbutils.fs.ls("dbfs:/FileStore/tables/raw/bike_trips")


# COMMAND ----------

display(csv_files)

# COMMAND ----------

from delta.tables import DeltaTable

# Load the Delta table as a DataFrame
df = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/bronze_station_info.delta").toDF()

# Show the DataFrame
display(df)


# COMMAND ----------


from delta.tables import DeltaTable

# Load the Delta table as a DataFrame
df = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/bronze_station_status.delta").toDF()

# Show the DataFrame
display(df)


# COMMAND ----------


from delta.tables import DeltaTable

# Load the Delta table as a DataFrame
df = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/bronze_nyc_weather.delta").toDF()

# Show the DataFrame
display(df)


# COMMAND ----------

dfs = []
for file in csv_files:
    if file.isFile() and file.name.endswith(".csv"):
        df = spark.read.csv(file.path, header=True, inferSchema=True)
        dfs.append(df)


# COMMAND ----------

combined_df = dfs[0]
for df in dfs[1:]:
      combined_df = combined_df.union(df)


# COMMAND ----------

display(combined_df.tail(5))

df_filtered = combined_df.filter(combined_df.start_station_name == 'Broadway & E 14 St')

display(station_df.count())

station_df.columns

from pyspark.sql.functions import month, year, count,col,sum
from pyspark.sql.window import Window
# add new columns for the year and month of each ride
df_filtered = df_filtered.withColumn("year", year("started_at"))
df_filtered = df_filtered.withColumn("month", month("started_at"))

# group the DataFrame by year and month and count the number of rides in each group
grouped_df = df_filtered.groupBy("year", "month").agg(count("ride_id").alias("num_trips"))

# display the results
display(grouped_df)

from pyspark.sql.functions import month, year, count,col,sum,date
from pyspark.sql.window import Window
# add new columns for the year and month of each ride
df_filtered = df_filtered.withColumn("year", year("started_at"))
df_filtered = df_filtered.withColumn("month", month("started_at"))
df_filtered = df_filtered.withColumn("date", date("started_at"))

# group the DataFrame by year and month and count the number of rides in each group
grouped_df = df_filtered.groupBy("date").agg(count("ride_id").alias("num_trips"))
display(grouped_df)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
