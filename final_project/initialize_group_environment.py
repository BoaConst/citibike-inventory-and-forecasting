# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# Delete all the tables underneath the directory G02, Note this also deletes the directory G02
dbutils.fs.rm(GROUP_DATA_PATH, recurse=True)

# COMMAND ----------

# Create the directory G02
dbutils.fs.mkdirs(GROUP_DATA_PATH)

# COMMAND ----------

# Check if the directory is correctly created or not
try:
    if len(dbutils.fs.ls(GROUP_DATA_PATH)) == 0: 
        print("Directory created : ", GROUP_DATA_PATH)
    else:
        display(dbutils.fs.ls(GROUP_DATA_PATH))
except FileNotFoundError as e:
    print("Oops! File not found:", e)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
