# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# Delete all the tables underneath the directory G02, Note this also deletes the directory G02
dbutils.fs.rm(GROUP_DATA_PATH, recurse=True)

# COMMAND ----------

# Create the directory G02
dbutils.fs.mkdirs(GROUP_DATA_PATH)

# Create a directory for checkpoints 
checkpoint_directory = GROUP_DATA_PATH+"_checkpoints"
dbutils.fs.mkdirs(checkpoint_directory)

# COMMAND ----------

# Check if the directory is correctly created or not
try:
    if len(dbutils.fs.ls(GROUP_DATA_PATH)) == 1: 
        print("Directory created : ", GROUP_DATA_PATH)    
        print("Checkpoint Directory created : ", GROUP_DATA_PATH)
        display(dbutils.fs.ls(GROUP_DATA_PATH))   
except FileNotFoundError as e:
    print("Oops! Directory not found:", e)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
