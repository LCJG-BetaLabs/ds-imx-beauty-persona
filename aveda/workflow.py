# Databricks notebook source
import datetime

# Get the current date
current_date = datetime.date.today()
end_date = current_date.replace(day=1) - datetime.timedelta(days=1)
start_date = current_date.replace(year=current_date.year - 1, day=1)
quarter_no = spark.sql(f"SELECT quarter('{end_date}')").collect()[0][0]

# COMMAND ----------

start_date, end_date

# COMMAND ----------

base_dir_no_dbfs = f"/mnt/prd/customer_segmentation/imx/aveda/{end_date.year}/Q{quarter_no}"
base_dir = "/dbfs" + base_dir_no_dbfs
start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")

# COMMAND ----------

jobs = [
    {
        "notebook_path": "../aveda/datamart",
        "arguments": {
            "base_dir": base_dir_no_dbfs, 
            "start_date": start_date,
            "end_date": end_date
            },
    },
    {
        "notebook_path": "../aveda/model_features",
        "arguments": {
            "base_dir": base_dir_no_dbfs,
            "start_date": start_date,
            "end_date": end_date
            },
    },
    {
        "notebook_path": "../aveda/predict",
        "arguments": {
            "base_dir": base_dir_no_dbfs,
            "start_date": start_date,
            "end_date": end_date
            },
    },
    {
        "notebook_path": "../aveda/profile_and_export",
        "arguments": {
            "base_dir": base_dir_no_dbfs,
            "start_date": start_date,
            "end_date": end_date
            },
    },
]

for job in jobs:
    dbutils.notebook.run(job["notebook_path"], 0, job["arguments"])
