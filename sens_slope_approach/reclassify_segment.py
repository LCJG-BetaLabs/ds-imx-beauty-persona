# Databricks notebook source
"""
hard code reclassify
- customer who is "Engaged" last year and "Emerging" this year to "At Risk" this year
- customer who is "Low Value" last year and "At Risk" this year to "Low Value" this year
"""

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("brand", "")
dbutils.widgets.text("region", "")

# COMMAND ----------

import os

base_dir = f"""/mnt/dev/customer_segmentation/imx/{getArgument("brand")}/datamart"""

region = getArgument("region")
if region != "":
    postfix = f"_{region.lower()}"
else:
    postfix = ""

spark.read.parquet(
    os.path.join(base_dir, f"value_segment{postfix}.parquet")
).createOrReplaceTempView("this_year_segment")
spark.read.parquet(
    os.path.join(base_dir, f"value_segment_last_year{postfix}.parquet")
).createOrReplaceTempView("last_year_segment")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW Result AS WITH comparison AS (
# MAGIC   SELECT
# MAGIC     a.*,
# MAGIC     CASE WHEN isnull(b.value_segment_last_year) = 1 THEN "New Joiner / Inactive P24M" ELSE value_segment_last_year END AS value_segment_last_year
# MAGIC   FROM
# MAGIC     this_year_segment a
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         vip_main_no,
# MAGIC         value_segment AS value_segment_last_year
# MAGIC       FROM
# MAGIC         last_year_segment
# MAGIC     ) b USING (vip_main_no)
# MAGIC )
# MAGIC SELECT
# MAGIC   vip_main_no,
# MAGIC   first_transaction_date,
# MAGIC   avg_annual_revenue,
# MAGIC   new_joiner_flag,
# MAGIC   inactive_flag,
# MAGIC   p12m_clv,
# MAGIC   tenure,
# MAGIC   CASE
# MAGIC     WHEN value_segment = "Emerging"
# MAGIC     AND value_segment_last_year = "Engaged" THEN "At Risk"
# MAGIC     WHEN value_segment = "At Risk"
# MAGIC     AND value_segment_last_year = "Low Value" THEN "Low Value"
# MAGIC     ELSE value_segment
# MAGIC   END AS value_segment
# MAGIC FROM
# MAGIC   comparison

# COMMAND ----------

# overwrite original file
spark.table("Result").write.parquet(
    os.path.join(base_dir, f"value_segment{postfix}.parquet"), mode="overwrite"
)

# COMMAND ----------


