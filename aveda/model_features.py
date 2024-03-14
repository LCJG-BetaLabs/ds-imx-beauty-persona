# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("base_dir", "")

# COMMAND ----------

import os
import pandas as pd
import pyspark.sql.functions as f

base_dir = dbutils.widgets.get("base_dir") + "/datamart"

sales = spark.read.parquet(os.path.join(base_dir, "transaction.parquet"))
vip = spark.read.parquet(os.path.join(base_dir, "demographic.parquet"))
first_purchase = spark.read.parquet(os.path.join(base_dir, "first_last_transaction.parquet"))

# COMMAND ----------

feature_dir = dbutils.widgets.get("base_dir") + "/features"
os.makedirs(feature_dir, exist_ok=True)

# COMMAND ----------

def save_feature_df(df, filename):
    df.write.parquet(os.path.join(feature_dir, f"{filename}.parquet"), mode="overwrite")

# COMMAND ----------

sales.createOrReplaceTempView("sales0")
vip.createOrReplaceTempView("vip")
first_purchase.createOrReplaceTempView("first_purchase")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sales AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   item_subcat_desc AS item_subcat_desc_cleaned,
# MAGIC   maincat_desc AS maincat_desc_cleaned
# MAGIC FROM sales0
# MAGIC WHERE
# MAGIC   isnull(vip_main_no) = 0 AND vip_main_no != ""
# MAGIC   AND isnull(prod_brand) = 0 AND prod_brand NOT IN ("JBZZZ", "ZZ")
# MAGIC   AND isnull(item_subcat_desc) = 0 AND item_subcat_desc NOT IN ("ZZZ", "Dummy", "dummy")
# MAGIC   AND isnull(maincat_desc) = 0 AND maincat_desc NOT IN ("ZZZ", "Dummy", "dummy")

# COMMAND ----------

sales = spark.table("sales")

# COMMAND ----------

def features_in_list_by_vip(feature, table=sales):
    grouped_df = table.groupBy("vip_main_no").agg(f.collect_list(feature).alias(feature))
    return grouped_df

# COMMAND ----------

# MAGIC %md
# MAGIC demographic

# COMMAND ----------

demographic = spark.sql(f"""with tenure as (
  Select
    distinct
    vip_main_no,
    first_transaction_date AS first_pur_ba,
    round(
      datediff(
        TO_DATE(current_date(), "yyyyMMdd"),
        first_transaction_date
      ) / 365,
      0
    ) as tenure
  from
    first_purchase
)
select
  vip_main_no,
  min(
    case
      when customer_sex = "C"
      OR isnull(customer_sex) = 1
      OR customer_sex = "" then "C"
      else customer_sex
    end
  ) as customer_sex,
  min(
    case
      when cust_nat_cat = "Hong Kong" then "Hong Kong"
      when cust_nat_cat = "Mainland China" then "Mainland China"
      when cust_nat_cat = "Macau" then "Macau"
      else "Others"
    end
  ) as cust_nat_cat,
  case
    when tenure <= 1 then '0-1'
    when tenure > 1
    and tenure <= 3 then '1-3'
    when tenure > 3
    and tenure <= 7 then '3-7'
    else '8+'
  end as tenure,
  max(case 
    when customer_age_group = '01' then '< 25'
    when customer_age_group = '02' then '26 - 30'
    when customer_age_group = '03' then '31 - 35'
    when customer_age_group = '04' then '36 - 40'
    when customer_age_group = '05' then '41 - 50'
    when customer_age_group = '06' then '> 51'
    when customer_age_group = '07' then null
  else null end) as age
from
  sales
  left join tenure using (vip_main_no)
group by
  1,
  4
""")

# COMMAND ----------

# MAGIC %md
# MAGIC SHARE OF WALLET
# MAGIC

# COMMAND ----------

def sum_table(table, agg_col):
    df = table.groupBy("vip_main_no").agg(f.sum(agg_col).alias(agg_col))
    return df


def count_table(table, agg_col):
    df = table.groupBy("vip_main_no").agg(f.countDistinct(agg_col).alias(agg_col))
    return df


# COMMAND ----------

amt = sum_table(sales, "net_amt_hkd")
qty = sum_table(sales, "sold_qty")
no_of_order = count_table(sales, "invoice_no")

# COMMAND ----------

def share_of_wallet(by="item_subcat_desc_cleaned", postfix="_SOW_by_subcat"):
    amt_by_vip_by_feature = (
        sales.groupBy("vip_main_no").pivot(by).agg(f.sum("net_amt_hkd")).fillna(0)
    )
    columns_to_sum = [c for c in amt_by_vip_by_feature.columns if c != "vip_main_no"]
    amt_by_vip_by_feature = amt_by_vip_by_feature.withColumn("sum", sum(f.col(c) for c in columns_to_sum))

    result = amt_by_vip_by_feature
    for col in columns_to_sum:
        result = result.withColumn(
            col + postfix, (f.col(col) / f.col("sum")) * 100
        )
    columns_to_keep = ["vip_main_no"] + [c + postfix for c in columns_to_sum]
    return result.select(*columns_to_keep)


# COMMAND ----------

sow_subcat = share_of_wallet(by="item_subcat_desc_cleaned", postfix="_SOW_by_subcat")

# COMMAND ----------

sow_subcat.createOrReplaceTempView("sow_subcat")

# COMMAND ----------

sow_subcat = spark.sql("""
    SELECT vip_main_no,
        Conditioner_SOW_by_subcat,
        Shampoo_SOW_by_subcat,
        Style_SOW_by_subcat,
        Treatment_SOW_by_subcat
    FROM sow_subcat
""")

# COMMAND ----------

save_feature_df(sow_subcat, "sow_subcat")

# COMMAND ----------

sow_maincat = share_of_wallet(by="maincat_desc_cleaned", postfix="_SOW_by_maincat")

# COMMAND ----------

sow_maincat.createOrReplaceTempView("sow_maincat")

# COMMAND ----------

sow_maincat = spark.sql("""
    SELECT vip_main_no,
        Skin_SOW_by_maincat,
        Body_SOW_by_maincat
    FROM sow_maincat
""")

# COMMAND ----------

save_feature_df(sow_maincat, "sow_maincat")

# COMMAND ----------

sow_prodline = share_of_wallet(by="item_product_line_desc", postfix="_SOW_by_prodline")

# COMMAND ----------

sow_prodline.createOrReplaceTempView("sow_prodline")

# COMMAND ----------

sow_prodline = spark.sql("""
    SELECT vip_main_no,
        `Botanical Repair_SOW_by_prodline`,
        (
            `Color Control_SOW_by_prodline` + 
            `Color Enhancers_SOW_by_prodline`
        ) AS Color_SOW_by_prodline,
        (
            `Invati Advanced_SOW_by_prodline` + 
            `Invati Men_SOW_by_prodline`
        ) AS Invati_SOW_by_prodline,
        (
            `Botancial Kinetcs_SOW_by_prodline` + 
            `Brilliant_SOW_by_prodline` +
            `Chakra_SOW_by_prodline` +
            `Cherry Almond_SOW_by_prodline` +
            `Composition_SOW_by_prodline` +
            `Curl_SOW_by_prodline` +
            `Damage Remedy_SOW_by_prodline` +
            `Holiday 05_SOW_by_prodline` +
            `Light Elements_SOW_by_prodline` +
            `Pure Abundance_SOW_by_prodline` +
            `Sap Moss_SOW_by_prodline` +
            `Smooth Fusion_SOW_by_prodline` +
            `Stress-Fix_SOW_by_prodline` +
            `Sun Care_SOW_by_prodline` +
            `Tea_SOW_by_prodline` +
            `Tulasara_SOW_by_prodline` +
            `Volumizing_SOW_by_prodline`
        ) AS Others_SOW_by_prodline
    FROM sow_prodline
""")

# COMMAND ----------

save_feature_df(sow_prodline, "sow_prodline")
