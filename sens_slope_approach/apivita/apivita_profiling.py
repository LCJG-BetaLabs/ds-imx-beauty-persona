# Databricks notebook source
region = "hk"

# COMMAND ----------

import os

base_dir = "/mnt/dev/customer_segmentation/imx/apivita/datamart"
spark.read.parquet(
    os.path.join(base_dir, f"value_segment_{region}.parquet")
).createOrReplaceTempView("value_segment")
spark.read.parquet(
    os.path.join(base_dir, "transaction.parquet")
).createOrReplaceTempView("transaction")
spark.read.parquet(
    os.path.join(base_dir, "demographic.parquet")
).createOrReplaceTempView("demographic")
spark.read.parquet(
    os.path.join(base_dir, f"total_clv_{region}.parquet")
).createOrReplaceTempView("total_clv")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view value_segment_final as
# MAGIC select 
# MAGIC   a.*, 
# MAGIC   b.p12m_clv,
# MAGIC   b.value_segment,
# MAGIC   CASE WHEN a.new_joiner_flag = "New Joiner" THEN "New Joiner"
# MAGIC   WHEN a.inactive_flag = "Inactive P24" THEN "Inactive P24"
# MAGIC   WHEN a.inactive_flag = "Inactive P12" THEN "Inactive P12"
# MAGIC   ELSE b.value_segment END AS customer_tag
# MAGIC from total_clv a
# MAGIC left join value_segment b using (vip_main_no)

# COMMAND ----------

import pyspark.sql.functions as f


def sum_pivot_table(table, group_by_col, agg_col, show_inactive=True):
    df = table.groupBy("customer_tag", group_by_col).agg(f.sum(agg_col))
    pivot_table = (
        df.groupBy(group_by_col).pivot("customer_tag").agg(f.sum(f"sum({agg_col})"))
    )
    if show_inactive:
        display(pivot_table.select(group_by_col, "Engaged", "Emerging", "Low Value", "At Risk", "New Joiner", "Inactive P12", "Inactive P24")) # , "Inactive P12", "Inactive P24"
    else:
        display(pivot_table.select(group_by_col, "Engaged", "Emerging", "Low Value", "At Risk", "New Joiner"))
    return pivot_table


def count_pivot_table(table, group_by_col, agg_col, percentage=False, show_inactive=True):
    df = table.groupBy("customer_tag", group_by_col).agg(f.countDistinct(agg_col).alias("count"))
    if percentage is True:
        total_counts = df.agg(f.sum("count")).collect()[0][0]
    pivot_table = (
        df.groupBy(group_by_col)
        .pivot("customer_tag")
        .agg(f.sum(f"count"))
    )
    if percentage is True:
        pivot_table = pivot_table.toPandas()
        pivot_table.iloc[:, 1:] = (pivot_table.iloc[:, 1:] / pivot_table.iloc[:, 1:] * 100).round(2)
        display(pivot_table[[group_by_col, "Engaged", "Emerging", "Low Value", "At Risk", "New Joiner", "Inactive P12", "Inactive P24"]])
    else:
        if show_inactive:
            display(pivot_table.select(group_by_col, "Engaged", "Emerging", "Low Value", "At Risk", "New Joiner", "Inactive P12", "Inactive P24")) # , "Inactive P12", "Inactive P24"
        else:
            display(pivot_table.select(group_by_col, "Engaged", "Emerging", "Low Value", "At Risk", "New Joiner"))
    return pivot_table

# COMMAND ----------

final_demo_table = spark.sql(
    """
    select *, 1 as dummy from value_segment_final
    left join demographic using (vip_main_no)
    """
)
final_demo_table.createOrReplaceTempView("final_demo_table")

# COMMAND ----------

final_sales_table = spark.sql(
    """
    select *, 1 as dummy from value_segment_final
    left join transaction using (vip_main_no)
    """
)
final_sales_table.createOrReplaceTempView("final_sales_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demographic Profile

# COMMAND ----------

# MAGIC %md
# MAGIC count of customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct vip_main_no), customer_tag from final_sales_table group by customer_tag

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct vip_main_no) from imx_prd.dashboard_crm_gold.beauty_valid_tx
# MAGIC where membership_p12m_ba = "New Joiners" and date_order = 0 and order_date >= "2022-10-01" and order_date <= "2023-09-30" and is_member =1

# COMMAND ----------

# %sql
# select count(distinct vip_main_no) from imx_prd.dashboard_crm_gold.beauty_valid_tx
# where membership_p12m_ba = "New Joiners" and date_order = 0 and order_date >= "2022-10-01" and order_date <= "2023-09-30" and is_member =1

# COMMAND ----------

count_pivot_table(final_sales_table, group_by_col="dummy", agg_col="vip_main_no")

# COMMAND ----------

count_pivot_table(final_demo_table, group_by_col="dummy", agg_col="vip_main_no")

# COMMAND ----------

count_pivot_table(final_demo_table, group_by_col="dummy", agg_col="vip_main_no", percentage=True)

# COMMAND ----------

# MAGIC %md
# MAGIC gender

# COMMAND ----------

# count_pivot_table(final_demo_table, group_by_col="VIP_SEX", agg_col="vip_main_no")

# COMMAND ----------

# count_pivot_table(final_demo_table, group_by_col="VIP_SEX", agg_col="vip_main_no", percentage=True)

# COMMAND ----------

df = spark.sql("""select
  distinct vip_main_no,
  case
    when tenure <= 1 then '0-1'
    when tenure > 1
    and tenure <= 3 then '1-3'
    when tenure > 3
    and tenure <= 7 then '3-7'
    else '8+'
  end as tenure,
  customer_tag
from
  final_demo_table
""")
count_pivot_table(df, group_by_col="tenure", agg_col="vip_main_no")

# COMMAND ----------

df = spark.sql(
    """
    with tem as (select distinct vip_main_no, case when customer_sex = "C" OR isnull(customer_sex) = 1 then "C"
        else customer_sex end as customer_sex_new,
        customer_tag
        from final_sales_table)
        select distinct vip_main_no, min(customer_sex_new) as customer_sex_new, customer_tag from tem group by vip_main_no, customer_tag

    """
)
count_pivot_table(df, group_by_col="customer_sex_new", agg_col="vip_main_no")

# COMMAND ----------

# MAGIC %sql
# MAGIC with tem as (select vip_main_no, case when customer_sex = "C" OR isnull(customer_sex) = 1 then "C"
# MAGIC else customer_sex end as customer_sex_new,
# MAGIC customer_tag
# MAGIC from final_sales_table
# MAGIC where customer_tag = "Engaged")
# MAGIC select vip_main_no, customer_sex_new from tem
# MAGIC group by vip_main_no, customer_sex_new
# MAGIC having count(vip_main_no) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC with tem as (select distinct vip_main_no, case when customer_sex = "C" OR isnull(customer_sex) = 1 then "C"
# MAGIC else customer_sex end as customer_sex_new,
# MAGIC customer_tag
# MAGIC from final_sales_table),
# MAGIC tem2 as (
# MAGIC   select distinct vip_main_no, min(customer_sex_new) as customer_sex_new, customer_tag from tem group by vip_main_no, customer_tag
# MAGIC )
# MAGIC select customer_tag, customer_sex_new, count(distinct vip_main_no) from tem2 group by customer_tag, customer_sex_new

# COMMAND ----------

# count_pivot_table(df, group_by_col="customer_sex_new", agg_col="vip_main_no", percentage=True)

# COMMAND ----------

# MAGIC %md
# MAGIC tenure

# COMMAND ----------

df = spark.sql("""select
  distinct vip_main_no,
  case
    when tenure <= 1 then '0-1'
    when tenure > 1
    and tenure <= 3 then '1-3'
    when tenure > 3
    and tenure <= 7 then '3-7'
    else '8+'
  end as tenure,
  customer_tag
from
  final_demo_table
""")
count_pivot_table(df, group_by_col="tenure", agg_col="vip_main_no")

# COMMAND ----------

# MAGIC %md
# MAGIC VIP_NATION

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct cust_nat_cat from final_sales_table

# COMMAND ----------

df = spark.sql(
    """
    with tem as (select *,
    case when cust_nat_cat = "Hong Kong" then "Hong Kong" 
    when cust_nat_cat = "Mainland China" then "Mainland China" 
    when cust_nat_cat = "Macau" then "Macau" 
    when cust_nat_cat = "Taiwanese" then "Taiwanese" 
    else "Others" end as cust_nat_cat_new
    from final_sales_table)
    select distinct vip_main_no, min(cust_nat_cat_new) cust_nat_cat_new, customer_tag from tem group by vip_main_no, customer_tag
    """
)
count_pivot_table(df, group_by_col="cust_nat_cat_new", agg_col="vip_main_no")

# COMMAND ----------

count_pivot_table(df, group_by_col="cust_nat_cat_new", agg_col="vip_main_no", percentage=True)

# COMMAND ----------

# count_pivot_table(final_demo_table, group_by_col="VIP_NATION", agg_col="vip_main_no")

# COMMAND ----------

# MAGIC %md
# MAGIC shop region

# COMMAND ----------

df = spark.sql(
    """
    select distinct vip_main_no, min(region_key) region_key, customer_tag from final_sales_table group by vip_main_no, customer_tag
    """
)
count_pivot_table(df, group_by_col="region_key", agg_col="vip_main_no")

# COMMAND ----------

count_pivot_table(final_sales_table, group_by_col="region_key", agg_col="vip_main_no")

# COMMAND ----------

count_pivot_table(final_sales_table, group_by_col="region_key", agg_col="vip_main_no", percentage=True)

# COMMAND ----------

# MAGIC %md
# MAGIC age group

# COMMAND ----------

df = spark.sql(
    """
    select distinct vip_main_no, min(customer_age_group) customer_age_group, customer_tag from final_sales_table group by vip_main_no, customer_tag
    """
)

table = count_pivot_table(df, group_by_col="customer_age_group", agg_col="vip_main_no").createOrReplaceTempView("age_gp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   distinct
# MAGIC   case 
# MAGIC     when customer_age_group = '01' then '< 25'
# MAGIC     when customer_age_group = '02' then '26 - 30'
# MAGIC     when customer_age_group = '03' then '31 - 35'
# MAGIC     when customer_age_group = '04' then '36 - 40'
# MAGIC     when customer_age_group = '05' then '41 - 50'
# MAGIC     when customer_age_group = '06' then '> 51'
# MAGIC     when customer_age_group = '07' then null
# MAGIC   else null end as age,
# MAGIC   sum(Engaged),
# MAGIC   sum(Emerging),
# MAGIC   sum(`Low Value`),
# MAGIC   sum(`At Risk`),
# MAGIC   sum(`New Joiner`),
# MAGIC   sum(`Inactive P12`),
# MAGIC   sum(`Inactive P24`)
# MAGIC   
# MAGIC from age_gp
# MAGIC group by age

# COMMAND ----------

# MAGIC %md
# MAGIC ## transactional profile

# COMMAND ----------

# MAGIC %md
# MAGIC amt, qty, # of order, # of visit

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table final_sales_table

# COMMAND ----------

df = spark.sql(
    """
    select * from final_sales_table
    where  order_date >= "2022-10-01" and order_date <= "2023-09-30"  
    """
)
sum_pivot_table(df, group_by_col="dummy", agg_col="net_amt_hkd", show_inactive=False)

# COMMAND ----------

sum_pivot_table(df, group_by_col="dummy", agg_col="sold_qty", show_inactive=False)

# COMMAND ----------

count_pivot_table(df, group_by_col="dummy", agg_col="invoice_no", show_inactive=False)

# COMMAND ----------

visit = spark.sql(
    """with visit as (
select
  distinct vip_main_no,
  order_date,
  shop_code,
  customer_tag
from final_sales_table
 where  order_date >= "2022-10-01" and order_date <= "2023-09-30" 
)
select 
  vip_main_no,
  order_date,
  shop_code,
  customer_tag,
  count(distinct vip_main_no,
  order_date,
  shop_code) as visit,
  1 as dummy
from visit
group by
  vip_main_no,
  order_date,
  shop_code,
  customer_tag
""")

# COMMAND ----------

sum_pivot_table(visit, group_by_col="dummy", agg_col="visit", show_inactive=False)

# COMMAND ----------

# MAGIC %md
# MAGIC unit table
# MAGIC for
# MAGIC - share of wallet (by product class)
# MAGIC - AVERAGE ITEM VALUE
# MAGIC - MEMBER PENETRATION
# MAGIC - $ SPEND PER MEMBER

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table transaction

# COMMAND ----------

# %sql
# -- 1. amt table by category and segment
# select * from
# (select 
#     distinct item_product_line_desc,
#     customer_tag, 
#     sum(net_amt_hkd) as overall_amount
# from final_sales_table
# where  order_date >= "2022-10-01" and order_date <= "2023-09-30" 
# group by 
#     customer_tag,
#     item_product_line_desc
# )
# PIVOT (
#   SUM(overall_amount)
#   FOR customer_tag IN ("Engaged", "Emerging", "Low Value", "At Risk",  "New Joiner", "Inactive P12", "Inactive P24")
# ) 

# COMMAND ----------

# %sql
# -- 2. qty table by category and segment
# select * from
# (select 
#     distinct item_product_line_desc,
#     customer_tag, 
#     sum(sold_qty) as overall_sold_qty
# from final_sales_table
# where  order_date >= "2022-10-01" and order_date <= "2023-09-30" 
# group by 
#     customer_tag,
#     item_product_line_desc
# )
# PIVOT (
#   SUM(overall_sold_qty)
#   FOR customer_tag IN ("Engaged", "Emerging", "Low Value", "At Risk",  "New Joiner", "Inactive P12", "Inactive P24")
# ) 

# COMMAND ----------

# %sql
# -- 3. number of member purchase by category and segment
# select * from
# (select 
#     distinct item_product_line_desc,
#     customer_tag, 
#     count(distinct vip_main_no) as vip_count
# from final_sales_table
# where  order_date >= "2022-10-01" and order_date <= "2023-09-30" 
# group by 
#     customer_tag,
#     item_product_line_desc
# )
# PIVOT (
#   SUM(vip_count)
#   FOR customer_tag IN ("Engaged", "Emerging", "Low Value", "At Risk",  "New Joiner", "Inactive P12", "Inactive P24")
# ) 

# COMMAND ----------

# MAGIC %md
# MAGIC MEMBER PENETRATION BY STORE
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cust count by store and segment
# MAGIC select * from
# MAGIC (select 
# MAGIC     distinct shop_desc,
# MAGIC     customer_tag, 
# MAGIC     count(distinct vip_main_no) as vip_count
# MAGIC from final_sales_table
# MAGIC where  order_date >= "2022-10-01" and order_date <= "2023-09-30" 
# MAGIC group by 
# MAGIC     customer_tag,
# MAGIC     shop_desc
# MAGIC )
# MAGIC PIVOT (
# MAGIC   SUM(vip_count)
# MAGIC   FOR customer_tag IN ("Engaged", "Emerging", "Low Value", "At Risk",  "New Joiner", "Inactive P12", "Inactive P24")
# MAGIC ) 

# COMMAND ----------

# MAGIC %md
# MAGIC Member penetration by month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cust count by yearmon and segment
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       distinct yyyymm,
# MAGIC       customer_tag,
# MAGIC       count(distinct vip_main_no) as vip_count
# MAGIC     from
# MAGIC       (
# MAGIC         select
# MAGIC           *,
# MAGIC           CONCAT(
# MAGIC             year(order_date),
# MAGIC             LPAD(month(order_date), 2, '0')
# MAGIC           ) as yyyymm
# MAGIC         from
# MAGIC           final_sales_table
# MAGIC       )
# MAGIC     group by
# MAGIC       customer_tag,
# MAGIC       yyyymm
# MAGIC   ) PIVOT (
# MAGIC     SUM(vip_count) FOR customer_tag IN ("Engaged", "Emerging", "Low Value", "At Risk",  "New Joiner", "Inactive P12", "Inactive P24")
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare with Last Year Value Segment

# COMMAND ----------

prefix = "_last_year"

spark.read.parquet(
    os.path.join(base_dir, f"value_segment{prefix}_{region}.parquet")
).createOrReplaceTempView(f"value_segment{prefix}")
spark.read.parquet(
    os.path.join(base_dir, f"transaction{prefix}.parquet")
).createOrReplaceTempView(f"transaction{prefix}")
spark.read.parquet(
    os.path.join(base_dir, f"demographic{prefix}.parquet")
).createOrReplaceTempView(f"demographic{prefix}")
spark.read.parquet(
    os.path.join(base_dir, f"total_clv{prefix}_{region}.parquet")
).createOrReplaceTempView(f"total_clv{prefix}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from value_segment

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from value_segment_last_year

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view value_segment_last_year_final as
# MAGIC select 
# MAGIC   a.*, 
# MAGIC   b.p12m_clv,
# MAGIC   b.value_segment,
# MAGIC   CASE WHEN a.new_joiner_flag = "New Joiner" THEN "New Joiner"
# MAGIC   WHEN a.inactive_flag = "Inactive P24" THEN "Inactive P24"
# MAGIC   WHEN a.inactive_flag = "Inactive P12" THEN "Inactive P12"
# MAGIC   ELSE b.value_segment END AS customer_tag
# MAGIC from total_clv_last_year a
# MAGIC left join value_segment_last_year b using (vip_main_no);
# MAGIC
# MAGIC create or replace temp view comparison as
# MAGIC select 
# MAGIC   a.vip_main_no,
# MAGIC   a.customer_tag,
# MAGIC   coalesce(b.customer_tag_last_year, "Inactive P24") AS customer_tag_last_year
# MAGIC from value_segment_final a
# MAGIC left join 
# MAGIC (select vip_main_no, customer_tag as customer_tag_last_year from value_segment_last_year_final) b using (vip_main_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cust count by store and segment
# MAGIC select * from
# MAGIC (select 
# MAGIC     distinct customer_tag_last_year,
# MAGIC     customer_tag,
# MAGIC     count(distinct vip_main_no) as vip_count
# MAGIC from comparison
# MAGIC group by 
# MAGIC     customer_tag,
# MAGIC     customer_tag_last_year
# MAGIC )
# MAGIC PIVOT (
# MAGIC   SUM(vip_count)
# MAGIC   FOR customer_tag IN ("Engaged", "Emerging", "Low Value", "At Risk", "New Joiner", "Inactive P12", "Inactive P24")
# MAGIC ) 

# COMMAND ----------


