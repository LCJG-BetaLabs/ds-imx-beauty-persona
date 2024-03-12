# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "") 
dbutils.widgets.text("base_dir", "")

# COMMAND ----------

import os
import pyspark.sql.functions as f
import pandas as pd

datamart_dir = os.path.join(dbutils.widgets.get("base_dir"), "datamart")

sales = spark.read.parquet(os.path.join(datamart_dir, "transaction.parquet"))
vip = spark.read.parquet(os.path.join(datamart_dir, "demographic.parquet"))
first_purchase = spark.read.parquet(os.path.join(datamart_dir, "first_last_transaction.parquet"))
sales.createOrReplaceTempView("sales")
vip.createOrReplaceTempView("vip")
first_purchase.createOrReplaceTempView("first_purchase")

# COMMAND ----------

# clustering result
model_dir = os.path.join(dbutils.widgets.get("base_dir"), "model")
persona = spark.read.parquet(os.path.join(model_dir, "clustering_result.parquet"))
persona.createOrReplaceTempView("persona0")


# COMMAND ----------

# MAGIC %md
# MAGIC persona
# MAGIC
# MAGIC <!-- 0&1&2: Average Fashion Connoisseur
# MAGIC
# MAGIC 3: Prime Connoisseur
# MAGIC
# MAGIC 4: Menswear Specialist
# MAGIC
# MAGIC 5: Outerwear Fashionista
# MAGIC
# MAGIC 6: Bottoms and Dresses Diva -->

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW persona AS
# MAGIC SELECT
# MAGIC   vip_main_no,
# MAGIC   CASE WHEN persona = 0 THEN "Haircare Enthusiasts"
# MAGIC   WHEN persona = 1 THEN "Tress Treatment Devotees"
# MAGIC   WHEN persona = 2 THEN "Shampoo Connoisseurs"
# MAGIC   WHEN persona = 3 THEN "Beauty Style Seekers"
# MAGIC   WHEN persona = 4 THEN "Skincare Enthusiasts" END AS persona
# MAGIC FROM persona0

# COMMAND ----------

cluster_order = ["Haircare Enthusiasts", "Tress Treatment Devotees", "Shampoo Connoisseurs", "Beauty Style Seekers", "Skincare Enthusiasts"]

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW persona_counts AS
# MAGIC SELECT persona, COUNT(*) AS count, COUNT(DISTINCT vip_main_no) as no_of_vip
# MAGIC FROM persona
# MAGIC GROUP BY persona
# MAGIC WITH ROLLUP;

# COMMAND ----------

persona_df = spark.sql("""
SELECT
  'no. of customer' AS Persona,
  SUM(CASE WHEN persona = 'Haircare Enthusiasts' THEN count END) AS Haircare_Enthusiasts,
  SUM(CASE WHEN persona = 'Tress Treatment Devotees' THEN count END) AS Tress_Treatment_Devotees,
  SUM(CASE WHEN persona = 'Shampoo Connoisseurs' THEN count END) AS Shampoo_Connoisseurs,
  SUM(CASE WHEN persona = 'Beauty Style Seekers' THEN count END) AS Beauty_Style_Seekers,
  SUM(CASE WHEN persona = 'Skincare Enthusiasts' THEN count END) AS Skincare_Enthusiasts,
  SUM(CASE WHEN persona IS NULL THEN count END) AS Total
FROM persona_counts
""").toPandas()

# COMMAND ----------

output_dir = os.path.join("/dbfs" + dbutils.widgets.get("base_dir"), "output")
os.makedirs(output_dir, exist_ok=True)
persona_df.to_csv(os.path.join(output_dir, "persona.csv"), index=False)

# COMMAND ----------


def sum_pivot_table(table, group_by_col, agg_col, show_inactive=True):
    df = table.groupBy("customer_tag", group_by_col).agg(f.sum(agg_col))
    pivot_table = (
        df.groupBy(group_by_col).pivot("customer_tag").agg(f.sum(f"sum({agg_col})"))
    )
    display(pivot_table.select(group_by_col, *cluster_order))
    return pivot_table
    


def count_pivot_table(table, group_by_col, agg_col, percentage=False, show_inactive=True):
    df = table.groupBy("customer_tag", group_by_col).agg(f.countDistinct(agg_col).alias("count"))
    pivot_table = (
        df.groupBy(group_by_col)
        .pivot("customer_tag")
        .agg(f.sum(f"count"))
    )
    display(pivot_table.select(group_by_col, *cluster_order))
    return pivot_table


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sales_cleaned AS
# MAGIC WITH cte1 AS (
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   CASE WHEN maincat_desc = "gift" THEN "GIFT"
# MAGIC   WHEN maincat_desc = "ZZ" OR maincat_desc = "Dummy" THEN "Unknown" ELSE maincat_desc END AS maincat_desc_cleaned,
# MAGIC   CASE WHEN item_subcat_desc = "ZZZ" THEN "Unknown" 
# MAGIC   WHEN item_subcat_desc = "dummy" THEN "Unknown" 
# MAGIC   WHEN item_subcat_desc = "Dummy" THEN "Unknown" 
# MAGIC   WHEN item_subcat_desc = "gift" THEN "GIFT" ELSE item_subcat_desc END AS item_subcat_desc_cleaned
# MAGIC from sales
# MAGIC ),
# MAGIC Cte2 AS (
# MAGIC   select 
# MAGIC     *,
# MAGIC     concat(maincat_desc_cleaned, " - ", item_subcat_desc_cleaned) as maincat_and_subcat 
# MAGIC   from cte1
# MAGIC )
# MAGIC SELECT * FROM Cte2
# MAGIC WHERE prod_brand = "BA"
# MAGIC AND maincat_desc_cleaned not in ("Unknown")
# MAGIC AND sales_staff_flag = 0

# COMMAND ----------

final_sales_table = spark.sql(
    """
    select *, 1 as dummy, persona as customer_tag from sales_cleaned
    inner join persona using (vip_main_no)
    """
)
final_sales_table.createOrReplaceTempView("final_sales_table")

# COMMAND ----------

# MAGIC %md
# MAGIC demographic

# COMMAND ----------

# count of customer
count_pivot_table(final_sales_table, group_by_col="dummy", agg_col="vip_main_no")

# COMMAND ----------

# MAGIC %md
# MAGIC transactional

# COMMAND ----------

# amt
df = spark.sql(
    """
    select * from final_sales_table
    where order_date >= getArgument("start_date") and order_date <= getArgument("end_date")
    """
)
sum_pivot_table(df, group_by_col="dummy", agg_col="net_amt_hkd", show_inactive=False)

# COMMAND ----------

# qty
sum_pivot_table(df, group_by_col="dummy", agg_col="sold_qty", show_inactive=False)

# COMMAND ----------

# # of order
count_pivot_table(df, group_by_col="dummy", agg_col="invoice_no", show_inactive=False)

# COMMAND ----------

def pivot_table_by_cat(group_by="item_subcat_desc_cleaned", agg_col="net_amt_hkd", mode="sum",
                       table="final_sales_table"):
    df = spark.sql(
        f"""
        select * from
            (select 
                distinct 
                case when isnull({group_by}) = 1 or {group_by} = "N/A" then "Unknown" else {group_by} end as {group_by},
                customer_tag, 
                {mode}({agg_col}) as overall_amount
            from {table}
            where order_date >= getArgument("start_date") and order_date <= getArgument("end_date")
            group by 
                customer_tag,
                {group_by}
            )
            PIVOT (
            SUM(overall_amount)
            FOR customer_tag IN ("Haircare Enthusiasts", 
                "Tress Treatment Devotees", 
                "Shampoo Connoisseurs", 
                "Beauty Style Seekers", 
                "Skincare Enthusiasts")
            )
        """
    )
    display(df)
    return df

# COMMAND ----------

# amt table by subcat and segment
pivot_table_by_cat(group_by="item_subcat_desc_cleaned", agg_col="net_amt_hkd", mode="sum")

# COMMAND ----------

# amt table by maincat_desc and segment
pivot_table_by_cat(group_by="maincat_desc_cleaned", agg_col="net_amt_hkd", mode="sum")

# COMMAND ----------

amt_df = spark.sql(
    """
    select * from final_sales_table
    where order_date >= getArgument("start_date") and order_date <= getArgument("end_date")
    """
).groupBy("customer_tag", "dummy").agg(f.sum("net_amt_hkd")).groupBy("dummy").pivot("customer_tag").agg(f.sum(f"sum(net_amt_hkd)")).select("dummy", *cluster_order)
amt_df.display()

# COMMAND ----------

# MAGIC %run "/utils/sendgrid_utils"

# COMMAND ----------

import datetime

current_date = datetime.date.today()    

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
quarter_no = spark.sql(f"SELECT quarter('{end_date}')").collect()[0][0]
quarter_year = (current_date.replace(day=1) - datetime.timedelta(days=1)).year

base_dir = getArgument("base_dir").replace("/dbfs", "")

subcat_df = pivot_table_by_cat(group_by="item_subcat_desc_cleaned", agg_col="net_amt_hkd", mode="sum").toPandas()
subcat_df = subcat_df.sort_values(by=["item_subcat_desc_cleaned"])
subcat_df.iloc[:, 1:] = (
    subcat_df.iloc[:, 1:]
    .apply(pd.to_numeric, errors='coerce')
    .div(subcat_df.iloc[:, 1:].sum(axis=0), axis=1)
    .mul(100)
    .round(1)
    .astype(str) + "%"
)

maincat_df = pivot_table_by_cat(group_by="maincat_desc_cleaned", agg_col="net_amt_hkd", mode="sum").toPandas()
maincat_df = maincat_df.sort_values(by=["maincat_desc_cleaned"])
maincat_df.iloc[:, 1:] = (
    maincat_df.iloc[:, 1:]
    .apply(pd.to_numeric, errors='coerce')
    .div(maincat_df.iloc[:, 1:].sum(axis=0), axis=1)
    .mul(100)
    .round(1)
    .astype(str) + "%"
)

tracker_email_body = (
    f"<b>{quarter_year} Q{quarter_no}</b><br>"
    +f"<b>from {start_date} - {end_date}</b>"
    +"<br><br><b>No. of customers:</b><br><br>"
    + persona_df.to_html(index=False)
    + "<br><br><b>Persona SOW by maincat:</b><br><br>"
    + maincat_df.to_html(index=False)
    + "<br><br><b>Persona SOW by subcat:</b><br><br>"
    + subcat_df.to_html(index=False)
)
tracker_email_subject = f"Aveda Persona - SOW in {quarter_year} Q{quarter_no}"
send_email(
    ["seanchan@lanecrawford.com", "arnabmaulik@lcjgroup.com"],
    tracker_email_subject,
    tracker_email_body,
    scope='ds-secret'
)
