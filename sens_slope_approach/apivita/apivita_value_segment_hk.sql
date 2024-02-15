-- Databricks notebook source
-- MAGIC %py
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("p12m_first_date", "")
-- MAGIC dbutils.widgets.text("first_date", "")
-- MAGIC dbutils.widgets.text("last_date", "")
-- MAGIC dbutils.widgets.dropdown("last_year", "0", ["1", "0"])

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import os
-- MAGIC
-- MAGIC base_dir = "/mnt/dev/customer_segmentation/imx/apivita/datamart"
-- MAGIC
-- MAGIC if getArgument("last_year") == "1":
-- MAGIC     postfix = "_last_year"
-- MAGIC else:
-- MAGIC     postfix = ""
-- MAGIC     
-- MAGIC spark.read.parquet(
-- MAGIC     os.path.join(base_dir, f"transaction{postfix}.parquet")
-- MAGIC ).createOrReplaceTempView("AvedaSalesProduct")
-- MAGIC spark.read.parquet(
-- MAGIC     os.path.join(base_dir, f"demographic{postfix}.parquet")
-- MAGIC ).createOrReplaceTempView("SalesVip")
-- MAGIC spark.read.parquet(
-- MAGIC     os.path.join(base_dir, f"first_last_transaction{postfix}.parquet")
-- MAGIC ).createOrReplaceTempView("FirstLastPurchase")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from scipy import stats
-- MAGIC from pyspark.sql.types import FloatType, ArrayType
-- MAGIC  
-- MAGIC
-- MAGIC def compute_stat(l):
-- MAGIC     result = stats.theilslopes(l,alpha=0.95)
-- MAGIC     return list([float(result[0]), float(result[1])])
-- MAGIC
-- MAGIC
-- MAGIC spark.udf.register("COMPUTE_SLOPE", compute_stat, ArrayType(FloatType()))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from datetime import date
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC this_year = date.today().year
-- MAGIC this_month = date.today().month
-- MAGIC
-- MAGIC yyyymm = ["%d%.2d" % (i, j) for j in range(1, 13) for i in range(this_year - 3, this_year)]
-- MAGIC yyyymm += ["%d%.2d" % (this_year, j) for j in range(1, this_month + 1)]
-- MAGIC
-- MAGIC df = pd.DataFrame(yyyymm, columns=["yyyymm"])
-- MAGIC spark.createDataFrame(df).createOrReplaceTempView("yearmon")

-- COMMAND ----------

--- getting revenue by month for CLV
create
or replace temporary view amt_by_vip_and_month as
select
  vip_main_no,
  CONCAT(year(order_date), LPAD(month(order_date), 2, '0')) as yyyymm,
  sum(net_amt_hkd) as rev,
  count(distinct invoice_no) as tran_no
from
  (SELECT * FROM AvedaSalesProduct WHERE region_key = "HK")
group by
  year(order_date),
  month(order_date),
  vip_main_no;

-- COMMAND ----------

---monthly clv for segmentation
create
or replace temp view p12m_clv_monthly_view as 
with customer_yearmon as (
  select
    distinct vip_main_no,
    b.yyyymm
  from
    amt_by_vip_and_month a
    CROSS JOIN yearmon b
),
p12m_value as(
  select
    a.vip_main_no,
    a.yyyymm,
    rev
  from
    customer_yearmon a
    left join amt_by_vip_and_month b on (a.vip_main_no = b.vip_main_no)
    and (a.yyyymm = b.yyyymm)
)
select
  vip_main_no,
  yyyymm,
  case
    when rev is null then 0
    else rev
  end as monthly_clv
from
  p12m_value
where yyyymm >= int(CONCAT(year(TO_DATE(getArgument("p12m_first_date"), "yyyyMMdd")), LPAD(month(TO_DATE(getArgument("p12m_first_date"), "yyyyMMdd")), 2, '0')))
and yyyymm <= int(CONCAT(year(TO_DATE(getArgument("last_date"), "yyyyMMdd")), LPAD(month(TO_DATE(getArgument("last_date"), "yyyyMMdd")), 2, '0')))

-- COMMAND ----------

-- getting information to start calculating value segments
create
or replace temporary view total_clv_pre as
Select
  distinct b.vip_main_no,
  a.first_transaction_date,
  last_transaction_date,
  round(
    datediff(
      TO_DATE(getArgument("last_date"), "yyyyMMdd"),
      first_transaction_date
    ) / 365,
    0
  ) as tenure,
  case
    when a.first_transaction_date >= TO_DATE(getArgument('p12m_first_date'), "yyyyMMdd") 
    and a.first_transaction_date <= TO_DATE(getArgument('last_date'), "yyyyMMdd") then 'New Joiner'
    when a.first_transaction_date < TO_DATE(getArgument('p12m_first_date'), "yyyyMMdd") then "old member"
    else null
  end as new_joiner_flag,
  case
    when last_transaction_date > date_add(TO_DATE(getArgument("last_date"), "yyyyMMdd"), -730)
    and last_transaction_date < TO_DATE(getArgument('p12m_first_date'), "yyyyMMdd") then 'Inactive P12'
    when last_transaction_date <= date_add(TO_DATE(getArgument("last_date"), "yyyyMMdd"), -730) then 'Inactive P24'
    else 'active_in_p12m'
  end as inactive_flag,
  sum(b.rev) as total_clv
from
  FirstLastPurchase a
  inner join amt_by_vip_and_month b on a.vip_main_no = b.vip_main_no
group by
  1,
  2,
  3,
  4,
  5,
  6;
  
create
  or replace temporary view total_clv as
select
  *,
  case
    when tenure >= 3 then total_clv / 3
    else total_clv / tenure
  end as avg_annual_revenue
from
  total_clv_pre
where isnull(new_joiner_flag) = 0;

-- COMMAND ----------

--- existing members p12m clv
create
or replace temporary view p12m_existing_member_clv as
Select
  a.vip_main_no,
  new_joiner_flag,
  inactive_flag,
  a.last_transaction_date,
  sum(rev) as p12m_clv
from
  total_clv a
  inner join amt_by_vip_and_month b on a.vip_main_no = b.vip_main_no
where
  yyyymm > (substring(getArgument("last_date"), 1, 6) -100) -- P12M
  and new_joiner_flag = 'old member'
group by
  1,
  2,
  3,
  4;

-- COMMAND ----------

---putting into monthly format
create
or replace temporary view existing_member_p12m_clv_monthly as
Select
  vip_main_no,
  yyyymm,
  monthly_clv
from
  p12m_clv_monthly_view
where
  vip_main_no in (
    select
      distinct vip_main_no
    from
      p12m_existing_member_clv
  );

-- COMMAND ----------

create
or replace temporary view sen_slope AS
with sens_slope_0 AS (
  SELECT
    vip_main_no,
    yyyymm,
    monthly_clv,
    sum(monthly_clv) OVER (
      partition by vip_main_no
      ORDER BY
        yyyymm
    ) as cumsum
  FROM
    existing_member_p12m_clv_monthly
  ORDER BY
    vip_main_no,
    yyyymm
),
sens_slope_2 AS (
  SELECT
    vip_main_no,
    collect_list(cast(cumsum as float)) as l
  FROM
    sens_slope_0
  GROUP BY
    vip_main_no
),
sens_slope_3 AS (
  SELECT
    vip_main_no,
    COMPUTE_SLOPE(l) as result
  FROM
    sens_slope_2
),
sens_slope AS (
  SELECT
    vip_main_no,
    result [0] as slope,
    result [1] as intercept
  FROM
    sens_slope_3
),
---converting the slope into H,M,L buckets based on the percentile distribution
sen_slope_final_1 as (
  select
    vip_main_no,
    slope,
    intercept,CASE
      WHEN slope >= 0
      and intercept <> 0 then slope +(abs(intercept) / 2)
      else slope
    end as slope_final
  from
    sens_slope
)
select
  a.vip_main_no,
  slope,
  intercept,
  slope_final
from
  sen_slope_final_1 a
  inner join p12m_existing_member_clv b on a.vip_main_no = b.vip_main_no

-- COMMAND ----------

-- SELECT percentile_approx(slope_final, .9, 100) AS percentile_cutoff FROM sen_slope

-- COMMAND ----------

-- SELECT percentile_approx(slope_final, .7, 100) AS percentile_cutoff FROM sen_slope

-- COMMAND ----------

create
or replace temporary view sen_slope_final AS
select
  vip_main_no,
  slope,
  intercept,
  slope_final,
  case
    when slope_final > 1050 then 'H'
    when slope_final >= 540
    and slope_final <= 1050 then 'M'
    else 'L'
  end as slope_flag
from sen_slope

-- COMMAND ----------

create
or replace temporary view overall_clv0 as 
  select
  distinct a.vip_main_no,
  first_transaction_date,
  avg_annual_revenue,
  new_joiner_flag,
  inactive_flag,
  tenure
from
  total_clv a
  inner join amt_by_vip_and_month b on a.vip_main_no = b.vip_main_no

-- COMMAND ----------

-- SELECT percentile_approx(avg_annual_revenue, .9, 100) AS percentile_cutoff FROM overall_clv0

-- COMMAND ----------

-- SELECT percentile_approx(avg_annual_revenue, .7, 100) AS percentile_cutoff FROM overall_clv0

-- COMMAND ----------

---converting the average CLV into H,M,L buckets based on the percentile distribution
create
or replace temporary view overall_clv as
Select
  distinct vip_main_no,
  first_transaction_date,
  avg_annual_revenue,
  new_joiner_flag,
  inactive_flag,
  tenure,
  case
    when avg_annual_revenue > 2400 then 'H'
    when avg_annual_revenue >= 1100
    and avg_annual_revenue <= 2400 then 'M'
    else 'L'
  end as flag_total_clv
from
  overall_clv0
where
  new_joiner_flag = 'old member'
  and inactive_flag = 'active_in_p12m';

-- COMMAND ----------

/**creating additional thresholds of 12500 p12m_clv for enaged and 3500 for emerging so that they cross 75% percentile and 50% percentile in spending in p12m threshold***/
create
or replace temporary view value_segment as with vs0 as (
  select
    a.vip_main_no,
    b.first_transaction_date,
    b.avg_annual_revenue,
    b.new_joiner_flag,
    b.inactive_flag,
    p12m_clv,
    tenure,
    flag_total_clv,
    slope_flag
  from
    sen_slope_final a
    left join overall_clv b on a.vip_main_no = b.vip_main_no
    left join p12m_existing_member_clv c on a.vip_main_no = c.vip_main_no
)
select
  vip_main_no,
  first_transaction_date,
  avg_annual_revenue,
  new_joiner_flag,
  inactive_flag,
  p12m_clv,
  tenure,
  /***low value***/
  case
    when (flag_total_clv = 'L') then 'Low Value'
    /***engaged.The p12m_clv value is for 75% percentile cutoff ***/
    when (
      flag_total_clv = 'H'
      and slope_flag = 'H'
      and p12m_clv >= 6900
    )
    or (
      flag_total_clv = 'H'
      and slope_flag = 'M'
      and p12m_clv >= 6900
    ) then 'Engaged'
    /***Emerging. Moved some borderline cases to At risk***/
    when (
      flag_total_clv = 'M'
      and (
        slope_flag = 'H'
        or slope_flag = 'M'
      )
      and p12m_clv > 3300 -- test
    )
     or (
      flag_total_clv = 'H'
      and slope_flag = 'M'
      and (
        p12m_clv >= 3300
        and p12m_clv < 6900
      )
    )
    or (
      flag_total_clv = 'H'
      and slope_flag = 'H'
      and  (p12m_clv >= 3300 and p12m_clv < 6900
    ))
     then 'Emerging'
    /*** At risk***/
    when 
    (
      flag_total_clv = 'M'
      and slope_flag = 'L'
    )
    or (
      flag_total_clv = 'H'
      and slope_flag = 'L'
    )
    or (
      flag_total_clv = 'M'
      and (
        slope_flag = 'H'
        or slope_flag = 'M'
        and p12m_clv <= 3300
      )
      or (
        flag_total_clv = 'H'
        and slope_flag = 'M'
        and p12m_clv < 3300
      )
      or (
      flag_total_clv = 'H'
      and slope_flag = 'H'
      and  p12m_clv <= 3300)
      
    ) then 'At Risk'
    else ''
  end as value_segment
from
  vs0

-- COMMAND ----------

select distinct value_segment,max(p12m_clv), min(p12m_clv),max(avg_annual_revenue),min(avg_annual_revenue), avg(avg_annual_revenue), avg(p12m_clv),count(distinct vip_main_no) from value_segment group by 1

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.table("value_segment").write.parquet(os.path.join(base_dir, f"value_segment{postfix}_hk.parquet"), mode="overwrite")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.table("total_clv").write.parquet(os.path.join(base_dir, f"total_clv{postfix}_hk.parquet"), mode="overwrite")

-- COMMAND ----------


