-- Databricks notebook source
-- MAGIC %py
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("start_date", "")
-- MAGIC dbutils.widgets.text("end_date", "")
-- MAGIC dbutils.widgets.text("base_dir", "")
-- MAGIC

-- COMMAND ----------

-- cleaned sales_vip
CREATE
OR REPLACE TEMPORARY VIEW vip0 AS
SELECT
    DISTINCT vip_no,
    MIN(vip_main_no) vip_main_no
FROM
    imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_vip_masked
WHERE
    isnull(vip_main_no) = 0
GROUP BY
    vip_no;

CREATE
OR REPLACE TEMPORARY VIEW vip AS
SELECT
    DISTINCT *
FROM
    vip0;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW data0 AS
SELECT
    a.region_key AS region_key,
    a.shop_code AS shop_code,
    a.invoice_no AS invoice_no,
    a.item_code AS item_code,
    CAST(a.sales_date AS DATE) AS order_date,
    CAST(a.sold_qty AS INT) AS sold_qty,
    CAST(a.net_amt_hkd AS DECIMAL) AS net_amt_hkd,
    CAST(a.net_amt AS DECIMAL) AS net_amt,
    CAST(a.item_list_price AS DECIMAL) AS item_list_price,
    a.shop_brand AS shop_brand,
    a.vip_no AS vip_no,
    a.prod_brand AS prod_brand,
    a.sale_lady_id AS sale_lady_id,
    a.cashier_id AS cashier_id,
    a.customer_nat AS customer_nat,
    a.cust_nat_cat AS cust_nat_cat,
    a.customer_sex AS customer_sex,
    a.customer_age_group AS customer_age_group,
    a.void_flag AS void_flag,
    a.valid_tx_flag AS valid_tx_flag,
    a.sales_staff_flag AS sales_staff_flag,
    a.invoice_remark AS invoice_remark,
    concat(a.shop_code, '-', a.invoice_no) AS sales_main_key,
    a.sales_type
FROM
    imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_sales a;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # trim all columns
-- MAGIC from pyspark.sql.functions import trim
-- MAGIC
-- MAGIC df = spark.table("data0")
-- MAGIC for column in df.columns:
-- MAGIC     df = df.withColumn(column, trim(df[column]))
-- MAGIC
-- MAGIC df.createOrReplaceTempView("data0")

-- COMMAND ----------

-- cannot select sales_key, causing duplicated record with diff sales_key
CREATE
OR REPLACE TEMPORARY VIEW data1 AS
SELECT
    DISTINCT *
FROM
    data0
WHERE
    shop_brand IN ('BA');

-- COMMAND ----------

-- if the vip_no is not in the vip table, use the vip_no in the sale table as vip_main_no
CREATE
OR REPLACE TEMPORARY VIEW sales AS
SELECT
    a.*,
    COALESCE(b.vip_main_no, a.vip_no) vip_main_no
FROM
    data1 a
    LEFT JOIN vip b ON a.vip_no = b.vip_no;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW data2 AS
SELECT
    region_key,
    shop_code,
    invoice_no,
    item_code,
    order_date,
    sold_qty,
    net_amt_hkd,
    net_amt,
    item_list_price,
    shop_brand,
    prod_brand,
    sale_lady_id,
    cashier_id,
    customer_nat,
    cust_nat_cat,
    customer_sex,
    customer_age_group,
    void_flag,
    valid_tx_flag,
    sales_staff_flag,
    invoice_remark,
    sales_main_key,
    vip_main_no,
    vip_no,
    sales_type
FROM
    sales;
    
CREATE
OR REPLACE TEMPORARY VIEW raw_sales0 AS
SELECT
    DISTINCT *
FROM
    data2;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW raw_sales AS
SELECT
   *
FROM
  raw_sales0
where order_date >= getArgument("start_date") AND order_date <= getArgument("end_date")

-- COMMAND ----------

-- cleaned location table
CREATE
OR REPLACE TEMPORARY VIEW clean_locn0 AS
SELECT
    shop_code,
    shop_desc,
    city_district,
    CAST(modified_date AS DATE) AS modified_date
FROM
    imx_prd.imx_dw_train_silver.dbo_viw_lc_cs2k_inv_location
WHERE
    shop_brand IN ('AU', 'BA', 'BP', 'BI', 'HB', 'JB');
CREATE
OR REPLACE TEMPORARY VIEW clean_locn1 AS
SELECT
    DISTINCT *
FROM
    clean_locn0;
CREATE
OR REPLACE TEMPORARY VIEW clean_locn2 AS
SELECT
    a.shop_code,
    a.shop_desc,
    a.city_district
FROM
    clean_locn1 a
    INNER JOIN (
        SELECT
            shop_code,
            MAX(modified_date) AS MaxDateTime
        FROM
            clean_locn1
        GROUP BY
            shop_code
    ) b ON a.shop_code = b.shop_code
    AND a.modified_date = b.MaxDateTime;
CREATE
OR REPLACE TEMPORARY VIEW clean_locn3 AS
SELECT
    DISTINCT shop_code,
    CASE
        WHEN shop_code = 'BAUHKG36' THEN 'Aveda - Cityplaza 231'
        WHEN shop_code = 'BAF1D401' THEN 'Aveda-W/S-HAIR HOUSE'
        ELSE shop_desc
    END AS shop_desc,
    city_district
FROM
    clean_locn2;

-- COMMAND ----------

-- Northern TW : Hsinchu, Taipei, Taoyuan, Xinbei
-- Central TW : Taichung
-- Southern TW : Kaohsiung, Tainan
CREATE
OR REPLACE TEMPORARY VIEW clean_locn AS
SELECT
    shop_code,
    shop_desc,
    CASE
        WHEN contains(city_district, 'TW-')
        AND (
            contains(city_district, 'Hsinchu')
            OR contains(city_district, 'Taipei')
            OR contains(city_district, 'Taoyuan')
            OR contains(city_district, 'Xinbei')
        ) THEN "Northern TW"
        WHEN contains(city_district, 'TW-')
        AND (contains(city_district, 'Taichung')) THEN "Central TW"
        WHEN contains(city_district, 'TW-')
        AND (
            contains(city_district, 'Kaohsiung')
            OR contains(city_district, 'Tainan')
        ) THEN "Southern TW"
        ELSE "Others"
    END AS sub_region
FROM
    clean_locn3

-- COMMAND ----------

-- cleaned sku table
CREATE
OR REPLACE TEMPORARY VIEW clean_sku0 AS
SELECT
    --sku_key,
    item_code,
    item_desc,
    item_desc_c,
    item_cat,
    item_sub_cat,
    item_product_line,
    item_product_line_desc,
    brand_code,
    retail_price_hk,
    retail_price_tw,
    maincat_key,
    CAST(last_modified_date AS DATE) AS last_modified_date
FROM
    imx_prd.imx_dw_train_silver.dbo_viw_lc_cs2k_item_sku;
CREATE
OR REPLACE TEMPORARY VIEW clean_sku1 AS
SELECT
    DISTINCT *
FROM
    clean_sku0;

CREATE
OR REPLACE TEMPORARY VIEW clean_sku_1 AS
SELECT
    a.*
FROM
    clean_sku1 a
    INNER JOIN (
        SELECT
            item_code,
            MAX(last_modified_date) AS MaxDateTime
        FROM
            clean_sku1
        GROUP BY
            item_code
    ) b ON a.item_code = b.item_code
    AND a.last_modified_date = b.MaxDateTime

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW clean_sku AS
with cleaned_maincat AS (
  SELECT 
    DISTINCT maincat_key,
    maincat_code,
    maincat_desc
  FROM imx_prd.imx_dw_train_silver.dbo_viw_lc_cs2k_item_item_maincat
)
SELECT
    a.*,
    b.maincat_code,
    b.maincat_desc
from clean_sku_1 a
left join cleaned_maincat b using (maincat_key)

-- COMMAND ----------

-- cleaned item_subcat table
CREATE
OR REPLACE TEMPORARY VIEW clean_item_subcat0 AS
SELECT
    item_brand_key,
    item_cat,
    item_sub_cat,
    CASE
        WHEN item_subcat_desc = 'FACE-CLEANSING' THEN 'FACE CLEANSING'
        ELSE item_subcat_desc
    END AS item_subcat_desc,
    load_date
FROM
    imx_prd.imx_dw_train_silver.dbo_viw_lc_cs2k_item_item_subcat;
CREATE
OR REPLACE TEMPORARY VIEW clean_item_subcat1 AS
SELECT
    DISTINCT *
FROM
    clean_item_subcat0;
CREATE
OR REPLACE TEMPORARY VIEW clean_item_subcat2 AS
SELECT
    DISTINCT a.item_sub_cat,
    item_cat,
    a.item_brand_key,
    a.item_subcat_desc
FROM
    clean_item_subcat1 a
    INNER JOIN (
        SELECT
            item_brand_key,
            item_sub_cat,
            MAX(load_date) AS MaxDateTime
        FROM
            clean_item_subcat1
        GROUP BY
            item_brand_key,
            item_sub_cat
    ) b ON a.item_sub_cat = b.item_sub_cat
    AND a.load_date = b.MaxDateTime
    AND a.item_brand_key = b.item_brand_key
    AND item_cat NOT IN (
        "BA13",
        "BA12",
        "BA11",
        "BP10",
        "BP11",
        "BP12",
        "BP13",
        "BP14",
        "BP15",
        "BP88",
        "BP99",
        "ZZ",
        "ZZZ",
        "HB06",
        "HB07",
        "HB88"
    );
CREATE
OR REPLACE TEMPORARY VIEW clean_item_subcat AS
SELECT
    DISTINCT *
FROM
    clean_item_subcat2;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW data2_1 AS
SELECT
    a.*,
    b.item_desc,
    b.item_cat,
    b.item_sub_cat,
    b.brand_code,
    b.retail_price_hk,
    b.retail_price_tw,
    b.item_product_line_desc,
    b.maincat_desc,
    c.item_subcat_desc
FROM
    raw_sales a
    LEFT JOIN clean_sku b ON a.item_code = b.item_code
    AND a.prod_brand = b.brand_code
    LEFT JOIN clean_item_subcat c ON c.item_brand_key = a.shop_brand
    AND c.item_sub_cat = b.item_sub_cat
    AND c.item_cat = b.item_cat;

CREATE
OR REPLACE TEMPORARY VIEW data3 AS
SELECT
    DISTINCT *
FROM
    data2_1;
CREATE
OR REPLACE TEMPORARY VIEW data4 AS
SELECT
    region_key,
    b.sub_region,
    a.shop_code,
    invoice_no,
    item_code,
    order_date,
    sold_qty,
    net_amt_hkd,
    net_amt,
    item_list_price,
    shop_brand,
    prod_brand,
    sale_lady_id,
    cashier_id,
    customer_nat,
    cust_nat_cat,
    customer_sex,
    customer_age_group,
    void_flag,
    valid_tx_flag,
    sales_staff_flag,
    invoice_remark,
    sales_main_key,
    sales_type,
    vip_main_no,
    item_desc,
    item_cat,
    item_sub_cat,
    brand_code,
    retail_price_hk,
    retail_price_tw,
    item_product_line_desc,
    maincat_desc,
    item_subcat_desc,
    b.shop_desc
FROM
    data3 a
    LEFT JOIN clean_locn b ON a.shop_code = b.shop_code;

-- COMMAND ----------

-- calculate the total qty of each order, filter out those = 0
CREATE
OR REPLACE TEMPORARY VIEW qty_table AS
SELECT
    sales_main_key,
    SUM(sold_qty) AS total_qty
FROM
    data4
GROUP BY
    sales_main_key

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW data5 AS
SELECT
    a.*
FROM
    data4 a
    INNER JOIN qty_table b ON a.sales_main_key = b.sales_main_key
WHERE
    total_qty != 0

-- COMMAND ----------

-- BA HK & MO - brick store
CREATE
OR REPLACE TEMPORARY VIEW ba_hk_bs AS
SELECT
    a.*
FROM
    data5 a
WHERE
    shop_brand = 'BA'
    AND retail_price_hk > 20
    AND item_cat NOT IN ("BA13", "BA12", "BA11")
    AND sales_staff_flag = 0
    AND cashier_id = "BAEXC"
    AND region_key IN ('HK')
    AND sold_qty != 0
    AND valid_tx_flag = 1
    AND isnull(void_flag) = 1
    AND sales_type = "Product"

-- COMMAND ----------

-- BA HK - online store
CREATE
OR REPLACE TEMPORARY VIEW ba_hk_os AS
SELECT
    a.*
FROM
    data5 a
WHERE
    shop_brand = "BA"
    AND retail_price_hk > 20
    AND sold_qty != 0
    AND region_key IN ("HK")
    AND item_cat NOT IN ("BA13", "BA12", "BA11")
    AND sales_staff_flag = 0
    AND shop_code = "BAEHKG02"
    AND valid_tx_flag = 1
    AND isnull(void_flag) = 1
    AND sales_type = "Product"

-- COMMAND ----------

-- validate BA
CREATE
OR REPLACE TEMPORARY VIEW ba AS
SELECT * FROM ba_hk_bs
UNION
SELECT * FROM ba_hk_os
WHERE sales_staff_flag = 0

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW AvedaSalesVip AS
WITH cleaned_sales_vip AS (
SELECT 
    DISTINCT VIP_MAIN_NO,
    VIP_TYPE,
    VIP_ISSUE_DATE,
    VIP_SEX,
    VIP_NATION,
    VIP_AGEGRP,
    REGION_KEY,
    VIP_STAFF_FLAG,
    VIP_BA_FIRST_PUR_DATE,
    VIP_BA_VIP_TYPE
FROM
    imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_vip_masked
WHERE
    isnull(VIP_MAIN_NO) = 0
    -- AND VIP_STAFF_FLAG = 0
)
SELECT  
  VIP_MAIN_NO,
  VIP_TYPE,
  VIP_ISSUE_DATE,
  VIP_SEX,
  VIP_NATION,
  VIP_AGEGRP,
  REGION_KEY,
  VIP_STAFF_FLAG,
  VIP_BA_FIRST_PUR_DATE,
  VIP_BA_VIP_TYPE
FROM cleaned_sales_vip

-- COMMAND ----------

-- first purchase raw_sales0
CREATE
OR REPLACE TEMPORARY VIEW first_purchase AS
SELECT
    VIP_MAIN_NO,
    shop_brand,
    MIN(order_date) AS first_purchase
FROM
    raw_sales0
GROUP BY
    VIP_MAIN_NO,
    shop_brand

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW AvedaFirstLastPurchase AS
SELECT 
    DISTINCT a.VIP_MAIN_NO,
    first_pur_ba AS first_transaction_date,
    MAX(a.order_date) AS last_transaction_date
FROM
    ba a
LEFT JOIN (
    SELECT DISTINCT vip_main_no, first_pur_ba FROM imx_prd.dashboard_crm_gold.first_pur_one_time_run
) USING (VIP_MAIN_NO)
GROUP BY
    a.VIP_MAIN_NO,
    first_pur_ba

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # save
-- MAGIC import os
-- MAGIC datamart_dir = os.path.join(dbutils.widgets.get("base_dir"), "datamart")
-- MAGIC os.makedirs(datamart_dir, exist_ok=True)
-- MAGIC spark.table("ba").write.parquet(os.path.join(datamart_dir, f"transaction.parquet"), mode="overwrite")
-- MAGIC spark.table("AvedaSalesVip").write.parquet(os.path.join(datamart_dir, f"demographic.parquet"), mode="overwrite")
-- MAGIC spark.table("AvedaFirstLastPurchase").write.parquet(os.path.join(datamart_dir, f"first_last_transaction.parquet"), mode="overwrite")
