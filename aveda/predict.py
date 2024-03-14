# Databricks notebook source
# MAGIC %py
# MAGIC dbutils.widgets.removeAll()
# MAGIC dbutils.widgets.text("start_date", "")
# MAGIC dbutils.widgets.text("end_date", "") 
# MAGIC dbutils.widgets.text("base_dir", "")

# COMMAND ----------

import os
import glob
import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import joblib

from sklearn.decomposition import FactorAnalysis
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE  

import matplotlib.pyplot as plt

# COMMAND ----------

feature_dir = os.path.join(dbutils.widgets.get("base_dir"), "features")
model_dir = os.path.join(dbutils.widgets.get("base_dir"), "model")
os.makedirs(model_dir, exist_ok=True)
files = glob.glob("/dbfs" + feature_dir + "/*.parquet")

features_df = None
for path in files:
    if "demographic" in path or "transactional" in path:
        continue
    if "tagging.parquet" not in path:
        df = spark.read.parquet(path.replace("/dbfs", ""))
        if features_df is None:
            features_df = df
        else:
            features_df = features_df.join(df, on='vip_main_no', how='inner')
    else:
        df = spark.read.parquet(path.replace("/dbfs", ""))
        features_df = features_df.join(df, on='vip_main_no', how='left')

# COMMAND ----------

features_df = features_df.fillna(0)
# manual select feature
features_to_keep = [col for col in features_df.columns if
                    col not in ['Color_SOW_by_prodline']]
features_df_filtered = features_df.select(features_to_keep)

#remove outlier
features_df_filtered = features_df_filtered.filter(f.col('vip_main_no') != '01H0329305')

# COMMAND ----------

feature_cols = [c for c in features_df_filtered.columns if c != "vip_main_no"]
all_vip = features_df_filtered.select("vip_main_no").toPandas().values.reshape(1, -1)

# COMMAND ----------

# get feature array
pandas_df = features_df_filtered.select(feature_cols).toPandas()
features_array = pandas_df.values

# COMMAND ----------

# standardization
scaler = StandardScaler()
standardized_df = scaler.fit_transform(features_array)
standardized_df = np.nan_to_num(standardized_df)

# COMMAND ----------

selected_features = [1, 0, 2, 3, 8, 4, 7, 5, 6]

# COMMAND ----------

# MAGIC %md
# MAGIC selected feature:
# MAGIC ```python
# MAGIC array(['Body_SOW_by_maincat', 'Skin_SOW_by_maincat',
# MAGIC        'Botanical Repair_SOW_by_prodline', 'Invati_SOW_by_prodline',
# MAGIC        'Treatment_SOW_by_subcat', 'Others_SOW_by_prodline',
# MAGIC        'Style_SOW_by_subcat', 'Conditioner_SOW_by_subcat',
# MAGIC        'Shampoo_SOW_by_subcat'], dtype='<U32')
# MAGIC ```

# COMMAND ----------

features_embed = standardized_df[:, selected_features]

# COMMAND ----------

import joblib
from sklearn.cluster import KMeans
kmeans = joblib.load("/dbfs/mnt/prd/customer_segmentation/imx/aveda/train/model/kmeans_model6.pkl")
cluster_assignment = kmeans.predict(features_embed)

# COMMAND ----------

result_df = pd.DataFrame(np.concatenate((all_vip.reshape(-1, 1), cluster_assignment.reshape(-1, 1)), axis=1),
                         columns=["vip_main_no", "persona"])
spark.createDataFrame(result_df).write.parquet(os.path.join(model_dir, "clustering_result.parquet"))
