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

# remove features with 85% zero
features_df = features_df.fillna(0)
zero_percentage_threshold = 1

zero_features = []

total_rows = features_df.count()
for column_name in features_df.columns:
    zero_count = features_df.filter(f.col(column_name) == 0).count()
    zero_percentage = zero_count / total_rows

    if zero_percentage >= zero_percentage_threshold:
        zero_features.append(column_name)

zero_features.append('Color_SOW_by_prodline')
print(zero_features, len(zero_features))

# COMMAND ----------

# manual select feature
features_to_keep = [col for col in features_df.columns if
                    col not in zero_features]
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

# FactorAnalysis
n_components = 10

fa = FactorAnalysis(n_components=n_components)
fa.fit(standardized_df)

factor_loadings = fa.components_
selected_features = np.abs(factor_loadings).sum(axis=0).argsort()[:n_components]

# COMMAND ----------

np.array(feature_cols)[np.array(selected_features)]

# COMMAND ----------

factor_table = pd.DataFrame(factor_loadings.T, index=np.array(feature_cols))
features_embed = standardized_df[:, selected_features]

# COMMAND ----------

import joblib
from sklearn.cluster import KMeans
kmeans = joblib.load("/dbfs/mnt/dev/customer_segmentation/imx/aveda/2023/model/kmeans_model6.pkl")
cluster_assignment = kmeans.predict(features_embed)

# COMMAND ----------

result_df = pd.DataFrame(np.concatenate((all_vip.reshape(-1, 1), cluster_assignment.reshape(-1, 1)), axis=1),
                         columns=["vip_main_no", "persona"])
spark.createDataFrame(result_df).write.parquet(os.path.join(model_dir, "clustering_result.parquet"))
