# Databricks notebook source
# aveda this year
dbutils.notebook.run(
    "./aveda/aveda_value_segment",
    0,
    {
        "first_date": 20201001,
        "last_date": 20230930,
        "last_year": "0",
        "p12m_first_date": 20221001,
    },
)

# COMMAND ----------

# aveda last year
dbutils.notebook.run(
    "./aveda/aveda_value_segment",
    0,
    {
        "first_date": 20191001,
        "last_date": 20220930,
        "last_year": "1",
        "p12m_first_date": 20211001,
    },
)

# COMMAND ----------

dbutils.notebook.run(
    "./reclassify_segment",
    0,
    {"brand": "aveda"},
)

# COMMAND ----------

# apivita hk this year
dbutils.notebook.run(
    "./apivita/apivita_value_segment_hk",
    0,
    {
        "first_date": 20201001,
        "last_date": 20230930,
        "last_year": "0",
        "p12m_first_date": 20221001,
    },
)

# COMMAND ----------

# apivita hk last year
dbutils.notebook.run(
    "./apivita/apivita_value_segment_hk",
    0,
    {
        "first_date": 20191001,
        "last_date": 20220930,
        "last_year": "1",
        "p12m_first_date": 20211001,
    },
)

# COMMAND ----------

dbutils.notebook.run(
    "./reclassify_segment",
    0,
    {
        "brand": "apivita",
        "region": "hk"
    },
)

# COMMAND ----------

# apivita tw this year
dbutils.notebook.run(
    "./apivita/apivita_value_segment_tw",
    0,
    {
        "first_date": 20201001,
        "last_date": 20230930,
        "last_year": "0",
        "p12m_first_date": 20221001,
    },
)

# COMMAND ----------

# apivita tw last year
dbutils.notebook.run(
    "./apivita/apivita_value_segment_tw",
    0,
    {
        "first_date": 20191001,
        "last_date": 20220930,
        "last_year": "1",
        "p12m_first_date": 20211001,
    },
)

# COMMAND ----------

dbutils.notebook.run(
    "./reclassify_segment",
    0,
    {
        "brand": "apivita",
        "region": "tw"
    },
)

# COMMAND ----------


