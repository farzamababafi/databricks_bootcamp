# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

RENAME_MAP = {
    "sls_ord_num": "order_number",
    "sls_prd_key": "product_key",
    "sls_cust_id": "customer_id",
    "sls_order_dt": "order_date",
    "sls_ship_dt": "ship_date",
    "sls_due_dt": "due_date",
    "sls_sales": "sales_amount",
    "sls_quantity": "quantity",
    "sls_price": "price",
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading From Bronze Table

# COMMAND ----------

df = spark.table("databricks_bootcamp.bronze.crm_sales_details")

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Trimming

# COMMAND ----------

for field in df.schema.fields:
  if isinstance(field.dataType, StringType):
      df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Date 

# COMMAND ----------


df = (
    df
    .withColumn(
        "sls_order_dt",
        F.when(
            (col("sls_order_dt") == 0) | (length(col("sls_order_dt")) != 8),
            None
        ).otherwise(F.to_date(col("sls_order_dt").cast("string"), "yyyyMMdd"))
    )
    .withColumn(
        "sls_ship_dt",
        F.when(
            (col("sls_ship_dt") == 0) | (length(col("sls_ship_dt")) != 8),
            None
        ).otherwise(F.to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd"))
    )
    .withColumn(
        "sls_due_dt",
        F.when(
            (col("sls_due_dt") == 0) | (length(col("sls_due_dt")) != 8),
            None
        ).otherwise(F.to_date(col("sls_due_dt").cast("string"), "yyyyMMdd"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Fixing

# COMMAND ----------

df = (
    df
    .withColumn(
        "sls_price",
        F.when(
            (col("sls_price").isNull()) | (col("sls_price") <= 0),
            F.when(
                col("sls_quantity") != 0,
                col("sls_sales") / col("sls_quantity")
            ).otherwise(None)
        ).otherwise(col("sls_price"))
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming The Columns

# COMMAND ----------

for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Into Silver Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.silver.crm_sales")