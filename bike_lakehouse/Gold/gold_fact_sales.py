# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Read From Silver Table's

# COMMAND ----------

# MAGIC %md
# MAGIC # Business Transformation And Modeling

# COMMAND ----------

query = """
SELECT
    sd.order_number,
    pr.product_key,
    cu.customer_key,
    sd.order_date,
    sd.ship_date,
    sd.due_date,
    sd.sales_amount,
    sd.quantity,
    sd.price
FROM databricks_bootcamp.silver.crm_sales sd
LEFT JOIN databricks_bootcamp.gold.dim_products pr
    ON sd.product_key = pr.product_number
LEFT JOIN databricks_bootcamp.gold.dim_customers cu
    ON sd.customer_id = cu.customer_id;
"""
df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write It To Gold

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.gold.fact_sales")
