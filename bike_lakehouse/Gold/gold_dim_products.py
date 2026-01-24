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
    ROW_NUMBER() OVER (ORDER BY pn.start_date, pn.product_number) AS product_key, -- Surrogate key
    pn.product_id,
    pn.product_number,
    pn.product_name,
    pn.category_id,
    pc.category,
    pc.subcategory,
    pc.maintenance_flag,
    pn.product_line,
    pn.start_date
FROM databricks_bootcamp.silver.crm_products pn
LEFT JOIN databricks_bootcamp.silver.erp_product_category pc
    ON pn.category_id = pc.category_id
--WHERE pn.end_date IS NULL; -- Filter out all historical data
"""
df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write It To Gold

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.gold.dim_products")
