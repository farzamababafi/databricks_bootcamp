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
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key,
    ci.customer_id,
    ci.customer_key as customer_number,
    ci.first_name,
    ci.last_name,
    la.country,
    ci.marital_status,
    CASE
        WHEN ci.gender <> 'n/a' THEN ci.gender
        ELSE COALESCE(ca.gender, 'n/a')
    END AS gender,
    ca.birth_date AS birthdate,
    ci.created_date AS create_date
FROM databricks_bootcamp.silver.crm_customers as ci
LEFT JOIN databricks_bootcamp.silver.erp_customers ca
    ON ci.customer_key = ca.customer_number
LEFT JOIN databricks_bootcamp.silver.erp_customer_location la
    ON ci.customer_key = la.customer_number
"""
df = spark.sql(query)


# COMMAND ----------

# MAGIC %md
# MAGIC # Write It To Gold

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.gold.dim_customers")
