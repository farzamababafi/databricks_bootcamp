# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

RENAME_MAP = {
    "prd_id": "product_id",
    "cat_id": "category_id",
    "prd_key": "product_number",
    "prd_nm": "product_name",
    "prd_cost": "product_cost",
    "prd_line": "product_line",
    "prd_start_dt": "start_date",
    "prd_end_dt": "end_date"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading From Bronze Table

# COMMAND ----------

df = spark.table("databricks_bootcamp.bronze.crm_prd_info")
df.display()

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
# MAGIC ### Product Key Parsing

# COMMAND ----------

df = df.withColumn("cat_id", F.regexp_replace(F.substring(col("prd_key"), 1, 5), "-", "_"))
df = df.withColumn("prd_key", F.substring(col("prd_key"), 7, F.length(col("prd_key"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost Cleanup

# COMMAND ----------

df = df.withColumn("prd_cost", F.coalesce(col("prd_cost"), F.lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Line Normalization

# COMMAND ----------


df = (
    df
    # Normalize product line
    .withColumn(
        "prd_line",
        F.when(F.upper(col("prd_line")) == "M", "Mountain")
         .when(F.upper(col("prd_line")) == "R", "Road")
         .when(F.upper(col("prd_line")) == "S", "Other Sales")
         .when(F.upper(col("prd_line")) == "T", "Touring")
         .otherwise("n/a")
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

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.silver.crm_products")