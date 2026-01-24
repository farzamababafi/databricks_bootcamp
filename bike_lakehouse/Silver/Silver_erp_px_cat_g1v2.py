# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

RENAME_MAP = {
    "id": "category_id",
    "cat": "category",
    "subcat": "subcategory",
    "maintenance": "maintenance_flag"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading From Bronze Table

# COMMAND ----------

df = spark.table("databricks_bootcamp.bronze.erp_px_cat_g1v2")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select CAT, SUBCAT from databricks_bootcamp.bronze.erp_px_cat_g1v2 group by CAT, SUBCAT

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
# MAGIC ### Normalize Maintenance Flag to Boolean

# COMMAND ----------



df = df.withColumn(
    "maintenance",
    F.when(F.upper(col("maintenance")) == "YES", F.lit(True))
     .when(F.upper(col("maintenance")) == "NO", F.lit(False))
     .otherwise(None)
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

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.silver.erp_product_category")