# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

RENAME_MAP = {
    "cid": "customer_number",
    "cntry": "country"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading From Bronze Table

# COMMAND ----------

df = spark.table("databricks_bootcamp.bronze.erp_loc_a101")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select CNTRY from databricks_bootcamp.bronze.erp_loc_a101 group by CNTRY

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
# MAGIC ### Customer ID Cleanup

# COMMAND ----------


df = df.withColumn("cid", F.regexp_replace(col("cid"), "-", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Country Normalization

# COMMAND ----------


df = df.withColumn(
    "cntry",
    F.when(col("cntry") == "DE", "Germany")
     .when(col("cntry").isin("US", "USA"), "United States")
     .when((col("cntry") == "") | col("cntry").isNull(), "n/a")
     .otherwise(col("cntry"))
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

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.silver.erp_customer_location")