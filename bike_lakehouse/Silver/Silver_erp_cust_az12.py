# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

RENAME_MAP = {
    "cid": "customer_number",
    "bdate": "birth_date",
    "gen": "gender"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading From Bronze Table

# COMMAND ----------

df = spark.table("databricks_bootcamp.bronze.erp_cust_az12")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select SUBSTRING(CID, 1, 5) from databricks_bootcamp.bronze.erp_cust_az12 group by SUBSTRING(CID, 1, 5);

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


df = df.withColumn(
    "cid",
    F.when(col("cid").startswith("NAS"),
           F.substring(col("cid"), 4, F.length(col("cid"))))
     .otherwise(col("cid"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Birthdate Validation

# COMMAND ----------

df = df.withColumn(
    "bdate",
    F.when(col("bdate") > F.current_date(), None)
     .otherwise(col("bdate"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gender Normalization

# COMMAND ----------

df = df.withColumn(
    "gen",
    F.when(F.upper(col("gen")).isin("F", "FEMALE"), "Female")
     .when(F.upper(col("gen")).isin("M", "MALE"), "Male")
     .otherwise("n/a")
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

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.silver.erp_customers")