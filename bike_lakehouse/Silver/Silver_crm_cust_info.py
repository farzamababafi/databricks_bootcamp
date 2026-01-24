# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

RENAME_MAP = {
    "cst_id": "customer_id",
    "cst_key": "customer_key",
    "cst_firstname": "first_name",
    "cst_lastname": "last_name",
    "cst_marital_status": "marital_status",
    "cst_gndr": "gender",
    "cst_create_date": "created_date",
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading From Bronze Table

# COMMAND ----------

df = spark.table("databricks_bootcamp.bronze.crm_cust_info")

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

df = df.withColumn("cst_marital_status", F.when(F.upper(col("cst_marital_status")) == "M", "Married")
                                          .when(F.upper(col("cst_marital_status")) == "S", "Single")
                                          .otherwise("n/a"))
df = df.withColumn("cst_gndr", F.when(F.upper(col("cst_gndr")) == "F", "Female")
                                          .when(F.upper(col("cst_gndr")) == "M", "Male")
                                          .otherwise("n/a"))


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

df.write.mode("overwrite").format("delta").saveAsTable("databricks_bootcamp.silver.crm_customers ")