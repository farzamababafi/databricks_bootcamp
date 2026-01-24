# Databricks notebook source
destination = "databricks_bootcamp.bronze."

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling CRM Data

# COMMAND ----------

source_crm_path = "/Volumes/databricks_bootcamp/bronze/source_system/source_crm/"
crm_file_path = ["cust_info.csv","sales_details.csv", "prd_info.csv"]
for i in crm_file_path:
  df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_crm_path+i)
  df.write.mode("overwrite").saveAsTable(destination + "crm_" +i.split('.')[0])

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling ERP Data

# COMMAND ----------

source_erp_path = "/Volumes/databricks_bootcamp/bronze/source_system/source_erp/"
erp_file_path = ["CUST_AZ12.csv","LOC_A101.csv", "PX_CAT_G1V2.csv"]
for i in erp_file_path:
  df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_erp_path+i)
  df.write.mode("overwrite").saveAsTable(destination + "erp_" +i.split('.')[0])

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from databricks_bootcamp.bronze.crm_sales_details