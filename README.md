# Bike Lakehouse ETL Project

This project implements a Databricks-based lakehouse pipeline for bike sales and customer data. It is organized into Bronze, Silver, and Gold layers to demonstrate data ingestion, cleansing, transformation, and analytical modeling.

## Project Structure

- `bike_lakehouse/Bronze.py`
  - Ingests CRM and ERP raw CSV files from source directories.
  - Writes each raw dataset into Delta-style Databricks tables under `databricks_bootcamp.bronze`.

- `bike_lakehouse/Silver/`
  - Contains Silver-layer transformations for CRM and ERP datasets.
  - Example: `Silver_crm_cust_info.py` reads raw CRM customer info, cleans and normalizes text fields, renames columns, and writes into `databricks_bootcamp.silver.crm_customers`.
  - Silver scripts typically shape raw data for use in business models.

- `bike_lakehouse/Gold/`
  - Contains Gold-layer business models and dimensional tables.
  - Example: `gold_dim_customers.py` builds a customer dimension by joining Silver CRM and ERP customer records.
  - `gold_fact_sales.py` assembles a sales fact table by joining Silver sales data with Gold dimension tables.

## Key Concepts

- Bronze layer: raw ingestion of source CSV files into Databricks tables.
- Silver layer: cleansing, normalization, and schema refinement.
- Gold layer: business-ready dimension and fact tables for analytics.

## How to Run

1. Open the project in Databricks or a notebook environment with Spark configured.
2. Run `bike_lakehouse/Bronze.py` to ingest raw data into Bronze tables.
3. Run Silver scripts in `bike_lakehouse/Silver/` to transform and store Silver tables.
4. Run Gold scripts in `bike_lakehouse/Gold/` to generate dimensional and fact models.

## Notes

- The scripts use Spark and Databricks SQL table operations.
- The Bronze ingestion path is currently configured for a mounted volume path such as `/Volumes/databricks_bootcamp/bronze/source_system/`.
- Generated tables are written into the `databricks_bootcamp` database namespace.

## Recommended Improvements

- Add a dedicated config file for source paths, database names, and storage formats.
- Make the pipeline executable end-to-end with a single orchestrator script or notebook.
- Add documentation for source file naming conventions and sample data schema.
