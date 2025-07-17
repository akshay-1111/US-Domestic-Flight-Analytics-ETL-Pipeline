# ✈US-Domestic-Flight-Analytics-ETL-Pipeline

This project demonstrates a batch ETL pipeline that ingests and analyzes US domestic flight data using AWS services like Glue, S3, Redshift, and orchestrates it using Apache Airflow. The output includes interactive charts visualizing delay metrics generated with Plotly and stored in S3.

## 📊 Architecture Overview

- **AWS Glue Crawler**
  - Crawls raw and transformed flight data in S3
  - Creates/updates Glue Data Catalog tables

- **AWS Glue Job**
  - Transforms raw CSV flight data into cleaned Parquet format
  - Writes to S3 under `processed/` path

- **Amazon Redshift**
  - Loads Parquet data from S3 using the COPY command
  - SQL views are created for analysis

- **Apache Airflow**
  - Orchestrates the pipeline end-to-end: crawling, transforming, loading, charting, and uploading

- **Plotly + S3**
  - Generates interactive delay analysis charts
  - Uploads them to S3 for access

## ⚙️ Technologies Used

- AWS Glue (Crawler + ETL Job)
- Amazon S3 (Storage of raw and processed data)
- Amazon Redshift (Data warehouse and analysis)
- Apache Airflow (Orchestration)
- Plotly + Pandas (Data visualization)
-Python (Core programming language)

## 🧩 Airflow DAG Tasks

1. `run_raw_crawler`: Runs Glue Crawler on `raw_data/` in S3
2. `run_glue_job`: Executes AWS Glue ETL job to process flight data
3. `run_processed_crawler`: Runs Glue Crawler on `processed/` S3 path
4. `copy_to_redshift`: Loads Parquet files into Redshift from S3
5. `create_views`: Creates Redshift views for analytics
6. `generate_plotly_chart`: Queries Redshift and creates three interactive charts
7. `upload_chart_to_s3`: Uploads the HTML chart output to S3

## ✅ Prerequisites

- AWS account with access to Glue, S3, Redshift
- Redshift cluster with public access or correct VPC setup
- IAM Role with policies for Glue, S3, and Redshift access
- Airflow environment (Docker/Astro CLI setup)
