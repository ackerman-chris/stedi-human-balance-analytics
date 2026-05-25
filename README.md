# STEDI Human Balance Analytics

## Project Overview
A data lakehouse solution built on AWS for the STEDI team to process sensor data 
from their Step Trainer devices and mobile app accelerometer readings. The 
pipeline curates data for machine learning model training while respecting 
customer privacy preferences (only including customers who opted into research).

## Architecture
Three-tier medallion architecture:
- **Landing Zone**: Raw JSON data from sensors and mobile app
- **Trusted Zone**: Filtered to include only customers who opted into research
- **Curated Zone**: Final ML-ready datasets joining customer, accelerometer, 
  and step trainer data

## Technologies Used
- AWS Glue (PySpark) — ETL transformations
- AWS S3 — Data lake storage
- AWS Athena — SQL queries and validation
- Python 3 / Apache Spark
- AWS IAM — Security and access control

## Project Structure
- `sql/` — DDL scripts for creating landing zone Glue tables in Athena
- `python/` — PySpark Glue job scripts for ETL transformations
- `screenshots/` — Athena query result screenshots

## Pipeline Flow
