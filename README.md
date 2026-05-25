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

~~~
Landing Zone (S3)
│
├── customer_landing (956 rows)
├── accelerometer_landing (81,273 rows)
└── step_trainer_landing (28,680 rows)
              │
              ▼
Trusted Zone (S3 + Glue Catalog)
│
├── customer_trusted (482 rows) ← filtered for research opt-in
├── accelerometer_trusted (40,981 rows) ← joined with consenting customers
└── step_trainer_trusted (14,460 rows) ← only data from curated customers
              │
              ▼
Curated Zone (S3 + Glue Catalog)
│
├── customer_curated (482 rows) ← customers with accelerometer data
└── machine_learning_curated (43,681 rows) ← final ML training dataset
~~~

## Row Counts (Verified)

| Zone | Table | Rows |
|------|-------|------|
| Landing | customer_landing | 956 |
| Landing | accelerometer_landing | 81,273 |
| Landing | step_trainer_landing | 28,680 |
| Trusted | customer_trusted | 482 |
| Trusted | accelerometer_trusted | 40,981 |
| Trusted | step_trainer_trusted | 14,460 |
| Curated | customer_curated | 482 |
| Curated | machine_learning_curated | 43,681 |

## How to Run

1. Upload data files to S3 buckets organized into landing folders
2. Run SQL DDL scripts in Athena to create landing zone tables
3. Run Glue jobs in this order:
   1. `customer_landing_to_trusted.py`
   2. `accelerometer_landing_to_trusted.py`
   3. `customer_trusted_to_curated.py`
   4. `step_trainer_trusted.py`
   5. `machine_learning_curated.py`
4. Validate row counts using Athena queries
