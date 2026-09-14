# AWS Telecom Data Pipeline

**Apache Spark | Databricks | AWS MWAA | EMR Serverless | S3 | Redshift Serverless | Delta Lake | ETL**

End-to-End Cloud Data Engineering project that processes telecom network performance measurements using Apache Spark, with two implementations of the same pipeline:

- a Databricks implementation using PySpark, Delta Lake, Unity Catalog and Databricks Jobs
- an AWS-based pipeline using Amazon EMR Serverless and Apache Airflow on Amazon MWAA


Both implementations follow the Medallion Data Lake Architecture (Bronze → Silver → Gold), transforming raw telecom measurements into analytics-ready datasets.

---

# Project Overview

Modern telecommunication systems generate large volumes of network performance measurements from mobile devices and infrastructure equipment.

Raw operational data requires automated processing before it can be used for:

* network performance analytics
* reporting
* dashboards
* analytical applications

The pipeline is built using commonly used technologies and patterns from modern Data Engineering environments.

---

# Business Problem

Raw telecom measurements often contain missing values, duplicated records or inconsistent formats. Network analysts require reliable, structured and optimized datasets that can support analytical workloads. The goal of this project is to automate the transformation of raw telecom data into analytics-ready datasets.

---

## Bronze Layer - Raw Data Storage

The dataset is synthetically generated.

The Bronze layer stores the original raw telecom network measurements before any transformations.

The dataset contains attributes such as device model, network type, download speed, upload speed, latency, signal strength, GPS coordinates and timestamps.

The raw data is stored in Amazon S3 in the AWS implementation and in a Databricks Volume in the Databricks implementation.

## Silver Layer - Data Cleaning and Transformation

The Silver layer contains cleaned and transformed telecom measurements prepared for further analysis.

Transformations might include duplicate removal, missing value handling, schema validation, datatype standardization and feature creation.

The AWS implementation stores the Silver layer as Parquet files, while the Databricks implementation stores it as a Delta table.

## Data Quality Validation

Before creating analytical datasets, the pipeline performs validation checks on the Silver layer.

The validation process helps ensure that the processed data is suitable for downstream analytical workloads.

The AWS implementation performs checks including missing value validation, schema validation, value range validation and data consistency checks.

The Databricks implementation validates that the Silver layer contains data before continuing to the Gold transformation.

## Gold Layer - Analytics Ready Data

The Gold layer contains aggregated datasets prepared for analytical workloads.

Current datasets:

```
gold/

├── city_summary/
├── device_summary/
└── network_summary/
```

The aggregations include metrics such as average download speed, average upload speed, average latency, average signal strength and measurement counts.

---

# Databricks Implementation

Before running the Databricks implementation, Databricks must be configured with the required AWS permissions. This includes setting up the appropriate AWS IAM role and permissions to allow Databricks to securely access the required AWS resources.

The project contains two implementations of the Bronze → Silver → Gold data pipeline.

```
                    Raw Telecom Data (CSV)
                              │
                              ▼
                    Databricks Volume
                              │
                              ▼
                    Bronze Delta Table
                              │
                              ▼
                    bronze_to_silver
                              │
                              ▼
                    Silver Delta Table
                              │
                              ▼
                    quality_check
                              │
                              ▼
                    silver_to_gold
                              │
                              ▼
                    Gold Delta Tables
```

The Databricks workflow is orchestrated using Databricks Jobs.

Current Job workflow:

<img width="600" alt="jobs pipelines" src="https://github.com/user-attachments/assets/fb60638c-d438-49a1-8339-8d4c946482f6" />

The Databricks pipeline successfully processes the raw dataset and creates the Bronze, Silver and Gold Delta tables in Unity Catalog.
The resulting Gold tables can be queried directly using Databricks SQL.

Example analytical query using the city-level summary:

```sql
SELECT
    city,
    measurements,
    ROUND(avg_download_speed_mbps, 2) AS avg_download_speed_mbps,
    ROUND(avg_upload_speed_mbps, 2) AS avg_upload_speed_mbps,
    ROUND(avg_latency_ms, 2) AS avg_latency_ms,
    ROUND(avg_signal_strength_dbm, 2) AS avg_signal_strength_dbm
FROM workspace.default.telecom_city_summary
WHERE measurements >= 50
ORDER BY avg_download_speed_mbps DESC;
```

Query result:
<img width="1371" height="192" alt="last" src="https://github.com/user-attachments/assets/d2c92208-df59-4913-9aa9-7cd6250233e9" />

The query returns city-level network performance metrics for cities with at least 50 measurements.

In this example, Vancouver has the highest average download speed at 80.57 Mbps, followed by Toronto at 79.92 Mbps and Calgary at 79.84 Mbps. The average upload speeds are similar across all three cities, while latency remains around 39–40 ms.

The results demonstrate how the Gold layer can be queried directly in Databricks to compare network performance across cities.


---

# AWS Implementation

```
                     Raw Telecom Data (CSV)
                              │
                              ▼
                     Amazon S3 Bronze Layer
                              │
                              ▼
                 AWS MWAA (Apache Airflow DAG)
                              │
                              ▼
                 Amazon EMR Serverless
                     PySpark ETL Jobs
                              │
                              ▼
                     Amazon S3 Silver Layer
                          Parquet Files
                              │
                              ▼
                 Silver Data Quality Validation
                              │
                              ▼
                 Silver → Gold Transformations
                              │
                              ▼
                     Amazon S3 Gold Layer
                Aggregated Analytical Datasets
                              │
                              ▼
                 Amazon Redshift Serverless
                Analytics-ready Warehouse Tables
```

The complete workflow is orchestrated using AWS Managed Workflows for Apache Airflow (MWAA).

Current Airflow DAG:

<img width="200" height="354" alt="pipeline" src="https://github.com/user-attachments/assets/64815c24-0a20-4576-93f0-fd1c4960a9e2" />

Gold datasets are loaded into Amazon Redshift Serverless using Airflow-managed COPY operations.

Example analytical query:

```sql
SELECT *
FROM city_summary
ORDER BY avg_download_speed_mbps DESC;
```

<img width="1118" height="136" alt="wynik_query" src="https://github.com/user-attachments/assets/893017e1-2128-49e4-927a-c77980d0986b" />

---

# AWS Services Used

* Amazon S3
* Amazon EMR Serverless
* AWS MWAA (Apache Airflow)
* Amazon Redshift Serverless
* AWS IAM
* Amazon CloudWatch Logs

# Databricks Technologies

* Databricks
* PySpark
* Delta Lake
* Unity Catalog
* Databricks Jobs
* Databricks Volumes
* GitHub integration

---

# Technologies Used

* Python
* PySpark
* Apache Spark
* Apache Airflow
* AWS MWAA
* Amazon EMR Serverless
* Amazon S3
* Amazon Redshift Serverless
* Parquet
* AWS IAM
* CloudWatch
* ETL / ELT Concepts
* Medallion Data Lake Architecture

---

# Project Structure


```
telecom-data-pipeline/

├── airflow/
│   └── dags/
│       └── telecom_pipeline_dag.py
│
├── databricks/
│   ├── bronze_to_silver
│   ├── quality_check
│   └── silver_to_gold
│
├── package/
│   ├── jobs/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   │
│   ├── transformations/
│   │   ├── __init__.py
│   │   ├── cleaning.py
│   │   ├── features.py
│   │   └── aggregations.py
│   │
│   ├── quality_checks/
│   │   ├── __init__.py
│   │   └── quality_checks.py
│   │
│   ├── config.py
│   └── __init__.py
│
├── scripts/
│   ├── generate_data.py
│   └── upload_to_s3.py
│
├── sql/
│   ├── city_analysis.sql
│   ├── device_analysis.sql
│   ├── network_analysis.sql
│   └── daily_analysis.sql
│
├── data/
│   └── raw/
│       └── .gitkeep
│
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

---

# How to Run the Pipeline

1. Generate synthetic telecom measurements.
2. Upload data to the S3 Bronze layer.
3. Upload PySpark jobs and dependencies to Amazon S3.
4. Upload Airflow DAG code to the MWAA DAGs folder.
5. Trigger the Airflow DAG.
6. MWAA starts EMR Serverless Spark jobs.
7. Spark processes Bronze data and creates Silver Parquet files.
8. Data quality checks validate Silver data.
9. Spark creates Gold analytical datasets.
10. Airflow loads Gold datasets into Amazon Redshift Serverless.

