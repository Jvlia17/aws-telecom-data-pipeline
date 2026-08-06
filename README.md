# Telecom Data Pipeline

**Apache Spark | AWS MWAA (Airflow) | Amazon EMR Serverless | S3 | Redshift Serverless | Parquet | Data Lake | ETL**

End-to-End Cloud Data Engineering pipeline that processes telecom network performance measurements using Apache Spark running on Amazon EMR Serverless and orchestrated with Apache Airflow on Amazon MWAA.

The project follows a modern Medallion Data Lake Architecture (Bronze → Silver → Gold), transforming raw telecom measurements into optimized analytical datasets stored in Amazon S3 and loaded into Amazon Redshift Serverless for analytics.

---

# 📖 Project Overview

Modern telecommunication systems generate large volumes of network performance measurements from mobile devices and infrastructure equipment.

Raw operational data requires automated processing before it can be used for:

* network performance analytics
* reporting
* dashboards
* analytical applications

This project demonstrates a production-style Data Engineering workflow by implementing:

* cloud-based data storage
* distributed Spark processing
* workflow orchestration
* data transformation layers
* data quality validation
* analytical data warehouse loading

The pipeline is built using commonly used technologies and patterns from modern Data Engineering environments.

---

# 🎯 Business Problem

Raw telecom measurements often contain:

* missing values
* duplicated records
* inconsistent formats
* invalid measurements
* inefficient storage formats

Network analysts require reliable, structured and optimized datasets that can support analytical workloads.

The goal of this project is to automate the transformation of raw telecom data into analytics-ready datasets.

---

# 🏗️ Data Pipeline Architecture

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

---

# 🔄 Pipeline Workflow

The complete workflow is orchestrated using AWS Managed Workflows for Apache Airflow (MWAA).

Current Airflow DAG:

![pipeline.png](..%2F..%2Fpipeline.png)

## 🥉 Bronze Layer - Raw Data Storage

The dataset is synthetically generated for demonstration purposes.
Raw telecom network measurements are stored in Amazon S3.


The dataset contains attributes such as:

* device model
* network type
* download speed
* upload speed
* latency
* signal strength
* GPS coordinates
* timestamps

The Bronze layer keeps the original source data before any transformations.

## 🥈 Silver Layer - Data Cleaning and Transformation

PySpark jobs running on Amazon EMR Serverless process raw data and create a cleaned Silver layer.

Transformations include:

* duplicate removal
* missing value handling
* schema validation
* datatype standardization

Processed data is stored as optimized Parquet files.

## 🧪 Data Quality Validation

Before creating analytical datasets, the pipeline performs validation checks on the Silver layer.

Current checks include:

* missing value validation
* schema validation
* value range validation
* data consistency checks

The quality check is executed as an Airflow task before the Gold transformation step.

## 🥇 Gold Layer - Analytics Ready Data

The Gold layer contains aggregated datasets prepared for analytical workloads.

Current datasets:

```
gold/

├── city_summary/
├── device_summary/
└── network_summary/
```

The aggregations include metrics such as:

* average download speed
* average upload speed
* average latency
* average signal strength
* measurement counts

These datasets are optimized for:

* dashboards
* reporting
* analytical SQL queries
* downstream applications

---

# 🏢 Amazon Redshift Serverless Warehouse Layer

Gold datasets are loaded into Amazon Redshift Serverless using Airflow-managed COPY operations.

The warehouse contains analytical tables:

Example analytical query:

```sql
SELECT *
FROM city_summary
ORDER BY avg_download_speed_mbps DESC;
```

![wynik_query.png](..%2F..%2Fwynik_query.png)

---

# ☁️ AWS Services Used

## Currently Implemented

- ✅ Amazon S3
- ✅ Amazon EMR Serverless
- ✅ AWS MWAA (Apache Airflow)
- ✅ Amazon Redshift Serverless
- ✅ AWS IAM
- ✅ Amazon CloudWatch Logs
- ✅ Apache Spark

---

# 🧪 Technologies Used

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

# 📁 Project Structure


```
telecom-data-pipeline/

├── airflow/
│   └── dags/
│       └── telecom_pipeline_dag.py
│
├── package/
│   │
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

# 🚀 How to Run the Pipeline

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

