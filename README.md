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

# Medallion Data Lake Architecture

The dataset is synthetically generated and contains telecom network measurements such as device model, network type, download and upload speed, latency, signal strength, GPS coordinates and timestamps.

Bronze stores the original raw data in Amazon S3. In the Databricks implementation, the data is read from the S3 Bronze layer and stored as a Delta table in Unity Catalog.

Silver contains cleaned and transformed data prepared for analysis. The AWS implementation stores Silver as Parquet files, while Databricks stores it as a Delta table. The pipeline also performs validation checks on the Silver layer before continuing to Gold.

Gold contains aggregated datasets for analytical workloads:

```
gold/

├── city_summary/
├── device_summary/
└── network_summary/
```

The aggregations include average download and upload speed, latency, signal strength and measurement counts.

---

# Databricks Implementation

Before running the Databricks implementation, Databricks must be configured with the required AWS permissions. This includes setting up the appropriate AWS IAM role and permissions to allow Databricks to securely access the required AWS resources.

```
                    Raw Telecom Data (CSV)
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
* GitHub integration

---

# Technologies Used

* Python
* Apache Spark
* PySpark
* Apache Airflow
* Amazon S3
* Amazon EMR Serverless
* Amazon Redshift Serverless
* Databricks
* Delta Lake
* Unity Catalog
* Parquet
* ETL / ELT
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

For AWS:
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

For Databricks:
1. Generate synthetic telecom measurements.
2. Store the raw dataset in the AWS S3 Bronze layer.
3. Configure Databricks with the required AWS IAM permissions and Unity Catalog access.
4. Configure the Databricks notebooks and pipeline dependencies.
5. Run the telecom_databricks_pipeline Databricks Job.
6. bronze_to_silver processes the Bronze data and creates the Silver Delta table.
7. quality_check validates the Silver layer.
8. silver_to_gold creates the Gold analytical tables.
9. Query the resulting Gold tables using Databricks SQL.
