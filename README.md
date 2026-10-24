# Telecom Data Pipeline

**AWS Lambda | Amazon S3 | Amazon Athena | AWS Glue Data Catalog | Terraform | GitHub Actions**

A data pipeline for telecom network measurements from five Canadian cities, built with Python and AWS Glue. The project focuses on data transformation and preparing data for SQL analysis with Amazon Athena. Infrastructure is managed with Terraform, and testing and deployment are automated with GitHub Actions.

# Business Problem

A telecommunications company collects network measurements from several Canadian cities throughout the month. At the end of each month, the company sends the data for processing. The goal is to clean and validate the data and compare network performance between cities.

# My approach

To address this problem, I designed a data pipeline that uses multiple AWS services to process telecom network measurements. The infrastructure is managed with Terraform, while GitHub Actions automates testing and deployment. 

<p align="center">
  <img src="images/diagram.png" alt="Image" width="300">
</p>

As shown in the diagram, several AWS services were used:
- As shown in the diagram, **Amazon S3** stores raw data in the bronze/ folder, cleaned data in silver/, processed data in gold/, and invalid records in quarantine/.
- **AWS Glue** uses two Python scripts to validate and clean the raw data, move valid records from Bronze to Silver, and transform the cleaned data from Silver to Gold.
- **Amazon Athena** is used to query the final data with SQL and compare network performance across Canadian cities.
- **Terraform** manages the AWS infrastructure, while GitHub Actions runs tests and automates deployment to AWS.


# Telecom Data
The pipeline uses simulated network measurements from five Canadian cities: Vancouver, Toronto, Montreal, Ottawa, and Calgary. I wrote a Python script to generate 50,000 records covering the last 30 days.

The data is not completely random. Each city and network type (4G or 5G) has its own speed and latency ranges, so the measurements follow different patterns. I also added realistic data quality issues to 8% of the records, such as missing values, duplicate IDs, invalid timestamps, and unusual measurements. This gives the pipeline data to clean and validate.

# Results

The results in Athena show differences in network performance across the five Canadian cities. 

<p align="center">
  <img src="images/athena.png" alt="Image">
</p>

- Toronto has the highest average download and upload speeds, while Calgary has the lowest average download speed. 
- Toronto also has the lowest latency, meaning a faster response time. 
- Vancouver has the highest latency. 

These results help the company compare network performance across cities, spot areas with weaker performance, and decide where improvements may be needed.
