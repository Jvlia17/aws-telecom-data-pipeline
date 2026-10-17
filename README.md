# Telecom Data Pipeline

**AWS Lambda | Amazon S3 | Amazon Athena | AWS Glue Data Catalog | Terraform | GitHub Actions**

A serverless batch data pipeline for telecom network analytics, built on AWS. It processes raw mobile network measurements using Python and AWS Lambda, validates data quality, handles invalid records, and prepares analytics-ready datasets for SQL analysis with Amazon Athena. Infrastructure is managed with Terraform, with automated testing and CI/CD using GitHub Actions.

# Business Problem

A telecommunications company collects daily batches of mobile network measurements from multiple cities, devices, and network types. Raw data may contain missing values, duplicate records, or invalid measurements, making reliable analysis difficult.

The goal is to build an automated data pipeline that validates incoming data, separates invalid records, transforms clean measurements into structured datasets, and enables SQL-based analysis of network performance and anomalies.

## This project is currently being rebuilt and is a work in progress.
