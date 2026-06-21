#  🎵 Data Pipelines with Airflow – Sparkify Project

## 📌 Project Overview

This project builds an **automated and monitored ETL data pipeline** for a fictional music streaming company, **Sparkify**, using **Apache Airflow**.

The goal is to move raw data from **Amazon S3** into a **Redshift data warehouse**, transform it into a star schema, and run **data quality checks** to ensure reliability for analytics.

The pipeline is:
- Dynamic
- Reusable
- Monitorable
- Backfill-enabled
---

## 🏗️ Architecture

The pipeline performs the following steps:

1. **Stage raw data from S3 → Redshift**
2. **Transform data into fact and dimension tables**
3. **Run data quality checks**

---

## 📂 Datasets

Two datasets stored in S3:

- 🎧 Log Data  
  `s3://udacity-dend/log_data`

- 🎵 Song Data  
  `s3://udacity-dend/song-data`

These datasets contain:
- User activity logs (JSON)
- Song metadata (JSON)

---


## ⚙️ Technologies Used

- Apache Airflow
- Amazon S3
- Amazon Redshift (Serverless)
- Python
- SQL
- AWS IAM

---

## 🔄 DAG Workflow
![DAG Workflow](assets/final_project_dag_graph2.png)

---

## 🧩 Custom Operators

This project implements four reusable Airflow operators:

### 1. Stage Operator
- Loads JSON data from S3 into Redshift staging tables
- Uses dynamic COPY SQL commands
- Supports execution-time templating for backfills

---

### 2. Fact & Dimension Load Operators

#### Fact Table Operator
- Loads large fact tables (append-only)
- Inserts transformed data into Redshift

#### Dimension Table Operator
- Uses **truncate-insert pattern**
- Clears table before loading fresh data
- Supports reusable SQL transformations

---

### 3. Data Quality Operator
- Runs SQL-based validation checks
- Compares actual vs expected results
- Raises error if data quality tests fail

Example test:
```sql
SELECT COUNT(*) FROM users WHERE user_id IS NULL;
```

## 🔐 AWS Setup Requirements

Before running the pipeline, ensure the following AWS resources are configured:

- IAM User is created with proper permissions
- Amazon Redshift Serverless is configured
- Airflow connections for AWS and Redshift are properly set up

---

## ☁️ Data Setup (Amazon S3)

This project uses datasets provided by Udacity. You must copy them into your own S3 bucket.


### 📥 Step 1: Copy Udacity data locally (CloudShell)

```bash
aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
aws s3 cp s3://udacity-dend/log_json_path.json ~/
```

### 📤 Step 2: Upload Data to Your S3 Bucket

Replace `your-bucket` with your actual S3 bucket name.

```bash
aws s3 cp ~/log-data/ s3://your-bucket/log-data/ --recursive
aws s3 cp ~/song-data/ s3://your-bucket/song-data/ --recursive
aws s3 cp ~/log_json_path.json s3://your-bucket/
```

### 📤 Step 3: Verify Upload
```bash
aws s3 ls s3://your-bucket/log-data/
aws s3 ls s3://your-bucket/song-data/
aws s3 ls s3://your-bucket/
```
