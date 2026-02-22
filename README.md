🚀 DBIQ Agent
AI Agent for Real-Time Athena Performance Monitoring & Query Optimization
📌 Project Overview

DBIQ Agent is an AI-powered monitoring system designed to:

Monitor Amazon Athena query executions

Detect inefficient SQL patterns

Calculate cost per query

Suggest optimized SQL rewrites

Detect cross-database joins

Store last 100 query executions

Automatically identify long-running queries

Provide real-time dashboard analytics

This project simulates enterprise-level multi-domain workloads across HR, Finance, Sales, and Analytics domains.
========================================================================================================================

🏗 Architecture

User (Streamlit UI)
↓
Python Monitoring Engine (Boto3)
↓
Amazon Athena
↓
Amazon S3 Data Lake

🛠 Technologies Used

Python 3.11

AWS Athena

AWS S3

AWS IAM

Boto3

Streamlit

Pandas

Matplotlib
=====================================================================================================================

📦 Project Structure
DBIQ-AGENT/
│
├── app.py
├── monitor.py
├── requirements.txt
└── README.md
=============================================================================================
🧰 Complete Setup Guide (Step-by-Step)
🔹 STEP 1 — AWS Account


You must have:

AWS Free Tier Account

Region set to: us-east-1

🔹 STEP 2 — Create IAM User (Do NOT Use Root)


Go to AWS Console

Search: IAM

Click Users → Create User

Username: dbiq-agent-user

Attach policies:

AmazonAthenaFullAccess

AmazonS3FullAccess

Create User


🔹 STEP 3 — Create Access Keys

Open created IAM user

Go to Security Credentials

Click Create Access Key

Choose CLI

Copy:

Access Key ID

Secret Access Key

⚠ Secret key is shown only once.

🔹 STEP 4 — Install AWS CLI


Download:
https://awscli.amazonaws.com/AWSCLIV2.msi

Install and verify:

aws --version

🔹 STEP 5 — Configure AWS CLI


Run:

aws configure


Enter:

AWS Access Key ID: <your_key>
AWS Secret Access Key: <your_secret>
Default region: us-east-1
Output format: json


Test:

aws sts get-caller-identity

🔹 STEP 6 — Create S3 Bucket


Go to S3 → Create Bucket

Example:

dbiq-agent-results-yourname-2026


Region: us-east-1

🔹 STEP 7 — Configure Athena Result Location


Open Athena

Go to Query Editor

Settings → Set result location:

s3://dbiq-agent-results-yourname-2026/


Save.

🔹 STEP 8 — Create Databases


Run in Athena:

CREATE DATABASE analytics_domain;
CREATE DATABASE hr_domain;
CREATE DATABASE finance_domain;
CREATE DATABASE sales_domain;

🔹 STEP 9 — Create Large Dataset


Switch:

USE analytics_domain;


Run:

CREATE TABLE large_data
WITH (format = 'PARQUET') AS
SELECT
    row_number() OVER () AS id,
    rand() * 10000 AS value1,
    rand() * 5000 AS value2,
    current_date AS created_date
FROM UNNEST(sequence(1, 3000000)) AS t(x);

🔹 STEP 10 

— Create HR Table
USE hr_domain;

CREATE TABLE employees
WITH (format = 'PARQUET') AS
SELECT
    row_number() OVER () AS employee_id,
    CONCAT('Employee_', CAST(row_number() OVER () AS VARCHAR)) AS name,
    (rand() * 80000 + 20000) AS salary,
    current_date AS join_date
FROM analytics_domain.large_data
LIMIT 1000000;

🔹 STEP 11

 — Create Finance Table
USE finance_domain;

CREATE TABLE transactions
WITH (format = 'PARQUET') AS
SELECT
    row_number() OVER () AS transaction_id,
    rand() * 10000 AS transaction_amount,
    current_date AS transaction_date
FROM analytics_domain.large_data
LIMIT 1500000;

🔹 STEP 12 

— Create Sales Tables
USE sales_domain;

CREATE TABLE customers
WITH (format = 'PARQUET') AS
SELECT
    row_number() OVER () AS customer_id,
    CONCAT('Customer_', CAST(row_number() OVER () AS VARCHAR)) AS name,
    current_date AS created_date
FROM analytics_domain.large_data
LIMIT 1000000;

🔹 STEP 13

 — Install Project Dependencies

Inside project folder:

pip install -r requirements.txt


If no requirements file:

pip install boto3 pandas streamlit matplotlib

🔹 STEP 14

 — Run DBIQ Agent
streamlit run app.py
=============================================================================================


Open:

http://localhost:8501

📊 Features

✔ Run Athena queries via UI
✔ Select database dynamically
✔ Store last 100 query executions
✔ Detect tables used
✔ Detect joins
✔ Calculate cost per query
✔ Show performance charts
✔ Suggest optimized SQL rewrites
✔ Detect inefficient patterns
✔ Auto-kill long-running queries
=============================================================================================


💰 Athena Cost Model

Athena pricing:

$5 per TB scanned

Cost formula:

Cost = (DataScannedBytes / 1TB) × 5

⚠ Free Tier Warning

Athena is NOT fully free.

To avoid high costs:

Avoid large CROSS JOIN queries

Do not exceed 3M row table creation repeatedly

Stop long-running queries

Delete unused tables

🛡 Security Best Practices

Do NOT use root account

Use IAM user

Rotate keys regularly

Use least privilege in production

Never commit access keys to GitHub
=============================================================================================


🔮 Future Enhancements

ML-based anomaly detection

Slack alerts

Historical trend dashboard

Query risk scoring

Production deployment on EC2

Role-based authentication

📄 License

This project is for educational and research purposes.


⭐ If You Found This Useful

Give this repository a ⭐

🚀 END OF README
