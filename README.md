# Serverless Data Pipeline: S3 to Redshift via AWS Glue Spark & Lambda

An end-to-end, event-driven Data Engineering pipeline built on AWS. This project ingests raw e-commerce transaction data uploaded to Amazon S3, triggers a PySpark ETL job using AWS Lambda, cleanses and transforms the dataset via AWS Glue, and loads the optimized data into both an S3 Parquet Data Lake and an Amazon Redshift Data Warehouse.

---

## 🏗️ Architecture & Data Flow

```text
[ Raw Data CSV ] ──► Amazon S3 (raw/) ──► S3 ObjectCreated Event
                                                 │
                                                 ▼
[ Redshift DW ]  ◄── AWS Glue PySpark ◄── AWS Lambda Trigger
  (Data Warehousing)    (Transformation &           (Orchestration)
                       Parquet Conversion)

```

Data Ingestion (Amazon S3): Raw files (Online Retail.csv) land in the s3://<bucket-name>/raw/ prefix.

Event Orchestration (AWS Lambda): An S3 event notification (s3:ObjectCreated:*) triggers a Python Lambda function to initiate the AWS Glue Job.

Data Transformation (AWS Glue - PySpark):

Filters out cancelled transactions and invalid quantities/prices.

Handles missing CustomerID records.

Parses dates into standardized Timestamps.

Computes derived financial metrics (TotalAmount).

Appends audit tracking metadata (load_timestamp).

Dual Storage Load:

Data Lake: Writes partitioned/compressed Parquet files back to s3://<bucket-name>/processed/.

Data Warehouse: Performs high-throughput bulk loads into Amazon Redshift using the Glue-Redshift native JDBC connector.