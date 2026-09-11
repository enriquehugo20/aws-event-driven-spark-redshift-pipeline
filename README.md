# AWS S3 to Redshift Serverless Automated ETL Pipeline

An automated, event-driven serverless data pipeline that ingests raw retail CSV data uploaded to Amazon S3, performs schema validation and data transformations using AWS Glue (PySpark), and loads the cleaned data into Amazon Redshift Serverless via JDBC connectivity. The pipeline is orchestrated automatically using AWS Lambda triggered by S3 bucket events.

## Prerequisites and Infrastructure Requirements

* AWS Account with administrator access or custom IAM permissions for S3, Glue, Lambda, Redshift, and Secrets Manager.
* Amazon S3 Bucket: `aws-etl-glue-lambda-demo`
* Amazon Redshift Serverless Workgroup: `workgroup-demo`
* Amazon Redshift Serverless Namespace: `namespace-demo` (Database: `dev`, User: `awsuser`)
* Port 5439 open on Redshift Security Group for inbound traffic.
* Redshift Workgroup configured with Public Accessibility enabled (if Glue operates outside dedicated VPC subnets).

## Required IAM Roles and Policies

### Glue Execution Role (GlueETL-Redshift-Role)

* Trusted Entity: `glue.amazonaws.com`
* Attached Managed Policies:
  * `AWSGlueServiceRole`
  * `AmazonS3FullAccess` (or scoped access to `aws-etl-glue-lambda-demo`)
* Inline Policy (`GlueReadSecretsPolicy`):
  
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueToReadSecretsManager",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            "Resource": "arn:aws:secretsmanager:us-east-1:276846898236:secret:RedshiftServerlessSecretDemo-*"
        }
    ]
}
```
# Configuration Details

## Lambda Trigger Role (Lambda-Trigger-Glue-Role)

* Trusted Entity: `lambda.amazonaws.com`
* Attached Managed Policies:
  * `AWSLambdaBasicExecutionRole`
  * `AWSGlueConsoleFullAccess` (or scoped `glue:StartJobRun` permissions for the specific Glue job)

## AWS Secrets Manager Configuration

* Secret Name: `RedshiftServerlessSecretDemo`
* Secret Type: Credentials for Amazon Redshift database
* Key-Value Structure:
  * `username`: `awsuser`
  * `password`: `<YOUR_REDSHIFT_PASSWORD>`
  * `engine`: `redshift`
  * `host`: `workgroup-demo.276846898236.us-east-1.redshift-serverless.amazonaws.com`
  * `port`: `5439`
  * `dbClusterIdentifier`: `workgroup-demo`

## AWS Glue JDBC Connection Configuration

* Connection Name: `redshift-serverless-conn`
* Connection Type: `Amazon Redshift`
* Instance Type: Serverless (`workgroup-demo`)
* Database Name: `dev`
* AWS Secret: `RedshiftServerlessSecretDemo`
* Network Configuration: Bypasses explicit custom VPC subnet mapping to prevent VPC endpoint STS resolution errors, using direct JDBC connectivity within PySpark.

## AWS Lambda Trigger Function

* Function Name: `trigger-glue-on-s3-upload`
* Runtime: Python 3.12
* Handler: `lambda_function.lambda_handler`
* Execution Role: `Lambda-Trigger-Glue-Role`
* Event Source Trigger: S3 `ObjectCreated` event on bucket `aws-etl-glue-lambda-demo` for prefix `raw/` and suffix `.csv`.

