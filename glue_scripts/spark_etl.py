import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, current_timestamp, round as spark_round, when

# Inicialización de Glue y Spark
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'REDSHIFT_CONNECTION_NAME',
    'REDSHIFT_TMP_DIR'
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parámetros pasados desde el Job de Glue
REDSHIFT_CONN = args['REDSHIFT_CONNECTION_NAME']
REDSHIFT_TMP = args['REDSHIFT_TMP_DIR']

# Rutas de S3
S3_INPUT_PATH = "s3://aws-etl-glue-lambda-demo/raw/Online Retail.csv"
S3_OUTPUT_PATH = "s3://aws-etl-glue-lambda-demo/processed/online_retail_cleaned/"

print("Cargando datos desde S3...")

# 1. Leer el archivo CSV desde S3
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(S3_INPUT_PATH)

# 2. Transformaciones de Datos + Columna de Auditoría (load_timestamp)
df_cleaned = df_raw \
    .filter(~col("InvoiceNo").startswith("C")) \
    .filter((col("Quantity") > 0) & (col("UnitPrice") > 0)) \
    .withColumn("CustomerID", when(col("CustomerID").isNull(), -1).otherwise(col("CustomerID"))) \
    .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "d/M/yyyy H:m")) \
    .withColumn("TotalAmount", spark_round(col("Quantity") * col("UnitPrice"), 2)) \
    .withColumn("load_timestamp", current_timestamp()) \
    .select(
        col("InvoiceNo").alias("invoice_no"),
        col("StockCode").alias("stock_code"),
        col("Description").alias("description"),
        col("Quantity").cast("integer").alias("quantity"),
        col("InvoiceDate").alias("invoice_date"),
        col("UnitPrice").cast("double").alias("unit_price"),
        col("CustomerID").cast("integer").alias("customer_id"),
        col("Country").alias("country"),
        col("TotalAmount").cast("double").alias("total_amount"),
        col("load_timestamp")
    )

print("Escribiendo datos procesados en formato Parquet a S3...")

# 3. Guardar copia en S3 en formato Parquet
df_cleaned.write \
    .mode("overwrite") \
    .parquet(S3_OUTPUT_PATH)

print("Cargando datos hacia Amazon Redshift...")

# 4. Cargar datos a Redshift usando el conector nativo de Glue
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(df_cleaned, glueContext, "df_cleaned"),
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": REDSHIFT_TMP,
        "useConnectionProperties": "true",
        "dbtable": "public.online_retail_sales",
        "connectionName": REDSHIFT_CONN
    },
    transformation_ctx="write_redshift"
)

print("Proceso ETL completado con éxito en S3 y Redshift.")
job.commit()