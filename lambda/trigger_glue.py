import json
import os
import urllib.parse
import boto3

# Inicializar cliente de AWS Glue
glue_client = boto3.client('glue')

# Nombre del Job de AWS Glue que invocaremos
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'job-etl-online-retail')

def lambda_handler(event, context):
    """
    Función que se activa cuando un objeto es subido a S3
    e inicia la ejecución del Job de AWS Glue.
    """
    try:
        # Extraer información del evento de S3
        for record in event.get('Records', []):
            bucket_name = record['s3']['bucket']['name']
            object_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            
            print(f"Nuevo archivo detectado en S3: s3://{bucket_name}/{object_key}")
            
            # Validar que el archivo esté dentro de la carpeta raw/
            if object_key.startswith('raw/'):
                print(f"Iniciando ejecucion del Job de Glue: {GLUE_JOB_NAME}...")
                
                # Iniciar el Job de Glue
                response = glue_client.start_job_run(
                    JobName=GLUE_JOB_NAME
                )
                
                job_run_id = response['JobRunId']
                print(f"Job de Glue iniciado exitosamente. JobRunId: {job_run_id}")
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Glue Job iniciado correctamente',
                        'JobRunId': job_run_id,
                        'FileProcessed': object_key
                    })
                }
            else:
                print(f"El archivo {object_key} no está en la carpeta 'raw/'. Se ignora la ejecución.")
                
    except Exception as e:
        print(f"Error procesando el evento de S3 e iniciando Glue: {str(e)}")
        raise e