import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    print(event)

    # Extraer datos del body
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']

    # Nombre de la tabla DynamoDB
    nombre_tabla = os.environ["TABLE_NAME"]

    # Nombre del bucket dinámico
    bucket_name = os.environ["INGEST_BUCKET"]

    # UUID del comentario
    uuidv1 = str(uuid.uuid1())

    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # ---------------------------
    # 1. Guardar en DynamoDB
    # ---------------------------
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response_db = table.put_item(Item=comentario)

    # ---------------------------
    # 2. Guardar en S3 (INGESTA PUSH)
    # ---------------------------
    s3 = boto3.client('s3')

    s3.put_object(
        Bucket=bucket_name,
        Key=f"{tenant_id}/{uuidv1}.json",
        Body=json.dumps(comentario),
        ContentType="application/json"
    )

    print("Guardado en DynamoDB y en S3")

    return {
        'statusCode': 200,
        'comentario': comentario,
        'db': response_db,
        's3_bucket': bucket_name
    }
