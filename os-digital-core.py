import requests
from requests_aws4auth import AWS4Auth
import boto3
import json
import hashlib
import hmac
import datetime
import random
import logging
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
host = os.getenv('ELASTICSEARCH_HOST')
index_create_url = os.getenv('ELASTICSEARCH_INDEX_URL')
region = os.getenv('AWS_REGION')
index_key = os.getenv('INDEX_KEY')
log_failed_responses = os.getenv('LOG_FAILED_RESPONSES').lower() == 'true'

def lambda_handler(event, context):
    try:
        logger.info("Lambda function triggered with event: %s", json.dumps(event))

        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logger.info("Processing file from S3 bucket: %s, key: %s", bucket, key)

        params = {'Bucket': bucket, 'Key': key}
        response = s3.get_object(**params)
        log_data = response['Body'].read().decode('utf-8')
        print("Log Data are: ", log_data)

        elasticsearch_bulk_data = transform(log_data, bucket, key)
        print("Elasticsearch Bulk Data are: ", elasticsearch_bulk_data)

        if not elasticsearch_bulk_data:
            logger.info("Control message detected. No data to process.")
            print("Elasticsearch Bulk Data is empty: ", elasticsearch_bulk_data)  # Add print statement here
            return 'Control message handled successfully'

        post(elasticsearch_bulk_data)
        logger.info("Successfully processed and indexed log data.")

        return "Success"
    except Exception as e:
        logger.error("Lambda execution failed: %s", str(e), exc_info=True)
        raise e


def transform(payload, bucket, key):
    bulk_request_body = ""
    unique_id = f"{int(datetime.datetime.now().timestamp())}{random.randint(1, 10**18)}{random.randint(1, 10**18)}"
    
    fetched_obj = payload.split('\n')
    count = -1
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    for k in fetched_obj:
        count += 1
        if k.strip():
            try:
                parsed_data = json.loads(k)
                logger.debug(f"Processing log entry: {json.dumps(parsed_data)}")  # Debugging log entry
            except json.JSONDecodeError as e:
                logger.warning("Skipping invalid JSON entry: %s, Error: %s", k, str(e))
                continue

            index_name = ''
            namespace = parsed_data.get('kubernetes', {}).get('namespace_name', '')
            if 'dte-' in namespace:
                date = parsed_data.get('date', '').split('T')[0]
                index_name = f"{index_key}{namespace}_{date}"
                log_line = parsed_data.get('log', '')

                if not index_name or not log_line:
                    logger.warning("Missing necessary fields: index_name or log_line are empty for entry: %s", k)
                    continue

                actions = {"index": {"_index": index_name, "_id": f"{unique_id}{count}"}}
                source = {
                    "kubernetes": parsed_data.get('kubernetes', {}),
                    "ms-name": parsed_data.get('kubernetes', {}).get('container_name', ''),
                    "log": log_line,
                    "cluster_name": parsed_data.get('cluster_name', ''),
                    "@id": f"{unique_id}{count}",
                    "@timestamp": parsed_data.get('date', ''),
                    "@owner": '',
                    "@log_group": bucket,
                    "@log_stream": key
                }

                bulk_request_body += "\n".join([json.dumps(actions), json.dumps(source)]) + "\n"
            else:
                logger.debug(f"Skipping entry as namespace doesn't match 'dte-': {parsed_data}")

    logger.info("Transformed %d log entries for indexing.", count + 1)
    logger.debug(f"Final Elasticsearch Bulk Data: {bulk_request_body}")  # Debugging the bulk data
    return bulk_request_body


################################### POST ##################################
def post(body):
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    try:
        response = requests.post(host, auth=awsauth, data=body, headers={"Content-Type": "application/x-ndjson"})
        response.raise_for_status()  # Raises an exception for HTTP 4xx/5xx

        info = response.json()
        failed_items = [x for x in info.get('items', []) if x.get('index', {}).get('status', 0) >= 300]
        
        success = {
            "attemptedItems": len(info.get('items', [])),
            "successfulItems": len(info.get('items', [])) - len(failed_items),
            "failedItems": len(failed_items)
        }

        if info.get('errors', False):
            del info['items']
            error = {"statusCode": response.status_code, "responseBody": info}
            log_failure(error, failed_items)
            raise Exception(f"Elasticsearch indexing failed with status {response.status_code}")

        logger.info("Elasticsearch indexing successful: %s", json.dumps(success))

    except requests.exceptions.RequestException as e:
        logger.error("Failed to send data to Elasticsearch: %s", str(e), exc_info=True)
        raise e


def log_failure(error, failed_items):
    global log_failed_responses
    if log_failed_responses:
        logger.error("Failed Elasticsearch response: %s", json.dumps(error, indent=2))
        if failed_items:
            logger.error("Failed Items: %s", json.dumps(failed_items, indent=2))


# Utility functions
def hmac_sha256(key, data, encoding):
    return hmac.new(key.encode(encoding), data.encode(encoding), hashlib.sha256).digest()

def sha256_hash(data, encoding):
    return hashlib.sha256(data.encode(encoding)).digest()
