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

# Environment variables for configuration
OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT', 'https://vpc-dte-development-01-kpzf7mrhamvqxicqdfcvwhbzwy.ap-southeast-1.es.amazonaws.com')
REGION = os.environ.get('AWS_REGION', 'ap-southeast-1')
INDEX_PREFIX = os.environ.get('INDEX_PREFIX', 'index_digital-core_')
NAMESPACE_FILTER = os.environ.get('NAMESPACE_FILTER', 'dte-')  # Only index logs from namespaces containing this

host = f'{OPENSEARCH_ENDPOINT}/_bulk'
index_create_url = f'{OPENSEARCH_ENDPOINT}/'
log_failed_responses = os.environ.get('LOG_FAILED_RESPONSES', 'false').lower() == 'true'

def lambda_handler(event, context):
    try:
        logger.info("Lambda function triggered with event: %s", json.dumps(event))

        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logger.info("Processing file from S3 bucket: %s, key: %s", bucket, key)

        params = {'Bucket': bucket, 'Key': key}
        response = s3.get_object(**params)
        log_data = response['Body'].read().decode('utf-8')
        logger.info("Retrieved log data with %d characters", len(log_data))

        elasticsearch_bulk_data = transform(log_data, bucket, key)

        if not elasticsearch_bulk_data:
            logger.info("No data to process after transformation.")
            return 'Control message handled successfully'

        post(elasticsearch_bulk_data)
        logger.info("Successfully processed and indexed log data.")

        return "Success"
    except Exception as e:
        logger.error("Lambda execution failed: %s", str(e), exc_info=True)
        raise e


def transform(payload, bucket, key):
    """
    Transform Fluent Bit S3 logs to Elasticsearch bulk format
    """
    bulk_request_body = ""
    unique_id = f"{int(datetime.datetime.now().timestamp())}{random.randint(1, 10**18)}{random.randint(1, 10**18)}"
    
    fetched_obj = payload.split('\n')
    count = -1
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, service, session_token=credentials.token)
    
    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    created_indexes = set()  # Track created indexes

    for k in fetched_obj:
        count += 1
        if k.strip():
            try:
                parsed_data = json.loads(k)
                logger.debug(f"Processing log entry {count}: {json.dumps(parsed_data)}")
            except json.JSONDecodeError as e:
                logger.warning("Skipping invalid JSON entry at line %d: %s", count, str(e))
                continue

            # Extract kubernetes metadata
            k8s_metadata = parsed_data.get('kubernetes', {})
            namespace = k8s_metadata.get('namespace_name', '')
            
            # Filter by namespace
            if NAMESPACE_FILTER not in namespace:
                logger.debug(f"Skipping entry - namespace '{namespace}' doesn't contain '{NAMESPACE_FILTER}'")
                continue

            # Get date for index suffix
            date = parsed_data.get('date', '').split('T')[0]
            if not date:
                date = datetime.datetime.now().strftime('%Y-%m-%d')
            
            index_name = f"{INDEX_PREFIX}{namespace}_{date}"
            log_line = parsed_data.get('log', '')

            if not log_line:
                logger.warning("Missing log line for entry: %s", k[:100])
                continue

            # Create index if doesn't exist
            if index_name not in created_indexes:
                url = f"{index_create_url}{index_name}"
                index_exists_response = requests.head(url, auth=awsauth)
                if index_exists_response.status_code != 200:
                    headers = {'Content-Type': 'application/json'}
                    create_response = requests.put(url, auth=awsauth, headers=headers, json=index_settings)
                    if create_response.status_code in [200, 201]:
                        logger.info(f"Created new index: {index_name}")
                    else:
                        logger.warning(f"Failed to create index {index_name}: {create_response.status_code}")
                created_indexes.add(index_name)

            # Build document
            actions = {"index": {"_index": index_name, "_id": f"{unique_id}{count}"}}
            source = {
                "kubernetes": k8s_metadata,
                "ms-name": k8s_metadata.get('container_name', ''),
                "microservice": k8s_metadata.get('container_name', ''),
                "namespace": namespace,
                "pod": k8s_metadata.get('pod_name', ''),
                "host": k8s_metadata.get('host', ''),
                "cluster_name": parsed_data.get('cluster_name', 'unknown'),
                "environment": parsed_data.get('environment', 'unknown'),
                "deployment": parsed_data.get('deployment', 'unknown'),
                "log": log_line,
                "@id": f"{unique_id}{count}",
                "@timestamp": parsed_data.get('date', ''),
                "@owner": '',
                "@log_group": bucket,
                "@log_stream": key
            }
            
            # Parse severity if available (DTE format)
            if len(log_line.split()) >= 3:
                severity = str(log_line.split()[2])
                if severity in ['INFO', 'DEBUG', 'ERROR', 'WARN', 'W']:
                    source['severity'] = severity

            bulk_request_body += "\n".join([json.dumps(actions), json.dumps(source)]) + "\n"

    if bulk_request_body:
        logger.info("Transformed %d log entries for indexing to %d indexes", count + 1, len(created_indexes))
    else:
        logger.info("No log entries matched the namespace filter '%s'", NAMESPACE_FILTER)
    
    return bulk_request_body


def post(body):
    """
    Post bulk data to OpenSearch
    """
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, service, session_token=credentials.token)

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
            error = {"statusCode": response.status_code, "responseBody": {k: v for k, v in info.items() if k != 'items'}}
            log_failure(error, failed_items)
            raise Exception(f"OpenSearch indexing failed with status {response.status_code}")

        logger.info("OpenSearch indexing successful: %s", json.dumps(success))

    except requests.exceptions.RequestException as e:
        logger.error("Failed to send data to OpenSearch: %s", str(e), exc_info=True)
        raise e


def log_failure(error, failed_items):
    """
    Log failed indexing attempts
    """
    if log_failed_responses:
        logger.error("Failed OpenSearch response: %s", json.dumps(error, indent=2))
        if failed_items:
            logger.error("Failed Items (first 10): %s", json.dumps(failed_items[:10], indent=2))


# Utility functions for potential future use
def hmac_sha256(key, data, encoding='utf-8'):
    return hmac.new(key.encode(encoding), data.encode(encoding), hashlib.sha256).digest()

def sha256_hash(data, encoding='utf-8'):
    return hashlib.sha256(data.encode(encoding)).digest()

