import boto3
import os
import csv
import gzip
import time

EFS_MOUNT_PATH = "/mnt/efs"
SOURCE_BUCKET = "cg-s3-dev-efs-application-log"
INVENTORY_BUCKET = "cg-s3-dev-efs-application-log-object-inventory"
INVENTORY_PREFIX = "cg-s3-dev-efs-application-log/cg-s3-dev-efs-application-log-inventory/data/"
RETENTION_DAYS = 90

s3 = boto3.client("s3")

def get_latest_inventory_key():
    paginator = s3.get_paginator("list_objects_v2")
    latest = None

    for page in paginator.paginate(Bucket=INVENTORY_BUCKET, Prefix=INVENTORY_PREFIX):
        for obj in page.get("Contents", []):
            if not latest or obj["LastModified"] > latest["LastModified"]:
                latest = obj

    return latest["Key"]

def load_inventory_keys(inventory_key):
    response = s3.get_object(Bucket=INVENTORY_BUCKET, Key=inventory_key)
    keys = set()

    with gzip.GzipFile(fileobj=response["Body"]) as gz:
        reader = csv.reader(line.decode("utf-8") for line in gz)
        next(reader)
        for row in reader:
            keys.add(row[1])

    return keys

def lambda_handler(event, context):
    now = time.time()
    retention_seconds = RETENTION_DAYS * 86400

    inventory_key = get_latest_inventory_key()
    s3_keys = load_inventory_keys(inventory_key)

    deleted = []
    skipped = []

    for root, _, files in os.walk(EFS_MOUNT_PATH):
        for file in files:
            if not file.endswith(".gz"):
                continue

            full_path = os.path.join(root, file)
            age = now - os.stat(full_path).st_mtime

            if age < retention_seconds:
                continue

            s3_key = full_path.replace(EFS_MOUNT_PATH + "/", "")

            if s3_key in s3_keys:
                os.remove(full_path)
                deleted.append(full_path)
            else:
                skipped.append(full_path)

    return {
        "statusCode": 200,
        "deleted_count": len(deleted),
        "skipped_count": len(skipped)
    }
