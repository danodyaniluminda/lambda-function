import os
import boto3
import csv
import gzip
import time

EFS_MOUNT_PATH = os.environ.get("EFS_MOUNT_PATH")
INVENTORY_BUCKET = os.environ.get("INVENTORY_BUCKET")
INVENTORY_PREFIX = os.environ.get("INVENTORY_PREFIX")
RETENTION_DAYS = os.environ.get("RETENTION_DAYS")
RH_RETENTION_DAYS = os.environ.get("RH_RETENTION_DAYS")
RH_PREFIX = os.environ.get("RH_PREFIX")

if not all([
    EFS_MOUNT_PATH,
    INVENTORY_BUCKET,
    INVENTORY_PREFIX,
    RETENTION_DAYS,
    RH_RETENTION_DAYS,
    RH_PREFIX
]):
    raise Exception("Missing required environment variables")

RETENTION_DAYS = int(RETENTION_DAYS)
RH_RETENTION_DAYS = int(RH_RETENTION_DAYS)

s3 = boto3.client("s3")

def get_latest_inventory_key():
    paginator = s3.get_paginator("list_objects_v2")
    latest = None
    for page in paginator.paginate(Bucket=INVENTORY_BUCKET, Prefix=INVENTORY_PREFIX):
        for obj in page.get("Contents", []):
            if not latest or obj["LastModified"] > latest["LastModified"]:
                latest = obj
    if not latest:
        raise Exception("No inventory file found in S3")
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

    default_retention_seconds = RETENTION_DAYS * 86400
    rh_retention_seconds = RH_RETENTION_DAYS * 86400

    inventory_key = get_latest_inventory_key()
    s3_keys = load_inventory_keys(inventory_key)

    deleted_count = 0
    skipped_count = 0

    for root, _, files in os.walk(EFS_MOUNT_PATH):
        for file in files:
            if not file.endswith(".gz"):
                continue

            full_path = os.path.join(root, file)
            relative_path = full_path.replace(EFS_MOUNT_PATH + "/", "")
            age = now - os.stat(full_path).st_mtime

            if RH_PREFIX in relative_path:
                retention_seconds = rh_retention_seconds
            else:
                retention_seconds = default_retention_seconds

            if age < retention_seconds:
                skipped_count += 1
                continue

            if relative_path in s3_keys:
                try:
                    os.remove(full_path)
                    deleted_count += 1
                except Exception as e:
                    skipped_count += 1
                    print(f"Error deleting {full_path}: {e}")
            else:
                skipped_count += 1

    print(f"Total deleted files: {deleted_count}")
    print(f"Total skipped files: {skipped_count}")

    return {
        "statusCode": 200,
        "deleted_count": deleted_count,
        "skipped_count": skipped_count
    }
