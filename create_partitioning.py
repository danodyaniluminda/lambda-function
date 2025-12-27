import os
import json
import ssl
import boto3
import pg8000

secrets_client = boto3.client("secretsmanager")

def get_db_secret():
    secret_arn = os.environ["SECRET_ARN"]
    response = secrets_client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response["SecretString"])
    return secret

def lambda_handler(event, context):
    secret = get_db_secret()

    conn = pg8000.connect(
        host=secret["host"],
        port=int(secret["port"]),
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        ssl_context=ssl.create_default_context()
    )

    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM d_bot_schema.create_audit_log_partitions();"
            )
            try:
                create_result = cur.fetchall()
            except Exception:
                create_result = "Executed (no rows)"

            cur.execute(
                "SELECT * FROM d_bot_schema.cleanup_old_audit_log_partitions();"
            )
            try:
                cleanup_result = cur.fetchall()
            except Exception:
                cleanup_result = "Executed (no rows)"

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "create_audit_log_partitions": create_result,
                    "cleanup_old_audit_log_partitions": cleanup_result
                },
                default=str
            )
        }

    finally:
        conn.close()
