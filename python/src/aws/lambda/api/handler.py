# lambda_function/handler.py

import os
import json
import boto3
import logging
from core.handlers import echo_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_secrets_into_env(secret_arn: str):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    secret_string = response.get("SecretString")

    if secret_string:
        secrets = json.loads(secret_string)
        for key, value in secrets.items():
            os.environ[key] = value


def load_local_secrets():
    """
    Loads environment variables from a JSON-formatted string stored in
    the LOCAL_SECRET_STRING environment variable. This string mimics the
    'SecretString' format from AWS Secrets Manager.
    """
    try:
        secret_string = os.environ.get("LOCAL_SECRET_STRING")
        if not secret_string:
            raise ValueError("LOCAL_SECRET_STRING environment variable is missing.")

        secrets = json.loads(secret_string)
        for key, value in secrets.items():
            os.environ[key] = value
        print("✅ Baked secrets loaded into environment variables.")
    except Exception as e:
        print(f"❌ Failed to load baked secrets: {e}")


def load_secrets():
    if os.environ.get("ENV") == "production":
        load_secrets_from_aws(os.environ["SECRETS_ARN"])
    else:
        load_local_secrets()


def load_secrets_from_aws(secret_arn: str):
    # Load secrets once per container reuse
    if "SECRETS_LOADED" not in os.environ:
        secret_arn = os.getenv("SECRETS_ARN")  # Set this in Lambda env config
        if secret_arn:
            load_secrets_into_env(secret_arn)
            os.environ["SECRETS_LOADED"] = "1"

            logger.info(f"Database: {os.environ.get('DB_TYPE')}")
        else:
            logger.warning("SECRETS_ARN not set in environment")


def lambda_handler(event, context):
    # Load secrets once per container reuse
    load_secrets()

    # Set up the database connection after we've loaded secrets
    from user import user_handler
    from core.db_loader import debug_connection

    path = event.get("path", "")
    http_method = event.get("httpMethod", "")
    query_params = event.get("queryStringParameters") or {}

    logger.info(f"Request body: {event.get('body')}")

    if path == "/echo" and http_method == "GET":
        response = echo_handler(query_params)
    elif path == "/debug" and http_method == "GET":
        response = debug_connection()
    elif path == "/register" and http_method == "POST":
        response = user_handler.register(json.loads(event.get("body")))
    else:
        return {"statusCode": 404, "body": json.dumps({"error": f"Not Found: {path}"})}

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response),
    }
