import json
import os
import boto3
import requests
import logging
from botocore.exceptions import ClientError
from utils import build_response, with_retries

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
secrets_client = boto3.client('secretsmanager')
s3_client = boto3.client('s3')

# Env vars
OMDB_API_SECRET_NAME = os.environ.get('OMDB_API_SECRET_NAME')
TARGET_S3_BUCKET = os.environ.get('TARGET_S3_BUCKET')
OMDB_URL = os.environ.get('OMDB_URL')

MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
BASE_DELAY_SECONDS = int(os.environ.get('BASE_DELAY_SECONDS', '1'))

CONTENT_TYPE_JSON = "application/json"

def get_secret(secret_name):
    response = with_retries(
        logger,
        MAX_RETRIES,
        BASE_DELAY_SECONDS,
        secrets_client.get_secret_value,
        f"Retrieving secret '{secret_name}'",
        SecretId=secret_name
    )
    return response.get('SecretString')

def get_omdb_api_key():
    if not OMDB_API_SECRET_NAME:
        return build_response(500, "Missing OMDB API secret name.")

    secret_string = get_secret(OMDB_API_SECRET_NAME)
    if not secret_string:
        return build_response(500, "OMDb API key secret not found or empty.")

    try:
        secret_json = json.loads(secret_string)
        return secret_json.get('omdbapi_key')
    except json.JSONDecodeError:
        return build_response(500, "OMDb secret is not a valid JSON string.")
    
def _perform_omdb_api_request_single(url):
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data

def call_omdb_api(imdb_id, omdb_api_key):
    if not OMDB_URL:
        return build_response(500, "OMDB_URL environment variable not set.")

    url = f"{OMDB_URL}/?apikey={omdb_api_key}&i={imdb_id}"

    try:
        omdb_data = with_retries(
            logger,
            MAX_RETRIES,
            BASE_DELAY_SECONDS,
            _perform_omdb_api_request_single,
            f"OMDb API call for ID: {imdb_id}",
            url
        )
        if omdb_data and omdb_data.get("Response") == "False":
            logger.warning(f"OMDb API returned explicit error for {imdb_id}: {omdb_data.get('Error')}")
            return None 
        
        return omdb_data
    except Exception as e:
        logger.error(f"Final failure after all retries for OMDb API call for {imdb_id}: {e}")
        return None

def put_object_to_s3(bucket_name, key, body):
    try:
        with_retries(
            logger,
            MAX_RETRIES,
            BASE_DELAY_SECONDS,
            s3_client.put_object,
            f"Uploading to s3://{bucket_name}/{key}",
            Bucket=bucket_name,
            Key=key,
            Body=body,
            ContentType=CONTENT_TYPE_JSON
        )
        logger.info(f"Uploaded to s3://{bucket_name}/{key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        return False

def lambda_handler(event, context):
    logger.info("Starting EnrichAndStoreMovie Lambda execution.")

    if not TARGET_S3_BUCKET:
        logger.error("Missing TARGET_S3_BUCKET environment variable.")
        return build_response(500, "Missing environment variables.")

    try:
        omdb_api_key = get_omdb_api_key()
    except ValueError as ve:
        logger.error(f"Secret error: {ve}")
        return build_response(500, str(ve))

    for record in event.get('Records', []):
        message_id = record.get('messageId', 'N/A')
        try:
            body = json.loads(record['body'])
            if not isinstance(body, list):
                logger.error(f"Message ID {message_id}: Expected a list, got: {type(body)}")
                continue

            for movie in body:
                imdb_id = movie.get('id')
                title = movie.get('title', 'Unknown Title')

                if not imdb_id:
                    logger.warning(f"Message ID {message_id}: Skipping movie with no ID.")
                    continue

                logger.info(f"Processing: {title} (ID: {imdb_id})")

                enriched_data = call_omdb_api(imdb_id, omdb_api_key) or {}
                enriched_movie = {**movie, **enriched_data}

                s3_key = f"bronze/{imdb_id}.json"
                if not put_object_to_s3(TARGET_S3_BUCKET, s3_key, json.dumps(enriched_movie, indent=2)):
                    logger.error(f"Failed to upload {imdb_id} to S3.")
                    return build_response(500, f"Failed to upload {imdb_id} to S3.")

            logger.info(f"Processed message ID {message_id} with {len(body)} movies.")

        except json.JSONDecodeError as e:
            logger.error(f"Message ID {message_id} has invalid JSON: {e}")
        except Exception as e:
            logger.error(f"Unhandled error processing message ID {message_id}: {e}")
            return build_response(500, f"Unhandled error processing message ID {message_id}: {e}")

    logger.info("Lambda finished processing all SQS records.")
    return build_response(200, "All SQS records processed.")
