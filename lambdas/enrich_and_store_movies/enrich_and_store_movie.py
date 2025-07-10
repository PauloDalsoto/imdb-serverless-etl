import os
import json
import logging
from datetime import datetime

import boto3
from src.utils import build_response
from src.secrets_service import SecretsService
from src.omdb_service import OMDBService
from src.s3_service import S3Service

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Env vars
OMDB_API_SECRET_NAME = os.environ.get("OMDB_API_SECRET_NAME")
TARGET_S3_BUCKET = os.environ.get("TARGET_S3_BUCKET")
OMDB_URL = os.environ.get("OMDB_URL")

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BASE_DELAY_SECONDS = int(os.environ.get("BASE_DELAY_SECONDS", "1"))

today_str = datetime.now().strftime("%Y-%m-%d")

def lambda_handler(event, context):
    logger.info("Starting EnrichAndStoreMovie Lambda execution.")

    if not TARGET_S3_BUCKET:
        logger.error("Missing TARGET_S3_BUCKET environment variable.")
        return build_response(500, "Missing environment variables.")

    secrets_service = SecretsService(boto3.client("secretsmanager"), MAX_RETRIES, BASE_DELAY_SECONDS, logger)
    omdb_service = OMDBService(OMDB_URL, MAX_RETRIES, BASE_DELAY_SECONDS, logger)
    s3_service = S3Service(boto3.client("s3"), MAX_RETRIES, BASE_DELAY_SECONDS, logger)

    try:
        omdb_api_key = secrets_service.get_omdb_api_key(OMDB_API_SECRET_NAME)
    except Exception as e:
        logger.error(f"Secret error: {e}")
        return build_response(500, str(e))

    for record in event.get("Records", []):
        message_id = record.get("messageId", "N/A")
        try:
            body = json.loads(record["body"])
            movies = body.get("movies")
            is_final_batch = body.get("is_final_batch", False)

            if not isinstance(movies, list):
                logger.error(f"Message ID {message_id}: Invalid 'movies' type: {type(movies)}")
                continue

            for movie in movies:
                imdb_id = movie.get("id")
                title = movie.get("title", "Unknown Title")

                if not imdb_id:
                    logger.warning(f"Message ID {message_id}: Movie without ID. Skipping.")
                    continue

                logger.info(f"Processing: {title} (ID: {imdb_id})")

                enriched_data = omdb_service.fetch_movie_data(imdb_id, omdb_api_key) or {}
                enriched_movie = {**movie, **enriched_data}

                s3_key = f"bronze/{today_str}/{imdb_id}.json"
                if not s3_service.upload_json(TARGET_S3_BUCKET, s3_key, enriched_movie):
                    logger.error(f"Failed to upload {imdb_id} to S3.")
                    return build_response(500, f"Failed to upload {imdb_id} to S3.")

            logger.info(f"Processed message ID {message_id} with {len(movies)} movie(s).")

            if is_final_batch:
                logger.info("Final batch detected. Writing _SUCCESS marker...")
                if not s3_service.upload_string(TARGET_S3_BUCKET, f"bronze/{today_str}/_SUCCESS", " "):
                    logger.error("Failed to write _SUCCESS marker to S3.")
                    return build_response(500, "Failed to write _SUCCESS marker to S3.")

        except Exception as e:
            logger.error(f"Unhandled error in message ID {message_id}: {e}")
            return build_response(500, f"Error in message ID {message_id}")

    logger.info("Finished processing all records.")
    return build_response(200, "All records processed.")
    
