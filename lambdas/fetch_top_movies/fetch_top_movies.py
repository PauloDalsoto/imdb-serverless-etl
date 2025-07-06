import os
import logging
import boto3

from src.utils import build_response
from src.imdb_service import IMDBService
from src.sqs_service import SQSService

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Config (env variables)
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
IMDB_DATA_URL = os.environ.get("IMDB_DATA_URL")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
BASE_DELAY_SECONDS = int(os.environ.get("BASE_DELAY_SECONDS", 1))

def lambda_handler(event, context):
    logger.info("Starting GetMoviesAndSendToQueue function")

    if not SQS_QUEUE_URL or not IMDB_DATA_URL:
        logger.error("Missing environment variables.")
        return build_response(500, "Missing environment variables.")

    top_n = event.get('top_n', 10)
    batch_size = event.get('batch_size', 1)
    logger.info(f"Event params: top_n={top_n}, batch_size={batch_size}")

    imdb_service = IMDBService(
        url=IMDB_DATA_URL,
        max_retries=MAX_RETRIES,
        base_delay=BASE_DELAY_SECONDS,
        logger=logger
    )

    sqs_service = SQSService(
        sqs_client=boto3.client('sqs'),
        queue_url=SQS_QUEUE_URL,
        max_retries=MAX_RETRIES,
        base_delay=BASE_DELAY_SECONDS,
        logger=logger
    )

    data = imdb_service.fetch_movie_data()
    if not data or 'items' not in data:
        logger.error("Invalid or missing movie data.")
        return build_response(500, "Failed to fetch movie data.")

    top_movies = imdb_service.get_top_rated_movies(data['items'], top_n)
    if not top_movies:
        logger.error("No valid top movies after sorting.")
        return build_response(500, "No valid movies to send.")

    batches = [top_movies[i:i + batch_size] for i in range(0, len(top_movies), batch_size)]
    logger.info(f"Prepared {len(batches)} message(s) to send to SQS, each with up to {batch_size} movies.")

    for idx, batch in enumerate(batches):
        is_last = (idx == len(batches) - 1)
        if not sqs_service.send_batch(batch, is_final_batch=is_last):
            logger.error("Batch sending failed. Aborting.")
            return build_response(500, "Batch sending failed.")

    logger.info("All batches sent successfully.")
    return build_response(200, "All movies sent to SQS.")
