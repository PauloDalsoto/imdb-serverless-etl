import json
import os
import requests
import boto3
import logging
from utils import build_response, with_retries
import uuid

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
IMDB_DATA_URL = os.environ.get('IMDB_DATA_URL')
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
BASE_DELAY_SECONDS = int(os.environ.get('BASE_DELAY_SECONDS', '1'))

# AWS client
sqs = boto3.client('sqs')

def _fetch(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def fetch_movie_data(url):
    return with_retries(
        logger,
        MAX_RETRIES,
        BASE_DELAY_SECONDS,
        _fetch,
        f"Fetching data from: {url}",
        url
    )

def get_top_rated_movies(items, top_n):
    try:
        valid_movies = []
        for m in items:
            if 'rank' in m and m['rank'] not in ('', 'N/A'):
                try:
                    m['rank'] = int(m['rank'])
                    valid_movies.append(m)
                except ValueError:
                    logger.warning(f"Skipping movie with invalid rank: {m.get('id', 'N/A')}")
                    continue
        
        sorted_movies = sorted(
            valid_movies,
            key=lambda m: m['rank']
        )
        return sorted_movies[:top_n]
    
    except Exception as e:
        logger.error(f"Error sorting movies: {e}")
        return []
    
def _perform_sqs_send_single(queue_url, message_body, message_group_id, deduplication_id):
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body,
        MessageGroupId=message_group_id,
        MessageDeduplicationId=deduplication_id
    )
    return True 

def send_batch_to_sqs(batch, is_final_batch=False):
    message_body = json.dumps({
        "movies": batch,
        "is_final_batch": is_final_batch
    })
    description = f"Sending batch with {len(batch)} movies to SQS"

    try:
        return with_retries(
            logger,
            MAX_RETRIES,
            BASE_DELAY_SECONDS,
            _perform_sqs_send_single, 
            description,
            SQS_QUEUE_URL,           
            message_body ,
            "movies-group",
            str(uuid.uuid4())       
        )
    except Exception as e:
        logger.error(f"Final failure after all retries to send batch: {e}")
        return False

def lambda_handler(event, context):
    logger.info("Starting GetMoviesAndSendToQueue function")

    if not SQS_QUEUE_URL or not IMDB_DATA_URL:
        logger.error("Missing environment variables.")
        return build_response(500, "Missing environment variables.")

    top_n = event.get('top_n', 10)
    batch_size = event.get('batch_size', 1)
    logger.info(f"Event params: top_n={top_n}, batch_size={batch_size}")

    data = fetch_movie_data(IMDB_DATA_URL)
    if not data or 'items' not in data:
        logger.error("Invalid or missing movie data.")
        return build_response(500, "Failed to fetch movie data.")

    top_movies = get_top_rated_movies(data['items'], top_n)
    if not top_movies:
        logger.error("No valid top movies after sorting.")
        return build_response(500, "No valid movies to send.")

    batches_to_send = [top_movies[i:i + batch_size] for i in range(0, len(top_movies), batch_size)]
    logger.info(f"Prepared {len(batches_to_send)} message(s) to send to SQS, each containing up to {batch_size} movies.")

    for idx, batch in enumerate(batches_to_send):
        is_last = (idx == len(batches_to_send) - 1)
        
        if not send_batch_to_sqs(batch, is_final_batch=is_last):
            logger.error("Batch sending failed. Aborting.")
            return build_response(500, "Batch sending failed.")

    logger.info("All batches sent successfully.")
    return build_response(200, "All movies sent to SQS.")
