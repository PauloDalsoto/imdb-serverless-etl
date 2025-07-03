import os
import logging
from datetime import datetime
from src.processor import BronzeToSilverProcessor
from src.s3_service import S3Service
from src.utils import build_response

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET_SOURCE = os.environ.get("S3_BUCKET_SOURCE")
S3_BUCKET_TARGET = os.environ.get("S3_BUCKET_TARGET")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BASE_DELAY_SECONDS = int(os.environ.get("BASE_DELAY_SECONDS", "1"))

def lambda_handler(event, context):
    logger.info("Starting process_bronze_to_silver Lambda...")

    try:
        date_str = event.get("date", datetime.now().strftime("%Y-%m-%d"))
        prefix = f"bronze/{date_str}/"
        logger.info(f"Triggered for date: {date_str}")

        s3_service = S3Service(logger, MAX_RETRIES, BASE_DELAY_SECONDS)
        processor = BronzeToSilverProcessor(s3_service, S3_BUCKET_SOURCE, S3_BUCKET_TARGET)

        record_count = processor.process(prefix)

        return build_response(200, f"Processed {record_count} records for {date_str}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return build_response(500, str(e))
