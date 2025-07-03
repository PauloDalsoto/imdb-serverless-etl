import time
import json

def with_retries(logger, max_retries, base_delay, func, description, *args, **kwargs):
    for attempt in range(max_retries):
        try:
            logger.info(f"{description} - Attempt {attempt + 1}")
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"{description} failed on attempt {attempt + 1}: {e}")
            time.sleep(base_delay * (2 ** attempt))
    logger.error(f"{description} failed after {max_retries} retries.")
    raise Exception(f"{description} failed after {max_retries} retries.")

def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "body": json.dumps({"message": body})
    }
