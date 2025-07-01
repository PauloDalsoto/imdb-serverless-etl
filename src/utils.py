import json
import time

def with_retries(logger, max_retries, base_delay, fn, description, *args, **kwargs):
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"{description} (Attempt {attempt})")
            return fn(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error during {description} (Attempt {attempt}): {e}")
            if attempt < max_retries:
                time.sleep(base_delay * (2 ** (attempt - 1)))
            else:
                raise

def build_response(status_code, message):
    return {
        "statusCode": status_code,
        "body": json.dumps(message),
    }
