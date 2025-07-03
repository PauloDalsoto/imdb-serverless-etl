import json
from botocore.exceptions import ClientError
from .utils import with_retries

CONTENT_TYPE_JSON = "application/json"

class S3Service:
    def __init__(self, client, max_retries, base_delay, logger):
        self.client = client
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = logger

    def upload_json(self, bucket, key, data):
        try:
            body = json.dumps(data, indent=2)
            return self.upload_string(bucket, key, body)
        except Exception as e:
            self.logger.error(f"Failed to convert data to JSON: {e}")
            return False

    def upload_string(self, bucket, key, body):
        try:
            with_retries(
                self.logger,
                self.max_retries,
                self.base_delay,
                self.client.put_object,
                f"Uploading to s3://{bucket}/{key}",
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType=CONTENT_TYPE_JSON
            )
            self.logger.info(f"Uploaded to s3://{bucket}/{key}")
            return True
        except ClientError as e:
            self.logger.error(f"Upload to S3 failed: {e}")
            return False
