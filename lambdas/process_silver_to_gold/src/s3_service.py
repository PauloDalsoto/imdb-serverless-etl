import io
import boto3
import pandas as pd
from src.utils import with_retries

class S3Service:
    def __init__(self, logger, max_retries, base_delay):
        self.logger = logger
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.s3 = boto3.client("s3")

    def load_csv(self, bucket, key):
        response = self.s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(response['Body'].read()))

    def save_csv(self, bucket, key, csv_data):
        with_retries(
            self.logger,
            self.max_retries,
            self.base_delay,
            self.s3.put_object,
            f"Uploading gold file to {key}",
            Bucket=bucket,
            Key=key,
            Body=csv_data.encode("utf-8"),
            ContentType="text/csv"
        )
        self.logger.info(f"Saved gold file to s3://{bucket}/{key}")
