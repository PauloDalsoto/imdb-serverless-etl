import json
import boto3

class S3Service:
    def __init__(self, logger, max_retries, base_delay):
        self.logger = logger
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.s3 = boto3.client("s3")

    def list_json_objects(self, bucket, prefix):
        paginator = self.s3.get_paginator("list_objects_v2")
        result = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json") and "_success" not in obj["Key"]:
                    result.append(obj["Key"])
        return result

    def load_json(self, bucket, key):
        response = self.s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read())

    def save_csv(self, bucket, key, csv_data):
        from src.utils import with_retries

        with_retries(
            self.logger,
            self.max_retries,
            self.base_delay,
            self.s3.put_object,
            f"Uploading silver file to {key}",
            Bucket=bucket,
            Key=key,
            Body=csv_data.encode("utf-8"),
            ContentType="text/csv"
        )
        self.logger.info(f"Saved silver file to s3://{bucket}/{key}")
