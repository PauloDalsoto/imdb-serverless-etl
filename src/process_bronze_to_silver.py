import os
import json
import boto3
import logging
import pandas as pd
from datetime import datetime
from utils import with_retries

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SOURCE_S3_BUCKET = os.environ.get("SOURCE_S3_BUCKET")
TARGET_S3_BUCKET = os.environ.get("TARGET_S3_BUCKET")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BASE_DELAY_SECONDS = int(os.environ.get("BASE_DELAY_SECONDS", "1"))

# AWS client
s3 = boto3.client('s3')

def list_json_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    result = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json") and "_success" not in obj["Key"]:
                result.append(obj["Key"])
    return result

def load_json_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())

def normalize_records(json_objects):
    records = []
    for obj in json_objects:
        flattened = {
            k.lower(): v for k, v in obj.items()
            if isinstance(v, (str, int, float, bool))
        }
        records.append(flattened)
    return pd.DataFrame(records)

def save_csv_to_s3(bucket, key, dataframe):
    csv_data = dataframe.to_csv(index=False)
    with_retries(
        logger,
        MAX_RETRIES,
        BASE_DELAY_SECONDS,
        s3.put_object,
        f"Uploading silver file to {key}",
        Bucket=bucket,
        Key=key,
        Body=csv_data.encode("utf-8"),
        ContentType="text/csv"
    )
    logger.info(f"Saved silver file to s3://{bucket}/{key}")

def lambda_handler(event, context):
    logger.info("Starting process_bronze_to_silver Lambda...")

    try:
        record = event["Records"][0]
        key = record["s3"]["object"]["key"]
        date_str = key.split("/")[1]
        prefix = f"bronze/{date_str}/"
        logger.info(f"Triggered for date: {date_str}")

        object_keys = list_json_objects(TARGET_S3_BUCKET, prefix)
        if not object_keys:
            logger.warning(f"No .json objects found under {prefix}")
            return {"statusCode": 404, "body": f"No data to process under {prefix}"}

        json_objects = [load_json_from_s3(TARGET_S3_BUCKET, k) for k in object_keys]
        df = normalize_records(json_objects)

        output_key = f"silver/{date_str}.csv"
        save_csv_to_s3(TARGET_S3_BUCKET, output_key, df)

        return {"statusCode": 200, "body": f"Processed {len(df)} records for {date_str}"}

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"statusCode": 500, "body": str(e)}
