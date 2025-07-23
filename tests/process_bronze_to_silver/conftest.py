import pytest
import os
import boto3
from moto import mock_aws
from unittest.mock import patch
import sys
from pathlib import Path

process_bronze_to_silver_path = os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'process_bronze_to_silver')
process_bronze_to_silver_path = os.path.abspath(process_bronze_to_silver_path)
if process_bronze_to_silver_path not in sys.path:
    sys.path.insert(0, process_bronze_to_silver_path)

@pytest.fixture(autouse=True)
def cleanup_sys_path():
    """Fixture to clean up sys.path after tests."""
    yield
    if str(process_bronze_to_silver_path) in sys.path:
        sys.path.remove(str(process_bronze_to_silver_path))

@pytest.fixture
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture
def s3_buckets(aws_credentials):
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        target_bucket = 'test-target-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=target_bucket)
        yield {
            's3_client': s3,
            'source_bucket': source_bucket,
            'target_bucket': target_bucket
        }

@pytest.fixture
def environment_variables(s3_buckets):
    env_vars = {
        'S3_BUCKET_SOURCE': s3_buckets['source_bucket'],
        'S3_BUCKET_TARGET': s3_buckets['target_bucket'],
        'MAX_RETRIES': '3',
        'BASE_DELAY_SECONDS': '1'
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars

@pytest.fixture(autouse=True)
def cleanup_sys_path():
    """Fixture to clean up sys.path after tests."""
    yield
    if str(process_bronze_to_silver_path) in sys.path:
        sys.path.remove(str(process_bronze_to_silver_path))
