import pytest
import os
import boto3
import importlib
from unittest.mock import patch, MagicMock
from moto import mock_aws
import json

@pytest.fixture
def setup_test_environment():
    with mock_aws():
        with patch.dict(os.environ, {
            'S3_BUCKET_SOURCE': 'test-source-bucket',
            'S3_BUCKET_TARGET': 'test-target-bucket',
            'MAX_RETRIES': '3',
            'BASE_DELAY_SECONDS': '1'
        }):
            # Create S3 buckets
            s3 = boto3.client('s3', region_name='us-east-1')
            s3.create_bucket(Bucket='test-source-bucket')
            s3.create_bucket(Bucket='test-target-bucket')

            import lambdas.process_bronze_to_silver.process_bronze_to_silver as process_module
            importlib.reload(process_module)

            yield {
                's3_client': s3,
                'source_bucket': 'test-source-bucket',
                'target_bucket': 'test-target-bucket',
                'lambda_handler': process_module.lambda_handler
            }

def test_lambda_handler_success(setup_test_environment):
    env = setup_test_environment

    # Add mock data to the source bucket
    date_str = "2025-07-22"
    prefix = f"bronze/{date_str}/"
    mock_data = {"id": "tt1234567", "title": "Test Movie"}
    env['s3_client'].put_object(
        Bucket=env['source_bucket'],
        Key=f"{prefix}tt1234567.json",
        Body=json.dumps(mock_data)
    )

    event = {"date": date_str}
    result = env['lambda_handler'](event, None)

    assert result["statusCode"] == 200
    assert f"Processed 1 records for {date_str}" in result["body"]

    # Validate data in the target bucket
    response = env['s3_client'].get_object(Bucket=env['target_bucket'], Key="silver/movies_normalized.csv")
    stored_data = response['Body'].read().decode('utf-8')
    assert "Test Movie" in stored_data
