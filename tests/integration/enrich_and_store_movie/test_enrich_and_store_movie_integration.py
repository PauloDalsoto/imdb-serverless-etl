import pytest
import json
import os
import boto3
import importlib
from unittest.mock import patch, MagicMock
from moto import mock_aws
from datetime import datetime

@pytest.fixture
def setup_test_environment():
    with mock_aws():
        with patch.dict(os.environ, {
            'S3_BUCKET_NAME': 'test-integration-bucket',
            'OMDB_API_KEY': 'test-key',
            'MAX_RETRIES': '3',
            'BASE_DELAY_SECONDS': '1'
        }):
            # Create S3 bucket
            s3 = boto3.client('s3', region_name='us-east-1')
            s3.create_bucket(Bucket='test-integration-bucket')

            # Mock Secrets Manager
            secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
            secrets_manager.create_secret(
                Name='test_omdb_secret',
                SecretString='{"OMDB_API_KEY": "test-key"}'
            )

            import lambdas.enrich_and_store_movies.enrich_and_store_movie as enrich_module
            importlib.reload(enrich_module)

            yield {
                's3_client': s3,
                'bucket_name': 'test-integration-bucket',
                'lambda_handler': enrich_module.lambda_handler
            }

def test_lambda_handler_success(setup_test_environment):
    env = setup_test_environment

    with patch('lambdas.enrich_and_store_movies.src.omdb_service.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "Title": "Test Movie",
            "Year": "2025",
            "Genre": "Action",
            "Director": "John Doe"
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        event = {
            "Records": [
                {
                    "messageId": "1",
                    "body": json.dumps({
                        "movies": [
                            {"id": "tt1234567", "title": "Test Movie"}
                        ],
                        "is_final_batch": True
                    })
                }
            ]
        }
        result = env['lambda_handler'](event, None)

        assert result["statusCode"] == 200
        assert "All records processed." in result["body"]

        # Validate S3 object
        s3_key = f"bronze/{datetime.now().strftime('%Y-%m-%d')}/tt1234567.json"
        response = env['s3_client'].get_object(Bucket=env['bucket_name'], Key=s3_key)
        stored_data = json.loads(response['Body'].read().decode('utf-8'))
        assert stored_data['Title'] == "Test Movie"
        assert stored_data['Year'] == "2025"
        assert stored_data['Genre'] == "Action"
        assert stored_data['Director'] == "John Doe"