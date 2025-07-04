import pytest
import json
import os
import boto3
import importlib
from unittest.mock import patch, MagicMock
from moto import mock_aws
from lambdas.fetch_top_movies.fetch_top_movies import lambda_handler

@pytest.fixture
def setup_test_environment():
    with mock_aws():
        with patch.dict(os.environ, {
            'SQS_QUEUE_URL': 'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue.fifo',
            'IMDB_DATA_URL': 'https://imdb-api.com/en/API/Top250Movies/test-key',
            'MAX_RETRIES': '3',
            'BASE_DELAY_SECONDS': '1'
        }):
            # Create SQS queue
            sqs = boto3.client('sqs', region_name='us-east-1')
            queue_url = sqs.create_queue(
                QueueName='test-queue.fifo',
                Attributes={
                    'FifoQueue': 'true',
                    'ContentBasedDeduplication': 'false'
                }
            )['QueueUrl']
            
            import lambdas.fetch_top_movies.fetch_top_movies as fetch_module
            importlib.reload(fetch_module)
            
            yield {
                'sqs_client': sqs,
                'queue_url': queue_url,
                'lambda_handler': fetch_module.lambda_handler
            }


def test_lambda_handler_full_integration_success(setup_test_environment, mock_imdb_data):
    env = setup_test_environment
    
    with patch('lambdas.fetch_top_movies.src.imdb_service.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = mock_imdb_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        event = {"top_n": 3, "batch_size": 2}
        result = env['lambda_handler'](event, None)
        
        assert result["statusCode"] == 200
        assert "All movies sent to SQS" in result["body"]
        
        messages = env['sqs_client'].receive_message(QueueUrl=env['queue_url'], MaxNumberOfMessages=10)
        assert 'Messages' in messages
        assert len(messages['Messages']) == 2
        
        first_message = json.loads(messages['Messages'][0]['Body'])
        assert len(first_message['movies']) == 2
        assert first_message['is_final_batch'] is False
        
        second_message = json.loads(messages['Messages'][1]['Body'])
        assert len(second_message['movies']) == 1
        assert second_message['is_final_batch'] is True


def test_lambda_handler_single_batch(setup_test_environment, mock_imdb_data):
    env = setup_test_environment
    
    with patch('lambdas.fetch_top_movies.src.imdb_service.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = mock_imdb_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        event = {"top_n": 3, "batch_size": 5}
        result = env['lambda_handler'](event, None)
        
        assert result["statusCode"] == 200
        
        messages = env['sqs_client'].receive_message(QueueUrl=env['queue_url'], MaxNumberOfMessages=10)
        assert 'Messages' in messages
        assert len(messages['Messages']) == 1
        
        message = json.loads(messages['Messages'][0]['Body'])
        assert len(message['movies']) == 3
        assert message['is_final_batch'] is True