import pytest
import json
from unittest.mock import MagicMock, patch
from lambdas.fetch_top_movies.src.sqs_service import SQSService


@pytest.fixture
def mock_sqs_client():
    return MagicMock()


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def sqs_service(mock_sqs_client, mock_logger):
    return SQSService(
        sqs_client=mock_sqs_client,
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        max_retries=3,
        base_delay=0.1,
        logger=mock_logger
    )


def test_sqs_service_init(sqs_service, mock_sqs_client, mock_logger):
    assert sqs_service.sqs == mock_sqs_client
    assert sqs_service.queue_url == "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    assert sqs_service.max_retries == 3
    assert sqs_service.base_delay == 0.1
    assert sqs_service.logger == mock_logger


def test_send_single_success(sqs_service, mock_sqs_client):
    message_body = json.dumps({"movies": ["movie1", "movie2"], "is_final_batch": False})
    message_group_id = "movies-group"
    deduplication_id = "test-dedup-id"
    
    result = sqs_service._send_single(message_body, message_group_id, deduplication_id)
    
    assert result is True
    mock_sqs_client.send_message.assert_called_once_with(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        MessageBody=message_body,
        MessageGroupId=message_group_id,
        MessageDeduplicationId=deduplication_id
    )


def test_send_single_with_sqs_error(sqs_service, mock_sqs_client):
    mock_sqs_client.send_message.side_effect = Exception("SQS Error")
    message_body = json.dumps({"movies": ["movie1"], "is_final_batch": True})
    message_group_id = "movies-group"
    deduplication_id = "test-dedup-id"
    
    with pytest.raises(Exception, match="SQS Error"):
        sqs_service._send_single(message_body, message_group_id, deduplication_id)


@patch('lambdas.fetch_top_movies.src.sqs_service.with_retries')
@patch('lambdas.fetch_top_movies.src.sqs_service.uuid.uuid4')
def test_send_batch_success(mock_uuid, mock_with_retries, sqs_service, mock_logger):
    mock_uuid.return_value = "test-uuid"
    mock_with_retries.return_value = True
    
    batch = ["movie1", "movie2", "movie3"]
    is_final_batch = False
    
    result = sqs_service.send_batch(batch, is_final_batch)
    
    expected_message_body = json.dumps({
        "movies": batch,
        "is_final_batch": is_final_batch
    })
    
    assert result is True
    mock_with_retries.assert_called_once_with(
        mock_logger,
        3,
        0.1,
        sqs_service._send_single,
        "Sending batch with 3 movies to SQS",
        expected_message_body,
        "movies-group",
        "test-uuid"
    )


@patch('lambdas.fetch_top_movies.src.sqs_service.with_retries')
@patch('lambdas.fetch_top_movies.src.sqs_service.uuid.uuid4')
def test_send_batch_final_batch(mock_uuid, mock_with_retries, sqs_service, mock_logger):
    mock_uuid.return_value = "test-uuid-final"
    mock_with_retries.return_value = True
    
    batch = ["movie1", "movie2"]
    is_final_batch = True
    
    result = sqs_service.send_batch(batch, is_final_batch)
    
    expected_message_body = json.dumps({
        "movies": batch,
        "is_final_batch": is_final_batch
    })
    
    assert result is True
    mock_with_retries.assert_called_once_with(
        mock_logger,
        3,
        0.1,
        sqs_service._send_single,
        "Sending batch with 2 movies to SQS",
        expected_message_body,
        "movies-group",
        "test-uuid-final"
    )


@patch('lambdas.fetch_top_movies.src.sqs_service.with_retries')
@patch('lambdas.fetch_top_movies.src.sqs_service.uuid.uuid4')
def test_send_batch_empty_batch(mock_uuid, mock_with_retries, sqs_service, mock_logger):
    mock_uuid.return_value = "test-uuid-empty"
    mock_with_retries.return_value = True
    
    batch = []
    is_final_batch = False
    
    result = sqs_service.send_batch(batch, is_final_batch)
    
    expected_message_body = json.dumps({
        "movies": batch,
        "is_final_batch": is_final_batch
    })
    
    assert result is True
    mock_with_retries.assert_called_once_with(
        mock_logger,
        3,
        0.1,
        sqs_service._send_single,
        "Sending batch with 0 movies to SQS",
        expected_message_body,
        "movies-group",
        "test-uuid-empty"
    )


@patch('lambdas.fetch_top_movies.src.sqs_service.with_retries')
@patch('lambdas.fetch_top_movies.src.sqs_service.uuid.uuid4')
def test_send_batch_with_retries_failure(mock_uuid, mock_with_retries, sqs_service, mock_logger):
    mock_uuid.return_value = "test-uuid-fail"
    mock_with_retries.side_effect = Exception("Retry failed")
    
    batch = ["movie1"]
    
    with pytest.raises(Exception, match="Retry failed"):
        sqs_service.send_batch(batch)


def test_send_batch_message_body_format(sqs_service):
    batch = ["movie1", "movie2"]
    is_final_batch = True
    
    with patch('lambdas.fetch_top_movies.src.sqs_service.with_retries') as mock_with_retries, \
         patch('lambdas.fetch_top_movies.src.sqs_service.uuid.uuid4') as mock_uuid:
        
        mock_uuid.return_value = "test-uuid"
        mock_with_retries.return_value = True
        
        sqs_service.send_batch(batch, is_final_batch)
        
       
        call_args = mock_with_retries.call_args
        message_body = call_args[0][5] 
        
        parsed_body = json.loads(message_body)
        assert parsed_body["movies"] == batch
        assert parsed_body["is_final_batch"] == is_final_batch


def test_send_batch_default_final_batch_false(sqs_service):
    batch = ["movie1"]
    
    with patch('lambdas.fetch_top_movies.src.sqs_service.with_retries') as mock_with_retries, \
         patch('lambdas.fetch_top_movies.src.sqs_service.uuid.uuid4') as mock_uuid:
        
        mock_uuid.return_value = "test-uuid"
        mock_with_retries.return_value = True
        
        sqs_service.send_batch(batch)  
        
        call_args = mock_with_retries.call_args
        message_body = call_args[0][5] 
        
        parsed_body = json.loads(message_body)
        assert parsed_body["is_final_batch"] is False
