import os
from unittest.mock import patch
from lambdas.fetch_top_movies.fetch_top_movies import lambda_handler


def test_success_scenario(mock_services):
    mocks = mock_services
    
    mocks['imdb'].fetch_movie_data.return_value = {"items": [{"rank": "1", "id": "a"}]}
    mocks['imdb'].get_top_rated_movies.return_value = [{"rank": 1, "id": "a"}]
    mocks['sqs'].send_batch.return_value = True
    
    result = lambda_handler({"top_n": 1, "batch_size": 1}, None)
    
    assert result["statusCode"] == 200
    assert mocks['sqs'].send_batch.called


def test_missing_env_vars(mock_services):
    with patch.dict(os.environ, {'SQS_QUEUE_URL': '', 'IMDB_DATA_URL': ''}):
        result = lambda_handler({}, None)
        assert result["statusCode"] == 500


def test_imdb_fails(mock_services):
    mocks = mock_services
    mocks['imdb'].fetch_movie_data.return_value = None
    
    result = lambda_handler({}, None)
    assert result["statusCode"] == 500


def test_no_movies_found(mock_services):
    mocks = mock_services
    mocks['imdb'].fetch_movie_data.return_value = {"items": []}
    mocks['imdb'].get_top_rated_movies.return_value = []
    
    result = lambda_handler({}, None)
    assert result["statusCode"] == 500


def test_sqs_fails(mock_services):
    mocks = mock_services
    
    mocks['imdb'].fetch_movie_data.return_value = {"items": [{"rank": "1", "id": "a"}]}
    mocks['imdb'].get_top_rated_movies.return_value = [{"rank": 1, "id": "a"}]
    mocks['sqs'].send_batch.return_value = False
    
    result = lambda_handler({}, None)
    assert result["statusCode"] == 500
