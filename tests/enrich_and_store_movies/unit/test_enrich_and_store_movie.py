import pytest
import json
import os
from unittest.mock import MagicMock, patch
from datetime import datetime
from lambdas.enrich_and_store_movies.enrich_and_store_movie import lambda_handler

@pytest.fixture
def mock_event():
    return {
        "Records": [
            {
                "messageId": "test-message-1",
                "body": json.dumps({
                    "movies": [
                        {
                            "id": "tt0111161",
                            "title": "The Shawshank Redemption",
                            "rank": "1"
                        },
                        {
                            "id": "tt0068646",
                            "title": "The Godfather",
                            "rank": "2"
                        }
                    ],
                    "is_final_batch": False
                })
            }
        ]
    }

@pytest.fixture
def mock_final_batch_event():
    return {
        "Records": [
            {
                "messageId": "test-message-final",
                "body": json.dumps({
                    "movies": [
                        {
                            "id": "tt0071562",
                            "title": "The Godfather Part II",
                            "rank": "3"
                        }
                    ],
                    "is_final_batch": True
                })
            }
        ]
    }

@pytest.fixture
def mock_empty_event():
    return {"Records": []}

@pytest.fixture
def mock_omdb_data():
    return {
        "Title": "The Shawshank Redemption",
        "Year": "1994",
        "Runtime": "142 min",
        "Genre": "Drama",
        "Director": "Frank Darabont",
        "imdbRating": "9.3",
        "Response": "True"
    }

@pytest.fixture
def mock_context():
    return MagicMock()

class TestEnrichAndStoreMovie:
    
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_success(self, mock_secrets_service_class, mock_datetime, mock_services, 
                                   mock_event, mock_context, mock_omdb_data):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        mocks['omdb'].fetch_movie_data.return_value = mock_omdb_data
        mocks['s3'].upload_json.return_value = True
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 200
        assert "All records processed" in result["body"]
        assert mocks['s3'].upload_json.call_count == 2

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_final_batch_success(self, mock_secrets_service_class, mock_datetime, mock_services, 
                                               mock_final_batch_event, mock_context, mock_omdb_data):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        mocks['omdb'].fetch_movie_data.return_value = mock_omdb_data
        mocks['s3'].upload_json.return_value = True
        mocks['s3'].upload_string.return_value = True
        
        result = lambda_handler(mock_final_batch_event, mock_context)
        
        assert result["statusCode"] == 200
        assert "All records processed" in result["body"]
        mocks['s3'].upload_json.assert_called_once()
        mocks['s3'].upload_string.assert_called_once()

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_secrets_error(self, mock_secrets_service_class, mock_services, mock_event, mock_context):
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.side_effect = Exception("Secret not found")
        mock_secrets_service_class.return_value = mock_secrets_service
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Secret not found" in result["body"]

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_empty_records(self, mock_secrets_service_class, mock_datetime, mock_services, 
                                         mock_empty_event, mock_context):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        result = lambda_handler(mock_empty_event, mock_context)
        
        assert result["statusCode"] == 200
        assert "All records processed" in result["body"]
        mocks['s3'].upload_json.assert_not_called()

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_invalid_movies_type(self, mock_secrets_service_class, mock_datetime, mock_services, mock_context):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        invalid_event = {
            "Records": [
                {
                    "messageId": "test-message-invalid",
                    "body": json.dumps({
                        "movies": "invalid_type",
                        "is_final_batch": False
                    })
                }
            ]
        }
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        result = lambda_handler(invalid_event, mock_context)
        
        assert result["statusCode"] == 200
        assert "All records processed" in result["body"]
        mocks['s3'].upload_json.assert_not_called()

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_movie_without_id(self, mock_secrets_service_class, mock_datetime, mock_services, mock_context):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        event_without_id = {
            "Records": [
                {
                    "messageId": "test-message-no-id",
                    "body": json.dumps({
                        "movies": [
                            {
                                "title": "Movie Without ID",
                                "rank": "1"
                            }
                        ],
                        "is_final_batch": False
                    })
                }
            ]
        }
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        result = lambda_handler(event_without_id, mock_context)
        
        assert result["statusCode"] == 200
        assert "All records processed" in result["body"]
        mocks['s3'].upload_json.assert_not_called()

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_s3_upload_failure(self, mock_secrets_service_class, mock_datetime, mock_services, 
                                             mock_event, mock_context, mock_omdb_data):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        mocks['omdb'].fetch_movie_data.return_value = mock_omdb_data
        mocks['s3'].upload_json.return_value = False
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Failed to upload tt0111161 to S3" in result["body"]

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_success_marker_failure(self, mock_secrets_service_class, mock_datetime, mock_services, 
                                                   mock_final_batch_event, mock_context, mock_omdb_data):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        mocks['omdb'].fetch_movie_data.return_value = mock_omdb_data
        mocks['s3'].upload_json.return_value = True
        mocks['s3'].upload_string.return_value = False
        
        result = lambda_handler(mock_final_batch_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Failed to write _SUCCESS marker to S3" in result["body"]

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_omdb_returns_none(self, mock_secrets_service_class, mock_datetime, mock_services, 
                                             mock_event, mock_context):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        mocks['omdb'].fetch_movie_data.return_value = None
        mocks['s3'].upload_json.return_value = True
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 200
        assert "All records processed" in result["body"]
        mocks['s3'].upload_json.assert_called()

    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.datetime')
    @patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.SecretsService')
    def test_lambda_handler_json_decode_error(self, mock_secrets_service_class, mock_datetime, mock_services, mock_context):
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        
        mocks = mock_services
        
        invalid_json_event = {
            "Records": [
                {
                    "messageId": "test-message-invalid-json",
                    "body": "invalid json"
                }
            ]
        }
        
        mock_secrets_service = MagicMock()
        mock_secrets_service.get_omdb_api_key.return_value = "test_api_key"
        mock_secrets_service_class.return_value = mock_secrets_service
        
        result = lambda_handler(invalid_json_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Error in message ID test-message-invalid-json" in result["body"]
