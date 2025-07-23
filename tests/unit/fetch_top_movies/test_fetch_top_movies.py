import os
import pytest
from unittest.mock import patch, MagicMock
from lambdas.fetch_top_movies.fetch_top_movies import lambda_handler

@pytest.fixture
def mock_event():
    return {
        "top_n": 10,
        "batch_size": 5
    }

@pytest.fixture
def mock_context():
    return MagicMock()

@pytest.fixture
def mock_imdb_data():
    return {
        "items": [
            {
                "id": "tt0111161",
                "rank": "1",
                "title": "The Shawshank Redemption",
                "fullTitle": "The Shawshank Redemption (1994)",
                "year": "1994",
                "imDbRating": "9.3"
            },
            {
                "id": "tt0068646",
                "rank": "2",
                "title": "The Godfather",
                "fullTitle": "The Godfather (1972)",
                "year": "1972",
                "imDbRating": "9.2"
            },
            {
                "id": "tt0071562",
                "rank": "3",
                "title": "The Godfather Part II",
                "fullTitle": "The Godfather Part II (1974)",
                "year": "1974",
                "imDbRating": "9.0"
            }
        ]
    }

class TestFetchTopMovies:
    
    def test_lambda_handler_success(self, mock_services, mock_event, mock_context, mock_imdb_data):
        mocks = mock_services
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"]
        mocks['sqs'].send_batch.return_value = True
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 200
        assert "All movies sent to SQS" in result["body"]
        mocks['imdb'].fetch_movie_data.assert_called_once()
        mocks['imdb'].get_top_rated_movies.assert_called_once()
        mocks['sqs'].send_batch.assert_called()

    def test_lambda_handler_default_parameters(self, mock_services, mock_context, mock_imdb_data):
        mocks = mock_services
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"][:10]
        mocks['sqs'].send_batch.return_value = True
        
        result = lambda_handler({}, mock_context)
        
        assert result["statusCode"] == 200
        mocks['imdb'].get_top_rated_movies.assert_called_once_with(mock_imdb_data["items"], 10)

    def test_lambda_handler_custom_parameters(self, mock_services, mock_context, mock_imdb_data):
        mocks = mock_services
        
        event = {"top_n": 5, "batch_size": 2}
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"][:5]
        mocks['sqs'].send_batch.return_value = True
        
        result = lambda_handler(event, mock_context)
        
        assert result["statusCode"] == 200
        mocks['imdb'].get_top_rated_movies.assert_called_once_with(mock_imdb_data["items"], 5)

    def test_lambda_handler_imdb_fetch_fails(self, mock_services, mock_event, mock_context):
        mocks = mock_services
        mocks['imdb'].fetch_movie_data.return_value = None
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Failed to fetch movie data" in result["body"]

    def test_lambda_handler_imdb_fetch_exception(self, mock_services, mock_event, mock_context):
        mocks = mock_services
        mocks['imdb'].fetch_movie_data.side_effect = Exception("IMDB API Error")
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Failed to fetch movie data" in result["body"]

    def test_lambda_handler_invalid_imdb_data(self, mock_services, mock_event, mock_context):
        mocks = mock_services
        mocks['imdb'].fetch_movie_data.return_value = {"invalid": "data"}
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Failed to fetch movie data" in result["body"]

    def test_lambda_handler_no_movies_found(self, mock_services, mock_event, mock_context):
        mocks = mock_services
        mocks['imdb'].fetch_movie_data.return_value = {"items": []}
        mocks['imdb'].get_top_rated_movies.return_value = []
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "No valid movies to send" in result["body"]

    def test_lambda_handler_sqs_send_fails(self, mock_services, mock_event, mock_context, mock_imdb_data):
        mocks = mock_services
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"]
        mocks['sqs'].send_batch.return_value = False
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Batch sending failed" in result["body"]

    def test_lambda_handler_sqs_send_exception(self, mock_services, mock_event, mock_context, mock_imdb_data):
        mocks = mock_services
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"]
        mocks['sqs'].send_batch.side_effect = Exception("SQS Error")
        
        result = lambda_handler(mock_event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Failed to send batches to SQS" in result["body"]

    def test_lambda_handler_multiple_batches(self, mock_services, mock_context, mock_imdb_data):
        mocks = mock_services
        
        event = {"top_n": 3, "batch_size": 2}
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"][:3]
        mocks['sqs'].send_batch.return_value = True
        
        result = lambda_handler(event, mock_context)
        
        assert result["statusCode"] == 200
        assert mocks['sqs'].send_batch.call_count == 2

    def test_lambda_handler_single_batch(self, mock_services, mock_context, mock_imdb_data):
        mocks = mock_services
        
        event = {"top_n": 2, "batch_size": 5}
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"][:2]
        mocks['sqs'].send_batch.return_value = True
        
        result = lambda_handler(event, mock_context)
        
        assert result["statusCode"] == 200
        mocks['sqs'].send_batch.assert_called_once()

    def test_lambda_handler_batch_final_flag(self, mock_services, mock_context, mock_imdb_data):
        mocks = mock_services
        
        event = {"top_n": 3, "batch_size": 2}
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"][:3]
        mocks['sqs'].send_batch.return_value = True
        
        result = lambda_handler(event, mock_context)
        
        assert result["statusCode"] == 200
        
        calls = mocks['sqs'].send_batch.call_args_list
        assert len(calls) == 2
        assert calls[0][1]['is_final_batch'] == False
        assert calls[1][1]['is_final_batch'] == True

    def test_lambda_handler_first_batch_fails(self, mock_services, mock_context, mock_imdb_data):
        mocks = mock_services
        
        event = {"top_n": 3, "batch_size": 2}
        
        mocks['imdb'].fetch_movie_data.return_value = mock_imdb_data
        mocks['imdb'].get_top_rated_movies.return_value = mock_imdb_data["items"][:3]
        mocks['sqs'].send_batch.side_effect = [False, True]
        
        result = lambda_handler(event, mock_context)
        
        assert result["statusCode"] == 500
        assert "Batch sending failed" in result["body"]
        mocks['sqs'].send_batch.assert_called_once()
