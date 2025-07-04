import pytest
import json
from unittest.mock import MagicMock, patch
import requests
from lambdas.fetch_top_movies.src.imdb_service import IMDBService


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def imdb_service(mock_logger):
    return IMDBService(
        url="https://test-url.com",
        max_retries=3,
        base_delay=0.1,
        logger=mock_logger
    )


def test_init(mock_logger):
    service = IMDBService(
        url="https://test-url.com",
        max_retries=3,
        base_delay=0.1,
        logger=mock_logger
    )
    
    assert service.url == "https://test-url.com"
    assert service.max_retries == 3
    assert service.base_delay == 0.1
    assert service.logger == mock_logger


@patch('lambdas.fetch_top_movies.src.imdb_service.requests.get')
def test_fetch_success(mock_get, imdb_service):
    mock_response = MagicMock()
    mock_response.json.return_value = {"items": [{"rank": "1", "id": "movie1"}]}
    mock_get.return_value = mock_response
    
    result = imdb_service._fetch("https://test-url.com")
    
    assert result == {"items": [{"rank": "1", "id": "movie1"}]}
    mock_get.assert_called_once_with("https://test-url.com", timeout=10)
    mock_response.raise_for_status.assert_called_once()


@patch('lambdas.fetch_top_movies.src.imdb_service.requests.get')
def test_fetch_http_error(mock_get, imdb_service):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_get.return_value = mock_response
    
    with pytest.raises(requests.exceptions.HTTPError):
        imdb_service._fetch("https://test-url.com")


@patch('lambdas.fetch_top_movies.src.imdb_service.with_retries')
def test_fetch_movie_data_success(mock_with_retries, imdb_service):
    mock_with_retries.return_value = {"items": [{"rank": "1", "id": "movie1"}]}
    
    result = imdb_service.fetch_movie_data()
    
    assert result == {"items": [{"rank": "1", "id": "movie1"}]}
    mock_with_retries.assert_called_once_with(
        imdb_service.logger,
        imdb_service.max_retries,
        imdb_service.base_delay,
        imdb_service._fetch,
        f"Fetching data from: {imdb_service.url}",
        imdb_service.url
    )


def test_get_top_rated_movies_success(imdb_service):
    items = [
        {"rank": "3", "id": "movie3", "title": "Movie 3"},
        {"rank": "1", "id": "movie1", "title": "Movie 1"},
        {"rank": "2", "id": "movie2", "title": "Movie 2"}
    ]
    
    result = imdb_service.get_top_rated_movies(items, 2)
    
    assert len(result) == 2
    assert result[0]["rank"] == 1
    assert result[0]["id"] == "movie1"
    assert result[1]["rank"] == 2
    assert result[1]["id"] == "movie2"


def test_get_top_rated_movies_with_invalid_ranks(imdb_service):
    items = [
        {"rank": "3", "id": "movie3", "title": "Movie 3"},
        {"rank": "", "id": "movie4", "title": "Movie 4"},
        {"rank": "N/A", "id": "movie5", "title": "Movie 5"},
        {"rank": "1", "id": "movie1", "title": "Movie 1"},
        {"rank": "invalid", "id": "movie6", "title": "Movie 6"}
    ]
    
    result = imdb_service.get_top_rated_movies(items, 5)
    
    assert len(result) == 2
    assert result[0]["rank"] == 1
    assert result[0]["id"] == "movie1"
    assert result[1]["rank"] == 3
    assert result[1]["id"] == "movie3"


def test_get_top_rated_movies_missing_rank(imdb_service):
    items = [
        {"id": "movie1", "title": "Movie 1"},
        {"rank": "2", "id": "movie2", "title": "Movie 2"},
        {"rank": "1", "id": "movie3", "title": "Movie 3"}
    ]
    
    result = imdb_service.get_top_rated_movies(items, 5)
    
    assert len(result) == 2
    assert result[0]["rank"] == 1
    assert result[0]["id"] == "movie3"
    assert result[1]["rank"] == 2
    assert result[1]["id"] == "movie2"


def test_get_top_rated_movies_limit_results(imdb_service):
    items = [
        {"rank": "5", "id": "movie5", "title": "Movie 5"},
        {"rank": "3", "id": "movie3", "title": "Movie 3"},
        {"rank": "1", "id": "movie1", "title": "Movie 1"},
        {"rank": "4", "id": "movie4", "title": "Movie 4"},
        {"rank": "2", "id": "movie2", "title": "Movie 2"}
    ]
    
    result = imdb_service.get_top_rated_movies(items, 3)
    
    assert len(result) == 3
    assert result[0]["rank"] == 1
    assert result[1]["rank"] == 2
    assert result[2]["rank"] == 3


def test_get_top_rated_movies_empty_list(imdb_service):
    items = []
    
    result = imdb_service.get_top_rated_movies(items, 5)
    
    assert result == []


def test_get_top_rated_movies_exception_handling(imdb_service):
    # Simular um erro inesperado
    items = None
    
    result = imdb_service.get_top_rated_movies(items, 5)
    
    assert result == []
    imdb_service.logger.error.assert_called_once()


def test_get_top_rated_movies_zero_top_n(imdb_service):
    items = [
        {"rank": "1", "id": "movie1", "title": "Movie 1"},
        {"rank": "2", "id": "movie2", "title": "Movie 2"}
    ]
    
    result = imdb_service.get_top_rated_movies(items, 0)
    
    assert result == []


def test_get_top_rated_movies_logs_invalid_rank_warning(imdb_service):
    items = [
        {"rank": "1", "id": "movie1", "title": "Movie 1"},
        {"rank": "invalid", "id": "movie2", "title": "Movie 2"}
    ]
    
    result = imdb_service.get_top_rated_movies(items, 5)
    
    assert len(result) == 1
    assert result[0]["id"] == "movie1"
    imdb_service.logger.warning.assert_called_once_with("Skipping movie with invalid rank: movie2")


def test_get_top_rated_movies_logs_invalid_rank_warning_no_id(imdb_service):
    items = [
        {"rank": "1", "id": "movie1", "title": "Movie 1"},
        {"rank": "invalid", "title": "Movie 2"}
    ]
    
    result = imdb_service.get_top_rated_movies(items, 5)
    
    assert len(result) == 1
    assert result[0]["id"] == "movie1"
    imdb_service.logger.warning.assert_called_once_with("Skipping movie with invalid rank: N/A")
