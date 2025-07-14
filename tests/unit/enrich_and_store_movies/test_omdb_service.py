import pytest
import json
from unittest.mock import MagicMock, patch
import requests
from lambdas.enrich_and_store_movies.src.omdb_service import OMDBService

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def omdb_service(mock_logger):
    return OMDBService(
        base_url="http://www.omdbapi.com",
        max_retries=3,
        base_delay=1,
        logger=mock_logger
    )

@pytest.fixture
def mock_movie_data():
    return {
        "Title": "The Shawshank Redemption",
        "Year": "1994",
        "Rated": "R",
        "Released": "14 Oct 1994",
        "Runtime": "142 min",
        "Genre": "Drama",
        "Director": "Frank Darabont",
        "Writer": "Stephen King, Frank Darabont",
        "Actors": "Tim Robbins, Morgan Freeman, Bob Gunton",
        "Plot": "Two imprisoned men bond over a number of years...",
        "Language": "English",
        "Country": "United States",
        "Awards": "Nominated for 7 Oscars. 21 wins & 42 nominations total",
        "Poster": "https://example.com/poster.jpg",
        "Ratings": [
            {"Source": "Internet Movie Database", "Value": "9.3/10"},
            {"Source": "Rotten Tomatoes", "Value": "91%"},
            {"Source": "Metacritic", "Value": "82/100"}
        ],
        "Metascore": "82",
        "imdbRating": "9.3",
        "imdbVotes": "2,830,928",
        "imdbID": "tt0111161",
        "Type": "movie",
        "DVD": "27 Jan 1998",
        "BoxOffice": "$16,000,000",
        "Production": "N/A",
        "Website": "N/A",
        "Response": "True"
    }

class TestOMDBService:
    
    def test_init(self, mock_logger):
        service = OMDBService(
            base_url="http://test.com",
            max_retries=5,
            base_delay=2,
            logger=mock_logger
        )
        
        assert service.base_url == "http://test.com"
        assert service.max_retries == 5
        assert service.base_delay == 2
        assert service.logger == mock_logger
    
    @patch('lambdas.enrich_and_store_movies.src.omdb_service.requests.get')
    def test_get_success(self, mock_get, omdb_service):
        mock_response = MagicMock()
        mock_response.json.return_value = {"test": "data"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = omdb_service._get("http://test.com")
        
        mock_get.assert_called_once_with("http://test.com", timeout=10)
        mock_response.raise_for_status.assert_called_once()
        assert result == {"test": "data"}

    @patch('lambdas.enrich_and_store_movies.src.omdb_service.requests.get')
    def test_get_http_error(self, mock_get, omdb_service):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            omdb_service._get("http://test.com")

    @patch('lambdas.enrich_and_store_movies.src.omdb_service.with_retries')
    def test_fetch_movie_data_success(self, mock_with_retries, omdb_service, mock_movie_data):
        mock_with_retries.return_value = mock_movie_data
        
        result = omdb_service.fetch_movie_data("tt0111161", "test_api_key")
        
        assert result == mock_movie_data
        mock_with_retries.assert_called_once_with(
            omdb_service.logger,
            omdb_service.max_retries,
            omdb_service.base_delay,
            omdb_service._get,
            "Calling OMDb API for tt0111161",
            "http://www.omdbapi.com/?apikey=test_api_key&i=tt0111161"
        )

    @patch('lambdas.enrich_and_store_movies.src.omdb_service.with_retries')
    def test_fetch_movie_data_api_error_response(self, mock_with_retries, omdb_service):
        error_response = {
            "Response": "False",
            "Error": "Movie not found!"
        }
        mock_with_retries.return_value = error_response
        
        result = omdb_service.fetch_movie_data("tt0000000", "test_api_key")
        
        assert result is None
        omdb_service.logger.warning.assert_called_once_with(
            "OMDb API returned error for tt0000000: Movie not found!"
        )

    @patch('lambdas.enrich_and_store_movies.src.omdb_service.with_retries')
    def test_fetch_movie_data_exception(self, mock_with_retries, omdb_service):
        mock_with_retries.side_effect = Exception("Network error")
        
        result = omdb_service.fetch_movie_data("tt0111161", "test_api_key")
        
        assert result is None
        omdb_service.logger.error.assert_called_once_with(
            "OMDb API failed for tt0111161: Network error"
        )
    
    def test_fetch_movie_data_no_base_url(self, mock_logger):
        service = OMDBService(
            base_url=None,
            max_retries=3,
            base_delay=1,
            logger=mock_logger
        )
        
        with pytest.raises(ValueError, match="OMDB_URL not set."):
            service.fetch_movie_data("tt0111161", "test_api_key")
    
    def test_fetch_movie_data_empty_base_url(self, mock_logger):
        service = OMDBService(
            base_url="",
            max_retries=3,
            base_delay=1,
            logger=mock_logger
        )
        
        with pytest.raises(ValueError, match="OMDB_URL not set."):
            service.fetch_movie_data("tt0111161", "test_api_key")

    @patch('lambdas.enrich_and_store_movies.src.omdb_service.with_retries')
    def test_fetch_movie_data_url_construction(self, mock_with_retries, omdb_service):
        mock_with_retries.return_value = {"Response": "True"}
        
        omdb_service.fetch_movie_data("tt0111161", "my_api_key")
        
        expected_url = "http://www.omdbapi.com/?apikey=my_api_key&i=tt0111161"
        mock_with_retries.assert_called_once_with(
            omdb_service.logger,
            omdb_service.max_retries,
            omdb_service.base_delay,
            omdb_service._get,
            "Calling OMDb API for tt0111161",
            expected_url
        )
    
    @patch('lambdas.enrich_and_store_movies.src.omdb_service.with_retries')
    def test_fetch_movie_data_different_imdb_ids(self, mock_with_retries, omdb_service):
        mock_with_retries.return_value = {"Response": "True"}
        
        test_ids = ["tt0111161", "tt0068646", "tt0071562"]
        
        for imdb_id in test_ids:
            omdb_service.fetch_movie_data(imdb_id, "test_key")
            
            expected_url = f"http://www.omdbapi.com/?apikey=test_key&i={imdb_id}"
            mock_with_retries.assert_called_with(
                omdb_service.logger,
                omdb_service.max_retries,
                omdb_service.base_delay,
                omdb_service._get,
                f"Calling OMDb API for {imdb_id}",
                expected_url
            )
    
    @patch('lambdas.enrich_and_store_movies.src.omdb_service.with_retries')
    def test_fetch_movie_data_response_false_without_error(self, mock_with_retries, omdb_service):
        error_response = {"Response": "False"}
        mock_with_retries.return_value = error_response
        
        result = omdb_service.fetch_movie_data("tt0000000", "test_api_key")
        
        assert result is None
        omdb_service.logger.warning.assert_called_once_with(
            "OMDb API returned error for tt0000000: None"
        )
    
    @patch('lambdas.enrich_and_store_movies.src.omdb_service.with_retries')
    def test_fetch_movie_data_response_true_with_data(self, mock_with_retries, omdb_service, mock_movie_data):
        mock_with_retries.return_value = mock_movie_data
        
        result = omdb_service.fetch_movie_data("tt0111161", "test_api_key")
        
        assert result == mock_movie_data
        assert result["Title"] == "The Shawshank Redemption"
        assert result["imdbID"] == "tt0111161"
        assert result["Response"] == "True"
        
        omdb_service.logger.warning.assert_not_called()
        omdb_service.logger.error.assert_not_called()
