import pytest
import os
import sys
from unittest.mock import patch, MagicMock

fetch_top_movies_path = os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'fetch_top_movies')
fetch_top_movies_path = os.path.abspath(fetch_top_movies_path)
if fetch_top_movies_path not in sys.path:
    sys.path.insert(0, fetch_top_movies_path)

@pytest.fixture(autouse=True)
def cleanup_sys_path():
    yield
    if fetch_top_movies_path in sys.path:
        sys.path.remove(fetch_top_movies_path)  

@pytest.fixture
def mock_services():
    mock_imdb = MagicMock()
    mock_sqs = MagicMock()
    
    with patch('lambdas.fetch_top_movies.fetch_top_movies.boto3.client') as mock_boto, \
         patch('lambdas.fetch_top_movies.fetch_top_movies.SQSService') as mock_sqs_class, \
         patch('lambdas.fetch_top_movies.fetch_top_movies.IMDBService') as mock_imdb_class:

        mock_sqs_class.return_value = mock_sqs
        mock_imdb_class.return_value = mock_imdb
        
        yield {
            'imdb': mock_imdb,
            'sqs': mock_sqs,
            'boto': mock_boto,
            'imdb_class': mock_imdb_class,
            'sqs_class': mock_sqs_class
        }
        
@pytest.fixture
def mock_event():
    return {
        "top_n": 10,
        "batch_size": 5
    }

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
                "image": "https://m.media-amazon.com/images/M/MV5BMDFkYTc0MGEtZmNhMC00ZDIzLWFmNTEtODM1ZmRlYWMwMWFmXkEyXkFqcGdeQXVyMTMxODk2OTU@._V1_UX128_CR0,3,128,176_AL_.jpg",
                "crew": "Frank Darabont (dir.), Tim Robbins, Morgan Freeman",
                "imDbRating": "9.3",
                "imDbRatingCount": "2620000"
            },
            {
                "id": "tt0068646",
                "rank": "2",
                "title": "The Godfather",
                "fullTitle": "The Godfather (1972)",
                "year": "1972",
                "image": "https://m.media-amazon.com/images/M/MV5BM2MyNjYxNmUtYTAwNi00MTYxLWJmNWYtYzZlODY3ZTk3OTFlXkEyXkFqcGdeQXVyNzkwMjQ5NzM@._V1_UX128_CR0,1,128,176_AL_.jpg",
                "crew": "Francis Ford Coppola (dir.), Marlon Brando, Al Pacino",
                "imDbRating": "9.2",
                "imDbRatingCount": "1710000"
            },
            {
                "id": "tt0071562",
                "rank": "3",
                "title": "The Godfather Part II",
                "fullTitle": "The Godfather Part II (1974)",
                "year": "1974",
                "image": "https://m.media-amazon.com/images/M/MV5BMWMwMGQzZTItY2JlNC00OWZiLWIyMDctNDk2ZDQ2YjRjMWQ0XkEyXkFqcGdeQXVyNzkwMjQ5NzM@._V1_UX128_CR0,1,128,176_AL_.jpg",
                "crew": "Francis Ford Coppola (dir.), Al Pacino, Robert De Niro",
                "imDbRating": "9.0",
                "imDbRatingCount": "1230000"
            },
            {
                "id": "tt0468569",
                "rank": "4",
                "title": "The Dark Knight",
                "fullTitle": "The Dark Knight (2008)",
                "year": "2008",
                "image": "https://m.media-amazon.com/images/M/MV5BMTMxNTMwODM0NF5BMl5BanBnXkFtZTcwODAyMTk2Mw@@._V1_UX128_CR0,3,128,176_AL_.jpg",
                "crew": "Christopher Nolan (dir.), Christian Bale, Heath Ledger",
                "imDbRating": "9.0",
                "imDbRatingCount": "2540000"
            },
            {
                "id": "tt0050083",
                "rank": "5",
                "title": "12 Angry Men",
                "fullTitle": "12 Angry Men (1957)",
                "year": "1957",
                "image": "https://m.media-amazon.com/images/M/MV5BMWU4N2FjNzYtNTVkNC00NzQ0LTg0MjAtYTJlMjFhNGUxZDFmXkEyXkFqcGdeQXVyNjc1NTYyMjg@._V1_UX128_CR0,1,128,176_AL_.jpg",
                "crew": "Sidney Lumet (dir.), Henry Fonda, Martin Balsam",
                "imDbRating": "9.0",
                "imDbRatingCount": "760000"
            }
        ]
    }
