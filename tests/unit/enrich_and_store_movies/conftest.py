import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the enrich_and_store_movies directory to sys.path to resolve imports
enrich_and_store_movies_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'lambdas', 'enrich_and_store_movies')
enrich_and_store_movies_path = os.path.abspath(enrich_and_store_movies_path)
if enrich_and_store_movies_path not in sys.path:
    sys.path.insert(0, enrich_and_store_movies_path)

os.environ['OMDB_API_SECRET_NAME'] = 'test_omdb_secret'
os.environ['TARGET_S3_BUCKET'] = 'test_url'
os.environ['OMDB_URL'] = 'test_url'
os.environ['MAX_RETRIES'] = '2'
os.environ['BASE_DELAY_SECONDS'] = '1'

@pytest.fixture(autouse=True)
def cleanup_sys_path():
    yield
    if enrich_and_store_movies_path in sys.path:
        sys.path.remove(enrich_and_store_movies_path)

@pytest.fixture
def mock_services():
    mock_omdb = MagicMock()
    mock_s3 = MagicMock()
    
    with patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.boto3.client') as mock_boto, \
         patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.OMDBService') as mock_omdbService_class, \
         patch('lambdas.enrich_and_store_movies.enrich_and_store_movie.S3Service') as mock_s3_class:
        
        mock_omdbService_class.return_value = mock_omdb
        mock_s3_class.return_value = mock_s3
        
        yield {
            'omdb': mock_omdb,
            's3': mock_s3,
            'boto': mock_boto,
            'omdb_class': mock_omdbService_class,
            's3_class': mock_s3_class
        }
