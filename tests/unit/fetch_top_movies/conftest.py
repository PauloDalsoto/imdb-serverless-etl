import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Add the fetch_top_movies directory to sys.path to resolve imports
fetch_top_movies_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'lambdas', 'fetch_top_movies')
fetch_top_movies_path = os.path.abspath(fetch_top_movies_path)
if fetch_top_movies_path not in sys.path:
    sys.path.insert(0, fetch_top_movies_path)

@pytest.fixture(autouse=True)
def cleanup_sys_path():
    yield
    if fetch_top_movies_path in sys.path:
        sys.path.remove(fetch_top_movies_path)  

os.environ['SQS_QUEUE_URL'] = 'test_queue'
os.environ['IMDB_DATA_URL'] = 'test_url'
os.environ['MAX_RETRIES'] = '3'
os.environ['BASE_DELAY_SECONDS'] = '1'

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