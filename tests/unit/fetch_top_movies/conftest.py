import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Set environment variables
os.environ['SQS_QUEUE_URL'] = 'test_queue'
os.environ['IMDB_DATA_URL'] = 'test_url'
os.environ['MAX_RETRIES'] = '2'
os.environ['BASE_DELAY_SECONDS'] = '1'


@pytest.fixture
def mock_services():
    """Fixture that provides mocked services for testing."""
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
