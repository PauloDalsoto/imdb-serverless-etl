import pytest
import os
import sys
import boto3
from pathlib import Path
from moto import mock_aws
from unittest.mock import patch

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

@pytest.fixture
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture
def sqs_queue(aws_credentials):
    with mock_aws():
        sqs = boto3.client('sqs', region_name='us-east-1')
        queue_url = sqs.create_queue(
            QueueName='test-movies-queue.fifo',
            Attributes={
                'FifoQueue': 'true',
                'ContentBasedDeduplication': 'false'
            }
        )['QueueUrl']
        yield queue_url


@pytest.fixture
def environment_variables(sqs_queue):
    env_vars = {
        'SQS_QUEUE_URL': sqs_queue,
        'IMDB_DATA_URL': 'https://imdb-api.com/en/API/Top250Movies/test-key',
        'MAX_RETRIES': '3',
        'BASE_DELAY_SECONDS': '1'
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars


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
