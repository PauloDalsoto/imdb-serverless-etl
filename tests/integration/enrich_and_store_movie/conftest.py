import pytest
import os
import sys
from pathlib import Path

# Add the enrich_and_store_movies directory to sys.path to resolve imports
enrich_and_store_movies_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'lambdas', 'enrich_and_store_movies')
enrich_and_store_movies_path = os.path.abspath(enrich_and_store_movies_path)
if enrich_and_store_movies_path not in sys.path:
    sys.path.insert(0, enrich_and_store_movies_path)

@pytest.fixture(autouse=True)
def cleanup_sys_path():
    yield
    if enrich_and_store_movies_path in sys.path:
        sys.path.remove(enrich_and_store_movies_path)

# Set environment variables for integration tests
os.environ['OMDB_API_SECRET_NAME'] = 'test_omdb_secret'
os.environ['TARGET_S3_BUCKET'] = 'test-integration-bucket'
os.environ['OMDB_URL'] = 'http://www.omdbapi.com/'
os.environ['MAX_RETRIES'] = '2'
os.environ['BASE_DELAY_SECONDS'] = '1'