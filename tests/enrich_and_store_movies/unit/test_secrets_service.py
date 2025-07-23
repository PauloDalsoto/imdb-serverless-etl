import pytest
import json
from unittest.mock import MagicMock, patch
from lambdas.enrich_and_store_movies.src.secrets_service import SecretsService

@pytest.fixture
def secrets_service(mock_secrets_client, mock_logger):
    return SecretsService(
        client=mock_secrets_client,
        max_retries=3,
        base_delay=1,
        logger=mock_logger
    )

@pytest.fixture
def mock_secret_response():
    return {
        "SecretString": json.dumps({"omdbapi_key": "test_api_key_123"})
    }

class TestSecretsService:
    
    def test_init(self, mock_secrets_client, mock_logger):
        service = SecretsService(
            client=mock_secrets_client,
            max_retries=5,
            base_delay=2,
            logger=mock_logger
        )
        
        assert service.client == mock_secrets_client
        assert service.max_retries == 5
        assert service.base_delay == 2
        assert service.logger == mock_logger

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_secret_string_success(self, mock_with_retries, secrets_service, mock_secret_response):
        mock_with_retries.return_value = mock_secret_response
        
        result = secrets_service.get_secret_string("test-secret")
        
        assert result == mock_secret_response["SecretString"]
        mock_with_retries.assert_called_once_with(
            secrets_service.logger,
            secrets_service.max_retries,
            secrets_service.base_delay,
            secrets_service.client.get_secret_value,
            "Retrieving secret: test-secret",
            SecretId="test-secret"
        )

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_secret_string_no_secret_string(self, mock_with_retries, secrets_service):
        mock_with_retries.return_value = {"SecretBinary": b"binary_data"}
        
        result = secrets_service.get_secret_string("test-secret")
        
        assert result is None

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_secret_string_empty_response(self, mock_with_retries, secrets_service):
        mock_with_retries.return_value = {}
        
        result = secrets_service.get_secret_string("test-secret")
        
        assert result is None

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_success(self, mock_with_retries, secrets_service):
        secret_string = json.dumps({"omdbapi_key": "test_api_key_123"})
        mock_with_retries.return_value = {"SecretString": secret_string}
        
        result = secrets_service.get_omdb_api_key("test-secret")
        
        assert result == "test_api_key_123"

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_empty_secret(self, mock_with_retries, secrets_service):
        mock_with_retries.return_value = {"SecretString": ""}
        
        with pytest.raises(ValueError, match="OMDb API secret is empty."):
            secrets_service.get_omdb_api_key("test-secret")

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_none_secret(self, mock_with_retries, secrets_service):
        mock_with_retries.return_value = {"SecretString": None}
        
        with pytest.raises(ValueError, match="OMDb API secret is empty."):
            secrets_service.get_omdb_api_key("test-secret")

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_no_secret_string(self, mock_with_retries, secrets_service):
        mock_with_retries.return_value = {}
        
        with pytest.raises(ValueError, match="OMDb API secret is empty."):
            secrets_service.get_omdb_api_key("test-secret")

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_invalid_json(self, mock_with_retries, secrets_service):
        mock_with_retries.return_value = {"SecretString": "invalid json string"}
        
        with pytest.raises(ValueError, match="Secret string is not valid JSON."):
            secrets_service.get_omdb_api_key("test-secret")

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_missing_key(self, mock_with_retries, secrets_service):
        secret_string = json.dumps({"other_key": "value"})
        mock_with_retries.return_value = {"SecretString": secret_string}
        
        result = secrets_service.get_omdb_api_key("test-secret")
        
        assert result is None

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_empty_key_value(self, mock_with_retries, secrets_service):
        secret_string = json.dumps({"omdbapi_key": ""})
        mock_with_retries.return_value = {"SecretString": secret_string}
        
        result = secrets_service.get_omdb_api_key("test-secret")
        
        assert result == ""

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_null_key_value(self, mock_with_retries, secrets_service):
        secret_string = json.dumps({"omdbapi_key": None})
        mock_with_retries.return_value = {"SecretString": secret_string}
        
        result = secrets_service.get_omdb_api_key("test-secret")
        
        assert result is None

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_complex_secret(self, mock_with_retries, secrets_service):
        secret_string = json.dumps({
            "omdbapi_key": "complex_api_key_456",
            "other_api_key": "another_key",
            "database_url": "postgresql://user:pass@localhost/db"
        })
        mock_with_retries.return_value = {"SecretString": secret_string}
        
        result = secrets_service.get_omdb_api_key("test-secret")
        
        assert result == "complex_api_key_456"

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_secret_string_different_secret_names(self, mock_with_retries, secrets_service):
        secret_names = ["secret1", "secret2", "secret3"]
        
        for secret_name in secret_names:
            mock_with_retries.return_value = {"SecretString": f"value_for_{secret_name}"}
            result = secrets_service.get_secret_string(secret_name)
            
            assert result == f"value_for_{secret_name}"
            mock_with_retries.assert_called_with(
                secrets_service.logger,
                secrets_service.max_retries,
                secrets_service.base_delay,
                secrets_service.client.get_secret_value,
                f"Retrieving secret: {secret_name}",
                SecretId=secret_name
            )

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_malformed_json(self, mock_with_retries, secrets_service):
        mock_with_retries.return_value = {"SecretString": '{"omdbapi_key": "key", invalid}'}
        
        with pytest.raises(ValueError, match="Secret string is not valid JSON."):
            secrets_service.get_omdb_api_key("test-secret")

    @patch('lambdas.enrich_and_store_movies.src.secrets_service.with_retries')
    def test_get_omdb_api_key_nested_structure(self, mock_with_retries, secrets_service):
        secret_string = json.dumps({
            "apis": {
                "omdbapi_key": "nested_key_789"
            }
        })
        mock_with_retries.return_value = {"SecretString": secret_string}
        
        result = secrets_service.get_omdb_api_key("test-secret")
        
        assert result is None
