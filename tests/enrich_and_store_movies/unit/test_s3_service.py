import pytest
import json
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
from lambdas.enrich_and_store_movies.src.s3_service import S3Service

@pytest.fixture
def s3_service(mock_s3_client, mock_logger):
    return S3Service(
        client=mock_s3_client,
        max_retries=3,
        base_delay=1,
        logger=mock_logger
    )

@pytest.fixture
def sample_data():
    return {
        "title": "The Shawshank Redemption",
        "year": "1994",
        "imdbID": "tt0111161"
    }

class TestS3Service:
    
    def test_init(self, mock_s3_client, mock_logger):
        service = S3Service(
            client=mock_s3_client,
            max_retries=5,
            base_delay=2,
            logger=mock_logger
        )
        
        assert service.client == mock_s3_client
        assert service.max_retries == 5
        assert service.base_delay == 2
        assert service.logger == mock_logger

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_json_success(self, mock_with_retries, s3_service, sample_data):
        mock_with_retries.return_value = None
        
        result = s3_service.upload_json("test-bucket", "test-key", sample_data)
        
        assert result is True
        expected_body = json.dumps(sample_data, indent=2)
        mock_with_retries.assert_called_once_with(
            s3_service.logger,
            s3_service.max_retries,
            s3_service.base_delay,
            s3_service.client.put_object,
            "Uploading to s3://test-bucket/test-key",
            Bucket="test-bucket",
            Key="test-key",
            Body=expected_body,
            ContentType="application/json"
        )
        s3_service.logger.info.assert_called_once_with("Uploaded to s3://test-bucket/test-key")

    def test_upload_json_invalid_data(self, s3_service):
        invalid_data = {"invalid": set([1, 2, 3])}
        
        result = s3_service.upload_json("test-bucket", "test-key", invalid_data)
        
        assert result is False
        s3_service.logger.error.assert_called_once()
        error_call = s3_service.logger.error.call_args[0][0]
        assert "Failed to convert data to JSON" in error_call

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_string_success(self, mock_with_retries, s3_service):
        mock_with_retries.return_value = None
        test_body = "test string content"
        
        result = s3_service.upload_string("test-bucket", "test-key", test_body)
        
        assert result is True
        mock_with_retries.assert_called_once_with(
            s3_service.logger,
            s3_service.max_retries,
            s3_service.base_delay,
            s3_service.client.put_object,
            "Uploading to s3://test-bucket/test-key",
            Bucket="test-bucket",
            Key="test-key",
            Body=test_body,
            ContentType="application/json"
        )
        s3_service.logger.info.assert_called_once_with("Uploaded to s3://test-bucket/test-key")

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_string_client_error(self, mock_with_retries, s3_service):
        mock_with_retries.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchBucket', 'Message': 'Bucket does not exist'}},
            operation_name='PutObject'
        )
        
        result = s3_service.upload_string("test-bucket", "test-key", "test content")
        
        assert result is False
        s3_service.logger.error.assert_called_once()
        error_call = s3_service.logger.error.call_args[0][0]
        assert "Upload to S3 failed" in error_call

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_json_with_nested_data(self, mock_with_retries, s3_service):
        mock_with_retries.return_value = None
        nested_data = {
            "movie": {
                "title": "The Shawshank Redemption",
                "details": {
                    "year": 1994,
                    "ratings": [
                        {"source": "IMDb", "value": "9.3/10"},
                        {"source": "Rotten Tomatoes", "value": "91%"}
                    ]
                }
            }
        }
        
        result = s3_service.upload_json("test-bucket", "nested-key", nested_data)
        
        assert result is True
        expected_body = json.dumps(nested_data, indent=2)
        mock_with_retries.assert_called_once_with(
            s3_service.logger,
            s3_service.max_retries,
            s3_service.base_delay,
            s3_service.client.put_object,
            "Uploading to s3://test-bucket/nested-key",
            Bucket="test-bucket",
            Key="nested-key",
            Body=expected_body,
            ContentType="application/json"
        )

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_string_empty_content(self, mock_with_retries, s3_service):
        mock_with_retries.return_value = None
        
        result = s3_service.upload_string("test-bucket", "empty-key", "")
        
        assert result is True
        mock_with_retries.assert_called_once_with(
            s3_service.logger,
            s3_service.max_retries,
            s3_service.base_delay,
            s3_service.client.put_object,
            "Uploading to s3://test-bucket/empty-key",
            Bucket="test-bucket",
            Key="empty-key",
            Body="",
            ContentType="application/json"
        )

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_json_empty_dict(self, mock_with_retries, s3_service):
        mock_with_retries.return_value = None
        empty_data = {}
        
        result = s3_service.upload_json("test-bucket", "empty-dict-key", empty_data)
        
        assert result is True
        expected_body = json.dumps(empty_data, indent=2)
        mock_with_retries.assert_called_once_with(
            s3_service.logger,
            s3_service.max_retries,
            s3_service.base_delay,
            s3_service.client.put_object,
            "Uploading to s3://test-bucket/empty-dict-key",
            Bucket="test-bucket",
            Key="empty-dict-key",
            Body=expected_body,
            ContentType="application/json"
        )

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_string_different_buckets_and_keys(self, mock_with_retries, s3_service):
        mock_with_retries.return_value = None
        
        test_cases = [
            ("bucket1", "key1", "content1"),
            ("bucket2", "folder/key2", "content2"),
            ("bucket3", "deep/nested/folder/key3", "content3")
        ]
        
        for bucket, key, content in test_cases:
            result = s3_service.upload_string(bucket, key, content)
            assert result is True
            
        assert mock_with_retries.call_count == 3

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_json_none_data(self, mock_with_retries, s3_service):
        mock_with_retries.return_value = None
        
        result = s3_service.upload_json("test-bucket", "none-key", None)
        
        assert result is True
        expected_body = json.dumps(None, indent=2)
        mock_with_retries.assert_called_once_with(
            s3_service.logger,
            s3_service.max_retries,
            s3_service.base_delay,
            s3_service.client.put_object,
            "Uploading to s3://test-bucket/none-key",
            Bucket="test-bucket",
            Key="none-key",
            Body=expected_body,
            ContentType="application/json"
        )

    @patch('lambdas.enrich_and_store_movies.src.s3_service.with_retries')
    def test_upload_json_array_data(self, mock_with_retries, s3_service):
        mock_with_retries.return_value = None
        array_data = [
            {"title": "Movie 1", "year": 2020},
            {"title": "Movie 2", "year": 2021}
        ]
        
        result = s3_service.upload_json("test-bucket", "array-key", array_data)
        
        assert result is True
        expected_body = json.dumps(array_data, indent=2)
        mock_with_retries.assert_called_once_with(
            s3_service.logger,
            s3_service.max_retries,
            s3_service.base_delay,
            s3_service.client.put_object,
            "Uploading to s3://test-bucket/array-key",
            Bucket="test-bucket",
            Key="array-key",
            Body=expected_body,
            ContentType="application/json"
        )
