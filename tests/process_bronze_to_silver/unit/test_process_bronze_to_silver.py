import pytest
from unittest.mock import MagicMock, patch
from lambdas.process_bronze_to_silver.process_bronze_to_silver import lambda_handler
from lambdas.process_bronze_to_silver.src.processor import BronzeToSilverProcessor
from lambdas.process_bronze_to_silver.src.s3_service import S3Service

@pytest.fixture
def mock_event():
    return {
        "date": "2025-07-23"
    }

@pytest.fixture
def mock_context():
    return MagicMock()

@pytest.fixture
def mock_processor(mock_s3_service):
    return BronzeToSilverProcessor(mock_s3_service, "source-bucket", "target-bucket")

def test_lambda_handler_success(mock_event, mock_context, mock_s3_service):
    with patch("lambdas.process_bronze_to_silver.process_bronze_to_silver.S3Service", return_value=mock_s3_service):
        with patch("lambdas.process_bronze_to_silver.process_bronze_to_silver.BronzeToSilverProcessor") as mock_processor_class:
            mock_processor_instance = mock_processor_class.return_value
            mock_processor_instance.process.return_value = 10

            result = lambda_handler(mock_event, mock_context)

            assert result["statusCode"] == 200
            assert "Processed 10 records" in result["body"]
            mock_processor_instance.process.assert_called_once()

def test_lambda_handler_no_files(mock_event, mock_context, mock_s3_service):
    mock_s3_service.list_json_objects.return_value = []

    with patch("lambdas.process_bronze_to_silver.process_bronze_to_silver.S3Service", return_value=mock_s3_service):
        result = lambda_handler(mock_event, mock_context)

        assert result["statusCode"] == 500
        assert "No .json files found" in result["body"]

def test_processor_normalize_records(mock_processor):
    json_objects = [{"Key": "Value"}, {"AnotherKey": "AnotherValue"}]
    mock_processor.s3.load_json.side_effect = json_objects

    df = mock_processor.normalize_records(json_objects)

    assert len(df) == 2
    assert "key" in df.columns
    assert "anotherkey" in df.columns

def test_s3_service_list_json_objects(mock_s3_service):
    bucket = "source-bucket"
    prefix = "bronze/2025-07-23/"

    result = mock_s3_service.list_json_objects(bucket, prefix)

    assert len(result) == 1
    assert result[0] == "bronze/2025-07-23/file1.json"
