import pytest
from unittest.mock import MagicMock
from lambdas.process_bronze_to_silver.src.s3_service import S3Service

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def s3_service(mock_logger):
    return S3Service(mock_logger, max_retries=3, base_delay=1)

def test_list_json_objects(s3_service):
    mock_s3 = MagicMock()
    mock_s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "file1.json"}, {"Key": "file2.json"}]}
    ]
    s3_service.s3 = mock_s3

    result = s3_service.list_json_objects("bucket", "prefix")

    assert len(result) == 2
    assert "file1.json" in result
    assert "file2.json" in result

def test_load_json(s3_service):
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: b'{"key": "value"}')}
    s3_service.s3 = mock_s3

    result = s3_service.load_json("bucket", "key")

    assert result == {"key": "value"}

def test_save_csv(s3_service):
    mock_s3 = MagicMock()
    s3_service.s3 = mock_s3

    s3_service.save_csv("bucket", "key", "csv_data")

    mock_s3.put_object.assert_called_once_with(
        Bucket="bucket",
        Key="key",
        Body=b"csv_data",
        ContentType="text/csv"
    )
