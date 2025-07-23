import pytest
from unittest.mock import MagicMock
from lambdas.process_silver_to_gold.src.s3_service import S3Service

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def s3_service(mock_logger):
    return S3Service(mock_logger, max_retries=3, base_delay=1)

def test_load_csv(s3_service):
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: b"col1,col2\nval1,val2")}
    s3_service.s3 = mock_s3

    result = s3_service.load_csv("bucket", "key")

    assert not result.empty

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
