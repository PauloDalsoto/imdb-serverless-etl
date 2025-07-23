import pytest
from unittest.mock import MagicMock
from lambdas.process_bronze_to_silver.src.processor import BronzeToSilverProcessor

@pytest.fixture
def processor(mock_s3_service):
    return BronzeToSilverProcessor(mock_s3_service, "source-bucket", "target-bucket")

def test_process_success(processor, mock_s3_service):
    mock_s3_service.list_json_objects.return_value = ["bronze/2025-07-23/file1.json"]
    mock_s3_service.load_json.return_value = {"key": "value"}

    record_count = processor.process("bronze/2025-07-23/")

    assert record_count == 1
    mock_s3_service.save_csv.assert_called_once()

def test_process_no_files(processor, mock_s3_service):
    mock_s3_service.list_json_objects.return_value = []

    with pytest.raises(Exception, match="No .json files found"):
        processor.process("bronze/2025-07-23/")

def test_normalize_records(processor):
    json_objects = [{"Key": "Value"}, {"AnotherKey": "AnotherValue"}]
    df = processor.normalize_records(json_objects)

    assert len(df) == 2
    assert "key" in df.columns
    assert "anotherkey" in df.columns
