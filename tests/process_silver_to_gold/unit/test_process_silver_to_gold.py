import pytest
from unittest.mock import MagicMock, patch
from lambdas.process_silver_to_gold.process_silver_to_gold import lambda_handler

@pytest.fixture
def mock_event():
    return {}

@pytest.fixture
def mock_context():
    return MagicMock()

def test_lambda_handler_success(mock_event, mock_context):
    with patch("lambdas.process_silver_to_gold.process_silver_to_gold.SilverToGoldProcessor") as mock_processor_class:
        mock_processor = mock_processor_class.return_value

        mock_processor.process.return_value = 100

        result = lambda_handler(mock_event, mock_context)

        assert result["statusCode"] == 200
        assert "Processed 100 films" in result["body"]
        mock_processor.process.assert_called_once()

def test_lambda_handler_empty_csv(mock_event, mock_context):
    with patch("lambdas.process_silver_to_gold.process_silver_to_gold.SilverToGoldProcessor") as mock_processor_class:
        mock_processor = mock_processor_class.return_value

        mock_processor.process.side_effect = Exception("No data to process, empty csv file!")

        result = lambda_handler(mock_event, mock_context)

        assert result["statusCode"] == 500
        assert "No data to process" in result["body"]

def test_lambda_handler_unexpected_error(mock_event, mock_context):
    with patch("lambdas.process_silver_to_gold.process_silver_to_gold.SilverToGoldProcessor") as mock_processor_class:
        mock_processor = mock_processor_class.return_value

        mock_processor.process.side_effect = Exception("Unexpected error")

        result = lambda_handler(mock_event, mock_context)

        assert result["statusCode"] == 500
        assert "Unexpected error" in result["body"]
