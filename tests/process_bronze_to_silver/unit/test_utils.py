import pytest
from unittest.mock import MagicMock
from lambdas.process_bronze_to_silver.src.utils import with_retries, build_response

def test_with_retries_success():
    mock_logger = MagicMock()
    mock_func = MagicMock(return_value="success")

    result = with_retries(mock_logger, max_retries=3, base_delay=1, func=mock_func, description="Test", args=(), kwargs={})

    assert result == "success"
    mock_func.assert_called_once()

def test_with_retries_failure():
    mock_logger = MagicMock()
    mock_func = MagicMock(side_effect=Exception("failure"))

    with pytest.raises(Exception, match="Test failed after 3 retries"):
        with_retries(mock_logger, max_retries=3, base_delay=1, func=mock_func, description="Test", args=(), kwargs={})

def test_build_response():
    result = build_response(200, "Success")

    assert result["statusCode"] == 200
    assert result["body"] == '{"message": "Success"}'
