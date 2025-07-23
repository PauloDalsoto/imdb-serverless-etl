import pytest
import json
from unittest.mock import MagicMock
from lambdas.fetch_top_movies.src.utils import build_response, with_retries

def test_build_response_success():
    result = build_response(200, "Success")
    
    assert result["statusCode"] == 200
    assert result["body"] == json.dumps({"message": "Success"})


def test_build_response_error():
    result = build_response(500, "Error occurred")
    
    assert result["statusCode"] == 500
    assert result["body"] == json.dumps({"message": "Error occurred"})


def test_with_retries_success():
    mock_logger = MagicMock()
    call_count = 0
    
    def mock_operation():
        nonlocal call_count
        call_count += 1
        return "success"
    
    result = with_retries(mock_logger, 3, 0.1, mock_operation, "Test operation")
    
    assert result == "success"
    assert call_count == 1


def test_with_retries_fails_then_succeeds():
    mock_logger = MagicMock()
    call_count = 0
    
    def mock_operation():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise Exception("Temporary failure")
        return "success"
    
    result = with_retries(mock_logger, 3, 0.1, mock_operation, "Test operation")
    
    assert result == "success"
    assert call_count == 2


def test_with_retries_always_fails():
    mock_logger = MagicMock()
    call_count = 0
    
    def mock_operation():
        nonlocal call_count
        call_count += 1
        raise Exception("Permanent failure")
    
    with pytest.raises(Exception):
        with_retries(mock_logger, 3, 0.1, mock_operation, "Test operation")
    
    assert call_count == 3
