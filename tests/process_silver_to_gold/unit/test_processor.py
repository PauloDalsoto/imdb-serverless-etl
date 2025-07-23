import pytest
from unittest.mock import MagicMock
from lambdas.process_silver_to_gold.src.processor import SilverToGoldProcessor
import pandas as pd

@pytest.fixture
def mock_s3_service():
    mock_s3 = MagicMock()
    mock_s3.load_csv.return_value = MagicMock()
    return mock_s3

@pytest.fixture
def processor(mock_s3_service):
    return SilverToGoldProcessor(mock_s3_service, "source-bucket", "target-bucket")

def test_process_success(processor, mock_s3_service):
    # Mock the return value of load_csv to simulate a valid DataFrame
    mock_df = pd.DataFrame({
        'rank': [1, 2, 3],
        'title': ['Movie1', 'Movie2', 'Movie3'],
        'imdbrating': [9.0, 8.5, 8.0],
        'year': [2020, 2021, 2022],
        'imdbratingcount': [1000, 2000, 3000],
        'released': ['2020-01-01', '2021-01-01', '2022-01-01'],
        'runtime': ['120 min', '130 min', '140 min'],
        'genre': ['Action, Drama', 'Comedy', 'Horror'],
        'director': ['Director1', 'Director2', 'Director3'],
        'language': ['English', 'Spanish', 'French'],
        'country': ['USA', 'Spain', 'France'],
        'awards': ['None', 'Oscar', 'Golden Globe'],
        'metascore': [80, 85, 90],
        'imdbvotes': [10000, 20000, 30000],
        'boxoffice': ['$1,000,000', '$2,000,000', '$3,000,000']
    })
    mock_s3_service.load_csv.return_value = mock_df

    # Call the process method
    record_count = processor.process("silver/movies_normalized.csv")

    # Assertions
    assert record_count == 3
    mock_s3_service.save_csv.assert_called()

def test_process_empty_csv(processor, mock_s3_service):
    # Mock the return value of load_csv to simulate an empty DataFrame
    mock_df = MagicMock()
    mock_df.empty = True
    mock_s3_service.load_csv.return_value = mock_df

    # Call the process method and expect an exception
    with pytest.raises(Exception, match="No data to process, empty csv file!"):
        processor.process("silver/movies_normalized.csv")
