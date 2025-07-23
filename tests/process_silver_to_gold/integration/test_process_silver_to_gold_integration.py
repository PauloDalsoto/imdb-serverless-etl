import pytest
import boto3
import importlib
import json
from moto import mock_aws

@mock_aws
def test_lambda_handler_success(environment_variables, s3_buckets):
    s3_client = s3_buckets['s3_client']
    source_bucket = s3_buckets['source_bucket']
    target_bucket = s3_buckets['target_bucket']

    # Add mock data to the source bucket
    mock_data = "id,title,rank,year,imdbrating,imdbratingcount,released,runtime,genre,director,language,country,awards,metascore,imdbvotes,boxoffice\n"
    mock_data += "tt1234567,Test Movie,1,2025,8.5,100000,2025-07-22,120 min,Action,John Doe,English,USA,None,75,50000,100000000\n"
    s3_client.put_object(
        Bucket=source_bucket,
        Key="silver/movies_normalized.csv",
        Body=mock_data
    )

    import lambdas.process_silver_to_gold.process_silver_to_gold as process_module
    importlib.reload(process_module)

    event = {}
    result = process_module.lambda_handler(event, None)

    assert result["statusCode"] == 200
    assert "Processed 1 films from silver to gold." in result["body"]

    response = s3_client.get_object(Bucket=target_bucket, Key="gold/topN_rated.csv")
    stored_data = response['Body'].read().decode('utf-8')
    assert "Test Movie" in stored_data
