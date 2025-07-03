# imdb-serverless-etl
A serverless ETL pipeline built on AWS using Python, Lambda, S3, and SQS. This project fetches the IMDb Top 250 movie dataset, filters the top 10 movies, enriches each entry with additional metadata from the OMDb API, and stores the final enriched data in S3. Designed with scalability and automation in mind, following modern cloud-native practices.

sam build -t buckets-template.yaml --config-file samconfig-buckets.toml
sam deploy -t buckets-template.yaml --config-file samconfig-buckets.toml