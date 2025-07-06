# IMDb Serverless ETL
A modern, serverless architecture to process and enrich IMDb data on AWS.  
Scalable data pipeline using AWS Lambda, SQS, S3, Secrets Manager and more.

[![Documantation](https://img.shields.io/badge/Documentation-GitHub%20Pages-blue)](https://paulodalsoto.github.io/imdb-serverless-etl-docs/)

## About This Project

This project implements a **serverless ETL pipeline** that demonstrates the **Medallion Architecture** pattern for data processing on AWS. The pipeline fetches IMDb Top 250 movie data, enriches it with additional metadata from the OMDb API, and processes it through multiple layers (Bronze → Silver → Gold) to create a clean, analytics-ready dataset.

### Key Features

- **Serverless Architecture**: Built entirely on AWS managed services  
- **Event-Driven Processing**: Uses SQS for decoupling and S3 events for triggering  
- **Medallion Architecture**: Implements Bronze, Silver, and Gold data layers  
- **Automatic Scaling**: Lambda functions scale based on demand  
- **Secure**: Uses AWS Secrets Manager for API keys and IAM roles for permissions  
- **Scheduled Execution**: Daily automated data processing via EventBridge  
- **Visualization Ready**: Output data designed for direct consumption by **Amazon QuickSight** dashboards
  
## Architecture Components

![Architecture Diagram](/images/final_arch.png)

### Lambda Functions

1. **GetMoviesAndSendToQueue** – Fetches top movies from IMDb and sends them to SQS  
2. **EnrichAndStoreMovie** – Enriches movie data with OMDb API and stores it in the Bronze layer  
3. **ProcessBronzeToSilver** – Cleanses and transforms data from Bronze to Silver  
4. **ProcessSilverToGold** – Aggregates and optimizes data from Silver to Gold  

### AWS Services Used

- **AWS Lambda** – Serverless compute  
- **Amazon S3** – Data storage (Bronze, Silver, Gold buckets)  
- **Amazon SQS** – Message queuing (FIFO)  
- **AWS Secrets Manager** – Secure API key storage  
- **Amazon EventBridge** – Scheduling and orchestration  
- **Amazon CloudWatch** – Logging and monitoring  
- **Amazon QuickSight** – Visualization of analytics-ready data  
- **AWS IAM** – Security and permissions  

## Quick Start

For deployment instructions and environment setup, refer to the official documentation: **[Deployment Guide](https://paulodalsoto.github.io/imdb-serverless-etl-docs/guide/deployment)**

## Data Flow
1. **Daily Trigger (EventBridge)** – Triggers the pipeline at scheduled intervals  
2. **SQS Queue** – Buffers and decouples data ingestion and enrichment  
3. **Bronze Layer** – Stores raw enriched data from OMDb  
4. **Silver Layer** – Contains normalized and validated movie data  
5. **Gold Layer** – Contains aggregated, analytics-ready datasets  

## Security
- All S3 buckets have public access blocked  
- IAM roles follow the principle of least privilege  
- API keys are securely stored in AWS Secrets Manager
  
---
*For complete configuration, deployment details, architecture diagrams, and usage examples, visit the official documentation site:*  
**[https://paulodalsoto.github.io/imdb-serverless-etl-docs/](https://paulodalsoto.github.io/imdb-serverless-etl-docs/)**
