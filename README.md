# IMDb Serverless ETL

> A modern, serverless architecture to process and enrich IMDb data on AWS.  
> Scalable data pipeline using AWS Lambda, SQS, S3, Secrets Manager and more.

## Architecture Overview

*[Architecture diagram will be placed here]*

## 🚀 About This Project

This project implements a **serverless ETL pipeline** that demonstrates the **Medallion Architecture** pattern for data processing on AWS. The pipeline fetches IMDb Top 250 movie data, enriches it with additional metadata from the OMDb API, and processes it through multiple layers (Bronze → Silver → Gold) to create a clean, analytics-ready dataset.

### Key Features

- **Serverless Architecture**: Built entirely on AWS managed services
- **Event-Driven Processing**: Uses SQS for decoupling and S3 events for triggering
- **Medallion Architecture**: Implements Bronze, Silver, and Gold data layers
- **Automatic Scaling**: Lambda functions scale based on demand
- **Secure**: Uses AWS Secrets Manager for API keys and IAM roles for permissions
- **Scheduled Execution**: Daily automated data processing via CloudWatch Events

## 📚 Documentation

The **main documentation** for this project is available at:
**[https://github.com/PauloDalsoto/imdb-serverless-etl-docs](https://github.com/PauloDalsoto/imdb-serverless-etl-docs)**

You can also view the **live demo** of the documentation at:
**[https://paulodalsoto.github.io/imdb-serverless-etl-docs/](https://paulodalsoto.github.io/imdb-serverless-etl-docs/)**

## 🏗️ Architecture Components

### Lambda Functions
1. **GetMoviesAndSendToQueue** - Fetches top movies from IMDb and sends to SQS
2. **EnrichAndStoreMovie** - Enriches movie data with OMDb API and stores in Bronze layer
3. **ProcessBronzeToSilver** - Cleanses and transforms data from Bronze to Silver
4. **ProcessSilverToGold** - Aggregates and optimizes data from Silver to Gold

### AWS Services Used
- **AWS Lambda** - Serverless compute
- **Amazon S3** - Data storage (Bronze, Silver, Gold buckets)
- **Amazon SQS** - Message queuing (FIFO)
- **AWS Secrets Manager** - Secure API key storage
- **Amazon CloudWatch** - Monitoring and scheduling
- **AWS IAM** - Security and permissions

## 🚦 Quick Start

### Prerequisites
- AWS CLI configured
- SAM CLI installed
- Python 3.13+

### Deployment

```bash
# Build the application
sam build

# Deploy the stack
sam deploy --guided
```

### Manual Bucket Setup (if needed)
```bash
sam build -t buckets-template.yaml --config-file samconfig-buckets.toml
sam deploy -t buckets-template.yaml --config-file samconfig-buckets.toml
```

## 🔧 Configuration

The pipeline requires the following parameters:
- `BronzeBucketName` - Name for the Bronze S3 bucket
- `SilverBucketName` - Name for the Silver S3 bucket  
- `GoldBucketName` - Name for the Gold S3 bucket
- `imdbDataUrl` - IMDb data source URL
- `omdbApiUrl` - OMDb API base URL
- `maxRetries` - Maximum retry attempts
- `baseDelaySeconds` - Base delay for exponential backoff

## 📊 Data Flow

1. **Daily Schedule** → Lambda fetches top movies from IMDb
2. **SQS Queue** → Decouples movie processing 
3. **Bronze Layer** → Raw enriched data from OMDb API
4. **Silver Layer** → Cleansed and validated data
5. **Gold Layer** → Analytics-ready aggregated data

## 🛡️ Security

- All buckets have public access blocked
- IAM roles follow least privilege principle
- API keys stored securely in Secrets Manager
- VPC isolation available for enhanced security

## 🤝 Contributing

Please refer to the [main documentation](https://github.com/PauloDalsoto/imdb-serverless-etl-docs) for contribution guidelines.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

*For detailed documentation, deployment guides, and examples, visit the [complete documentation site](https://paulodalsoto.github.io/imdb-serverless-etl-docs/).*