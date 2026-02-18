# Financial Data Warehouse

This project implements a cloud-based financial data warehouse on BigQuery.  
It focuses on automated ingestion of daily market data using Python jobs deployed on Cloud Run.

The goal of this repository is to demonstrate production-oriented data ingestion, cloud scheduling, and raw-layer warehouse design for financial datasets.

---

## Architecture Overview

Yahoo Finance API  
        ↓  
Cloud Run (Python ingestion jobs)  
        ↓  
BigQuery (RAW layer tables)  
        ↓  
Looker Studio (visualization)

---

## Tech Stack

- Python
- Google Cloud Run
- Google Cloud Scheduler
- BigQuery
- Looker Studio
- GitHub (version control)

---

## Environment & Infrastructure Setup

Before running ingestion jobs, the BigQuery raw layer must be provisioned using the CLI.

This repository follows an infrastructure-first approach: datasets and tables are created using version-controlled SQL files instead of manual UI configuration.

Setup includes:

- Creating a Python virtual environment
- Configuring Google Cloud SDK (`gcloud`, `bq`)
- Executing raw layer DDL via CLI
- Ensuring correct BigQuery region configuration (EU)

For detailed step-by-step instructions (PowerShell vs CMD commands, common errors, and region considerations), see:

➡️ `docs/setup.md`

---

## Data Pipeline

### 1. Historical Load (One-time job)
- Fetches historical daily market data
- Loads data into BigQuery raw tables
- Idempotent design to avoid duplication

### 2. Daily Incremental Job
- Scheduled via Cloud Scheduler
- Fetches latest available trading day
- Appends new records to BigQuery
- Includes basic validation checks

---

## Data Model (RAW Layer)

### Table: `market_raw.stock_prices`

| Column         | Type      | Description                     |
|---------------|-----------|---------------------------------|
| symbol        | STRING    | Ticker symbol                   |
| trade_date    | DATE      | Trading date                    |
| open          | FLOAT64   | Opening price                   |
| high          | FLOAT64   | Highest price of the day        |
| low           | FLOAT64   | Lowest price of the day         |
| close         | FLOAT64   | Closing price                   |
| volume        | INT64     | Daily traded volume             |
| load_timestamp| TIMESTAMP | Ingestion timestamp             |

---

## Design Principles

- Clear separation between ingestion and analytics layers
- Idempotent job design
- Partitioned BigQuery tables by trade_date
- Cloud-native deployment
- Reproducible and version-controlled pipeline

---

## Scheduling

Daily ingestion job is triggered via:
- Google Cloud Scheduler
- HTTP trigger to Cloud Run service

---

## Future Improvements

- Transformation layer (dbt)
- Dimensional modeling (star schema)
- Data quality tests
- CI/CD integration
- Monitoring & logging improvements

---

## Disclaimer

This project is for educational and demonstration purposes only.  
Market data is sourced from publicly available APIs.
