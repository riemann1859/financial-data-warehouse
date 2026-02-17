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

### Table: `raw_prices_daily`

| Column      | Type      | Description                     |
|------------|----------|---------------------------------|
| asset_id   | STRING   | Ticker symbol                   |
| trade_date | DATE     | Trading date                    |
| open       | FLOAT    | Opening price                   |
| high       | FLOAT    | Highest price of the day        |
| low        | FLOAT    | Lowest price of the day         |
| close      | FLOAT    | Closing price                   |
| volume     | INT64    | Daily traded volume             |
| load_ts    | TIMESTAMP| Ingestion timestamp             |

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
# financial-data-warehouse
Financial market data warehouse on BigQuery with dbt transformations and Looker Studio dashboards.
