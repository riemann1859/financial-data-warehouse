# Backfill Ingestion on Cloud Run Jobs (Docker + BigQuery Raw Layer)

## Purpose

This document describes how we run a one-time **historical backfill** job that fetches daily stock OHLCV data from Yahoo Finance (via `yfinance`) and writes it into the **BigQuery raw layer** table:

- `gma-prod-001.market_raw.stock_prices`

The backfill is designed to be **safe to re-run** because it writes **one partition per day** using `WRITE_TRUNCATE` on the partition decorator (`$YYYYMMDD`). If the job is executed again, the same date partitions are overwritten rather than duplicated.

---

## Quick glossary (2–3 sentences each)

**Docker / Image**  
Docker packages your code and dependencies into a portable “image”. The same image can run identically on your laptop and on Cloud Run, which prevents “works on my machine” problems.

**Container**  
A container is a running instance of a Docker image. Cloud Run Jobs executes your container as a batch task and captures stdout/stderr into Cloud Logging.

**CLI (Command Line Interface)**  
CLI tools like `gcloud` and `bq` let you manage Google Cloud resources and run queries from your terminal. They are convenient for automation, reproducibility, and quick debugging.

---

## Architecture overview

- **Source**: Yahoo Finance (pulled by `yfinance`).
- **Compute**: Cloud Run **Job** (batch execution, not an HTTP service).
- **Storage**: BigQuery raw layer table partitioned by `trade_date` and clustered by `symbol`.

Table schema (already created):

- `symbol STRING NOT NULL`
- `trade_date DATE NOT NULL` (partition column)
- `open FLOAT64`
- `high FLOAT64`
- `low FLOAT64`
- `close FLOAT64`
- `volume INT64`
- `load_timestamp TIMESTAMP NOT NULL`

---

## Repo structure (relevant files)

- `ingestion/backfill.py`  
  Backfill script: chunked fetching + day-by-day partition overwrite.
- `ingestion/Dockerfile`  
  Builds the image that runs the backfill.
- `entrypoint.sh`  
  Container entrypoint (runs the Python script with args).
- `requirements.txt` (repo root)  
  Python dependencies installed into the container.

---

## Container build & push (PowerShell)

### 1) Set project & region
```powershell
gcloud config set project gma-prod-001
gcloud config set run/region europe-west1

2) Create Artifact Registry repository (one-time)

gcloud artifacts repositories create ingestion `
  --repository-format=docker `
  --location=europe-west1 `
  --description="Docker images for ingestion jobs"


3) Build and push the image (Cloud Build)

This uploads the repo context and builds the container image remotely, then pushes it to Artifact Registry:

gcloud builds submit --tag europe-west1-docker.pkg.dev/gma-prod-001/ingestion/backfill:latest


Cloud Run Job setup (recommended approach)

1) Create a dedicated service account (one-time)

gcloud iam service-accounts create run-backfill-sa `
  --display-name="Cloud Run Backfill Job SA"


2) Grant IAM roles needed for BigQuery loads (one-time)

At minimum:

- Project-level: roles/bigquery.jobUser (to run load jobs)

- Dataset-level: roles/bigquery.dataEditor (to write into the dataset)

Project role:

gcloud projects add-iam-policy-binding gma-prod-001 `
  --member="serviceAccount:run-backfill-sa@gma-prod-001.iam.gserviceaccount.com" `
  --role="roles/bigquery.jobUser"

Dataset role (recommended via gcloud, dataset-level IAM):

gcloud projects add-iam-policy-binding gma-prod-001 `
  --member="serviceAccount:run-backfill-sa@gma-prod-001.iam.gserviceaccount.com" `
  --role="roles/bigquery.dataEditor"


Note: You can scope this tighter by granting dataset-level IAM specifically, but for a learning project this is acceptable.

Job execution configuration
Entrypoint & arguments

Cloud Run Jobs passes container args differently than local shells. The most reliable pattern is to execute through a shell:

Command: /bin/sh

Args: -c "<full command string>"

Example (conceptual):

/bin/sh -c "/usr/local/bin/python /app/backfill.py --start-date ... --end-date ..."

This ensures the Python script receives the CLI flags correctly.

Increase timeout for large backfills

A multi-year backfill can exceed the default 1-hour task timeout. Set it higher (e.g., 4 hours):

gcloud run jobs update market-backfill `
  --region europe-west1 `
  --task-timeout=4h


Backfill behavior (what happens inside backfill.py)

Uses a fixed list of tickers (e.g., ~50).

Fetches data in:

Ticker batches (e.g., 10 tickers per request)

Date chunks (e.g., 2 years per chunk)

Within each date chunk:

Concatenates only chunk data (bounded memory).

Iterates day-by-day and writes to:

market_raw.stock_prices$YYYYMMDD

Uses WRITE_TRUNCATE so each partition is overwritten safely.

Why partition overwrite is the “safe” choice

Even if the job is triggered twice:

You do not need to delete previously loaded data.

Each partition is replaced with the most recently fetched version for that date.

You avoid duplicates without needing complex de-dup logic in BigQuery.

Operational notes

Logs: print({...}) structured logs are visible in Cloud Logging for the Cloud Run Job execution.

Performance: Backfill duration depends on ticker count, date range, and Yahoo response speed.

Validation: You can verify totals and ranges in the BigQuery UI (recommended for quick checks).

