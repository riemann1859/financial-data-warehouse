# Project Setup Guide

This document explains how to bootstrap the local development environment and provision the BigQuery RAW layer using CLI tools.

The project follows an infrastructure-first approach:
Datasets and tables are created via version-controlled SQL files instead of manual UI configuration.

---

# 1️⃣ Prerequisites

Make sure the following are installed:

- Python 3.10+
- Google Cloud SDK (gcloud + bq)
- Windows PowerShell and Command Prompt (CMD)

---

# 2️⃣ Python Virtual Environment (Windows – PowerShell)

It is recommended to create the virtual environment **outside the repository folder**.

Example structure:

PythonProjects/
│
├── financial-data-warehouse/
└── fdw_env_finance/


## Create environment

```powershell
python -m venv fdw_env_finance


## Create environment

```powershell
python -m venv fdw_env_finance

Activate environment

From inside the repository directory:

cd C:\Users\<username>\PythonProjects\financial-data-warehouse
..\fdw_env_finance\Scripts\Activate.ps1

You should now see:

(fdw_env_finance)

in your terminal.

3️⃣ Google Cloud SDK

Verify installation:

gcloud --version
bq --version

If commands are not recognized:

Add this directory to your system PATH:

C:\Users\<username>\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin

Then restart your terminal

4️⃣ Set Active GCP Project

Run:

gcloud config set project gma-prod-001

Our project id is 'gma-prod-001'

You may see an organization policy warning about environment tags.
This does not block execution in most cases.

You may see an organization policy warning about environment tags.
This does not block execution in most cases.

cd C:\Users\<username>\PythonProjects\financial-data-warehouse

Run the SQL DDL file:

bq query --use_legacy_sql=false --location=EU < sql\create_raw_tables.sql

Expected output

Created gma-prod-001.market_raw.stock_prices

6️⃣ Validate Creation

bq ls gma-prod-001:
bq ls gma-prod-001:market_raw
bq show gma-prod-001:market_raw.stock_prices

You should see:

Dataset: market_raw

Table: stock_prices

Partitioned by: trade_date

Clustered by: symbol

Common Issues & Fixes
❌ Region Mismatch Error

Error example:

Location specified in query EU is not consistent with current execution region US

Solution:

Make sure you include:

--location=EU

in the bq query command.

❌ Illegal Input Character "\357"

Cause: UTF-8 BOM encoding in SQL file.

Fix:

Re-save SQL file without BOM

Or clean the file before execution

❌ gcloud / bq Not Recognized

Cause: PATH misconfiguration.

Fix:

Add Cloud SDK bin directory to system PATH

Restart terminal

❌ gcloud / bq Not Recognized

Cause: PATH misconfiguration.

Fix:

Add Cloud SDK bin directory to system PATH

Restart terminal

