# ingestion/backfill.py
"""
Production-style backfill job for BigQuery raw layer.

Goal:
- Fetch daily OHLCV data from Yahoo (yfinance) for a fixed ticker list
- Write into BigQuery table:
    gma-prod-001.market_raw.stock_prices
  using partition overwrite per trade_date:
    stock_prices$YYYYMMDD + WRITE_TRUNCATE

Why this is "production-style":
- Does NOT return DataFrames
- Does NOT accumulate all data in memory (no df_all)
- Streams work chunk-by-chunk and day-by-day
- Emits structured logs to stdout (Cloud Logging friendly)
- Fails fast on invalid args

Table schema (already created by you):
- symbol STRING NOT NULL
- trade_date DATE NOT NULL (PARTITION BY)
- open FLOAT64
- high FLOAT64
- low FLOAT64
- close FLOAT64
- volume INT64
- load_timestamp TIMESTAMP NOT NULL
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import Iterable, List, Tuple

import pandas as pd
import yfinance as yf
from google.cloud import bigquery


# -----------------------------
# BigQuery target
# -----------------------------
PROJECT_ID = "gma-prod-001"
DATASET_ID = "market_raw"
TABLE_ID = "stock_prices"


# -----------------------------
# Fixed ticker list (50 stocks)
# -----------------------------
TICKERS_50: List[str] = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","ORCL","CRM","ADBE",
    "AMD","INTC","QCOM","AVGO","TXN",
    "JPM","BAC","WFC","GS","MS","V","MA","AXP","BRK-B",
    "JNJ","UNH","PFE","MRK","ABBV","TMO","AMGN",
    "PG","KO","PEP","MCD","NKE","SBUX","WMT","COST",
    "CAT","HON","GE","BA","LMT","XOM","CVX",
    "T","VZ","NEE",
]


# -----------------------------
# Helpers: date iteration & chunking
# -----------------------------
def iter_days(start: date, end: date) -> Iterable[date]:
    """
    Input:
      - start: inclusive
      - end: inclusive
    Output:
      - yields each date from start..end inclusive
    """
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    """
    Input:
      - items: list of strings
      - chunk_size: positive integer
    Output:
      - list of sublists (batches), each up to chunk_size
    """
    if chunk_size <= 0:
        raise ValueError("ticker-batch-size must be > 0")
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def chunk_date_ranges(start: date, end: date, years_per_chunk: int) -> List[Tuple[date, date]]:
    """
    Split [start,end] into N-year inclusive chunks to reduce fetch risk.

    Input:
      - start: inclusive
      - end: inclusive
      - years_per_chunk: e.g., 2

    Output:
      - list of (chunk_start, chunk_end) inclusive
    """
    if years_per_chunk <= 0:
        raise ValueError("chunk-years must be > 0")

    ranges: List[Tuple[date, date]] = []
    cur = start
    while cur <= end:
        # tentative_end = (cur + N years) - 1 day
        try:
            tentative_end = date(cur.year + years_per_chunk, cur.month, cur.day) - timedelta(days=1)
        except ValueError:
            # handle Feb 29 etc.: fall back to end-of-month
            base = date(cur.year + years_per_chunk, cur.month, 1)
            if base.month == 12:
                tentative_end = date(base.year + 1, 1, 1) - timedelta(days=1)
            else:
                tentative_end = date(base.year, base.month + 1, 1) - timedelta(days=1)

        chunk_end = min(tentative_end, end)
        ranges.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)

    return ranges


# -----------------------------
# Fetch + normalize (Yahoo / yfinance)
# -----------------------------
def fetch_prices_yahoo(tickers: List[str], start: date, end: date) -> pd.DataFrame:
    """
    Fetch daily OHLCV data from Yahoo via yfinance for multiple tickers.

    Input:
      - tickers: list[str]
      - start: inclusive start date
      - end: inclusive end date

    Output:
      - DataFrame with BigQuery schema columns:
          symbol (str)
          trade_date (date)
          open (float)
          high (float)
          low (float)
          close (float)
          volume (int)
          load_timestamp (datetime UTC)
        May be empty.

    Notes:
      - yfinance often treats end as exclusive -> we request end+1 day.
      - We drop duplicates by (trade_date, symbol).
    """
    if not tickers:
        return pd.DataFrame()

    end_plus = end + timedelta(days=1)

    data = yf.download(
        tickers=tickers,
        start=start.isoformat(),
        end=end_plus.isoformat(),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )

    if data is None or (hasattr(data, "empty") and data.empty):
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    if isinstance(data.columns, pd.MultiIndex):
        # columns like (TICKER, Field)
        for t in tickers:
            if t not in data.columns.get_level_values(0):
                continue
            df_t = data[t].copy().reset_index()
            df_t["symbol"] = t
            frames.append(df_t)
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        # single-ticker fallback
        df = data.reset_index()
        df["symbol"] = tickers[0]

    if df.empty:
        return df

    # Normalize to your BigQuery schema
    df.rename(
        columns={
            "Date": "trade_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        },
        inplace=True,
    )

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

    # NOT NULL field
    df["load_timestamp"] = datetime.utcnow()

    # Ensure no duplicates in the dataset
    df = df.drop_duplicates(subset=["trade_date", "symbol"], keep="last")

    return df[["symbol", "trade_date", "open", "high", "low", "close", "volume", "load_timestamp"]]


# -----------------------------
# BigQuery write: partition overwrite
# -----------------------------
# -----------------------------
# BigQuery write: partition overwrite
# -----------------------------
def write_partition_overwrite(bq: bigquery.Client, df_day: pd.DataFrame, target_date: date) -> None:
    """
    Overwrite exactly one BigQuery partition for a given day.

    Input:
      - bq: BigQuery client
      - df_day: DataFrame containing rows for exactly target_date
      - target_date: date of the partition to overwrite

    Output:
      - None. Blocks until the load job completes.

    Behavior:
      - Writes to: PROJECT.DATASET.TABLE$YYYYMMDD
      - write_disposition = WRITE_TRUNCATE
        => overwrites only that partition (safe from duplicates)

    Important:
      - BigQuery table has REQUIRED fields (NOT NULL):
          symbol, trade_date, load_timestamp
      - Pandas -> BigQuery schema inference can mark columns as NULLABLE.
        To prevent schema mismatch, we:
          (1) enforce non-null for REQUIRED columns
          (2) pass an explicit BigQuery schema
    """
    # Defensive copy (avoid SettingWithCopy warnings and accidental view writes)
    df_day = df_day.copy()

    # ---- Enforce REQUIRED / NOT NULL fields ----
    # symbol: REQUIRED STRING
    before = len(df_day)
    df_day["symbol"] = df_day["symbol"].astype(str)
    df_day = df_day[df_day["symbol"].notna() & (df_day["symbol"].str.len() > 0)]

    # trade_date: REQUIRED DATE
    # (Should already be date objects, but enforce)
    df_day["trade_date"] = pd.to_datetime(df_day["trade_date"], errors="coerce").dt.date
    df_day = df_day[df_day["trade_date"].notna()]

    # load_timestamp: REQUIRED TIMESTAMP
    # Ensure datetime; keep it timezone-aware UTC to be safe.
    df_day["load_timestamp"] = pd.to_datetime(df_day["load_timestamp"], errors="coerce", utc=True)
    df_day = df_day[df_day["load_timestamp"].notna()]

    after = len(df_day)
    if after == 0:
        # Nothing valid to write for this partition date
        print({"msg": "partition_skip_no_valid_rows", "trade_date": target_date.isoformat(), "before": int(before)})
        return

    # Target partition
    yyyymmdd = target_date.strftime("%Y%m%d")
    table_partition = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}${yyyymmdd}"

    # Explicit schema to avoid REQUIRED/NULLABLE inference mismatches
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("trade_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("high", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("low", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("close", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("volume", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"),
        ],
    )

    # Load into the specific partition table decorator
    job = bq.load_table_from_dataframe(df_day, table_partition, job_config=job_config)
    job.result()

# -----------------------------
# Orchestration: backfill
# -----------------------------
def run_backfill(
    start: date,
    end: date,
    tickers: List[str],
    ticker_batch_size: int,
    chunk_years: int,
    dry_run: bool,
) -> None:
    """
    Execute backfill.

    Input:
      - start, end: inclusive date range
      - tickers: list of ticker symbols to fetch
      - ticker_batch_size: number of tickers per Yahoo request
      - chunk_years: years per date chunk for Yahoo requests
      - dry_run: if True => fetch but DO NOT write to BigQuery

    Output:
      - None

    Process:
      - For each date chunk:
          - Fetch data for each ticker batch
          - Concatenate within this chunk only (bounded memory)
          - For each day in the chunk:
              - Filter df_day and overwrite BigQuery partition
    """
    bq = None if dry_run else bigquery.Client(project=PROJECT_ID)

    ticker_batches = chunk_list(tickers, ticker_batch_size)
    date_chunks = chunk_date_ranges(start, end, chunk_years)

    print({
        "msg": "backfill_plan",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "tickers": len(tickers),
        "ticker_batch_size": ticker_batch_size,
        "chunk_years": chunk_years,
        "ticker_batches": len(ticker_batches),
        "date_chunks": len(date_chunks),
        "dry_run": dry_run,
    })

    for (c_start, c_end) in date_chunks:
        print({"msg": "date_chunk_start", "start": c_start.isoformat(), "end": c_end.isoformat()})

        chunk_frames: List[pd.DataFrame] = []

        # Fetch: ticker batches within this date chunk
        for batch in ticker_batches:
            print({"msg": "fetch_start", "start": c_start.isoformat(), "end": c_end.isoformat(), "tickers": batch})
            df_part = fetch_prices_yahoo(batch, c_start, c_end)
            print({"msg": "fetch_done", "rows": int(len(df_part)), "tickers_count": len(batch)})
            if not df_part.empty:
                chunk_frames.append(df_part)

        if not chunk_frames:
            print({"msg": "date_chunk_no_data", "start": c_start.isoformat(), "end": c_end.isoformat()})
            continue

        # Concatenate only within this chunk (bounded memory)
        df_chunk = pd.concat(chunk_frames, ignore_index=True)
        df_chunk = df_chunk.drop_duplicates(subset=["trade_date", "symbol"], keep="last")

        if dry_run:
            # In dry-run, we don't write. We just log a summary and move on.
            print({
                "msg": "date_chunk_summary",
                "start": c_start.isoformat(),
                "end": c_end.isoformat(),
                "rows": int(len(df_chunk)),
                "unique_symbols": int(df_chunk["symbol"].nunique()),
                "min_trade_date": str(df_chunk["trade_date"].min()),
                "max_trade_date": str(df_chunk["trade_date"].max()),
            })
            continue

        # Write: day partitions overwrite
        assert bq is not None
        for d in iter_days(c_start, c_end):
            df_day = df_chunk[df_chunk["trade_date"] == d]
            if df_day.empty:
                continue
            write_partition_overwrite(bq, df_day, d)
            print({"msg": "written_partition", "trade_date": d.isoformat(), "rows": int(len(df_day))})

        print({"msg": "date_chunk_done", "start": c_start.isoformat(), "end": c_end.isoformat()})

    print({"msg": "backfill_done"})


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    """
    CLI args for Cloud Run Job and local execution.

    Required:
      --start-date YYYY-MM-DD
      --end-date   YYYY-MM-DD

    Optional:
      --ticker-batch-size (default 10)
      --chunk-years       (default 2)
      --dry-run           (default False)
    """
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--ticker-batch-size", type=int, default=10, help="Tickers per Yahoo request")
    p.add_argument("--chunk-years", type=int, default=2, help="Years per Yahoo date chunk")
    p.add_argument("--dry-run", action="store_true", help="Fetch only; do not write to BigQuery")
    return p.parse_args()


def main() -> None:
    """
    Cloud Run Job entrypoint:
      - Parse args
      - Execute backfill
      - Exit with code 0 if successful, exception otherwise
    """
    args = parse_args()

    print({"msg": "args", "start_date": args.start_date, "end_date": args.end_date,
       "ticker_batch_size": args.ticker_batch_size, "chunk_years": args.chunk_years, "dry_run": args.dry_run})


    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if end < start:
        raise ValueError("end-date cannot be earlier than start-date")

    run_backfill(
        start=start,
        end=end,
        tickers=TICKERS_50,
        ticker_batch_size=args.ticker_batch_size,
        chunk_years=args.chunk_years,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    import os
    import traceback

    try:
        print({"msg": "boot", "python": "3.11", "project": os.getenv("GOOGLE_CLOUD_PROJECT")})
        main()
    except Exception as e:
        # Print full traceback to logs so Cloud Run shows the real error
        print({"msg": "fatal_error", "error": str(e)})
        traceback.print_exc()
        raise
