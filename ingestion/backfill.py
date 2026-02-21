# ingestion/backfill.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

import pandas as pd
import yfinance as yf
from google.cloud import bigquery


# BigQuery hedefi (senin DDL'ine göre)
PROJECT_ID = "gma-prod-001"
DATASET = "market_raw"
TABLE = "stock_prices"

# Sabit 50 ticker
TICKERS_50 = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","ORCL","CRM","ADBE",
    "AMD","INTC","QCOM","AVGO","TXN",
    "JPM","BAC","WFC","GS","MS","V","MA","AXP","BRK-B",
    "JNJ","UNH","PFE","MRK","ABBV","TMO","AMGN",
    "PG","KO","PEP","MCD","NKE","SBUX","WMT","COST",
    "CAT","HON","GE","BA","LMT","XOM","CVX",
    "T","VZ","NEE",
]


@dataclass(frozen=True)
class BackfillConfig:
    start: date
    end: date
    ticker_batch_size: int = 10
    chunk_years: int = 2
    write_to_bq: bool = False
    output: Optional[str] = None
    print_head: int = 0


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def chunk_list(items: List[str], n: int) -> List[List[str]]:
    return [items[i:i + n] for i in range(0, len(items), n)]


def chunk_date_ranges(start: date, end: date, years: int) -> List[tuple[date, date]]:
    """
    [start, end] aralığını 'years' yıllık parçalara böler.
    29 Şubat gibi edge case'lerde basit ama yeterli bir düzeltme yapar.
    """
    ranges: List[tuple[date, date]] = []
    cur = start
    while cur <= end:
        try:
            chunk_end = date(cur.year + years, cur.month, cur.day) - timedelta(days=1)
        except ValueError:
            # 29 Şubat gibi günlerde ay sonuna çek
            tmp = date(cur.year + years, cur.month, 1)
            if tmp.month == 12:
                chunk_end = date(tmp.year + 1, 1, 1) - timedelta(days=1)
            else:
                chunk_end = date(tmp.year, tmp.month + 1, 1) - timedelta(days=1)

        if chunk_end > end:
            chunk_end = end

        ranges.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)

    return ranges


def fetch_range_yahoo(tickers: List[str], start: date, end: date) -> pd.DataFrame:
    """
    Yahoo'dan günlük OHLCV çeker ve BigQuery şemasına uyarlar:
    symbol, trade_date, open, high, low, close, volume, load_timestamp
    """
    # yfinance end çoğu zaman exclusive; end+1 ile garanti ediyoruz
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

    rows = []
    if isinstance(data.columns, pd.MultiIndex):
        for t in tickers:
            if t not in data.columns.get_level_values(0):
                continue
            df_t = data[t].copy().reset_index()
            df_t["symbol"] = t
            rows.append(df_t)
        df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    else:
        # Tek ticker durumunda multiindex gelmeyebilir
        df = data.reset_index()
        df["symbol"] = tickers[0]

    if df.empty:
        return df

    df.rename(columns={
        "Date": "trade_date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    }, inplace=True)

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

    # NOT NULL
    df["load_timestamp"] = datetime.utcnow()

    # Aynı (trade_date, symbol) iki satır olmasın
    df = df.drop_duplicates(subset=["trade_date", "symbol"], keep="last")

    return df[["symbol", "trade_date", "open", "high", "low", "close", "volume", "load_timestamp"]]


def write_partition_overwrite(bq: bigquery.Client, df_day: pd.DataFrame, d: date):
    """
    Sadece ilgili günü overwrite eder: stock_prices$YYYYMMDD
    Duplicate riskini kökten bitirir.
    """
    yyyymmdd = d.strftime("%Y%m%d")
    target = f"{PROJECT_ID}.{DATASET}.{TABLE}${yyyymmdd}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = bq.load_table_from_dataframe(df_day, target, job_config=job_config)
    job.result()


def main(
    start_date: str | None = None,
    end_date: str | None = None,
    ticker_batch_size: int = 10,
    chunk_years: int = 2,
    write: bool = False,
    output: Optional[str] = None,
    print_head: int = 0,
    tickers: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    - Local kontrol: df = main(..., write=False)  -> concat DF döndürür
    - Cloud Run:   main(..., write=True) -> BigQuery'ye yazar (DF yine döner ama kullanılmaz)
    """
    if start_date is None or end_date is None:
        raise ValueError("start_date ve end_date zorunlu. (YYYY-MM-DD)")

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date start_date'den küçük olamaz.")

    tickers = tickers or TICKERS_50
    ticker_batches = chunk_list(tickers, ticker_batch_size)
    date_chunks = chunk_date_ranges(start, end, chunk_years)

    bq = bigquery.Client(project=PROJECT_ID) if write else None

    all_frames: List[pd.DataFrame] = []

    # Risk azaltma: tarih chunk + ticker batch
    for (c_start, c_end) in date_chunks:
        chunk_frames: List[pd.DataFrame] = []

        for batch in ticker_batches:
            df_part = fetch_range_yahoo(batch, c_start, c_end)
            if not df_part.empty:
                chunk_frames.append(df_part)

        if not chunk_frames:
            continue

        df_chunk = pd.concat(chunk_frames, ignore_index=True)
        df_chunk = df_chunk.drop_duplicates(subset=["trade_date", "symbol"], keep="last")

        # Local kontrol için biriktir
        all_frames.append(df_chunk)

        # Gerçek yazma modu: partition overwrite
        if write and bq is not None:
            for d in daterange(c_start, c_end):
                df_day = df_chunk[df_chunk["trade_date"] == d]
                if df_day.empty:
                    continue
                write_partition_overwrite(bq, df_day, d)

    df_all = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    if not df_all.empty:
        df_all = df_all.drop_duplicates(subset=["trade_date", "symbol"], keep="last")

    # Local'de hızlı göz kontrolü
    if print_head and not df_all.empty:
        print(df_all.sort_values(["trade_date", "symbol"]).head(print_head).to_string(index=False))

    # Local output (dry-run için)
    if output and not df_all.empty:
        if output.endswith(".parquet"):
            df_all.to_parquet(output, index=False)
        elif output.endswith(".csv"):
            df_all.to_csv(output, index=False)
        else:
            raise ValueError("output .csv veya .parquet olmalı")
        print(f"saved_to={output}")

    return df_all


def parse_args() -> BackfillConfig:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--ticker-batch-size", type=int, default=10)
    p.add_argument("--chunk-years", type=int, default=2)

    # default: dry-run (write yok)
    p.add_argument("--write", action="store_true", help="BigQuery'ye yaz (default: dry-run)")

    p.add_argument("--output", default=None, help="Dry-run çıktısını kaydet (.csv veya .parquet)")
    p.add_argument("--print-head", type=int, default=0, help="Ekrana N satır bas")

    args = p.parse_args()

    return BackfillConfig(
        start=date.fromisoformat(args.start_date),
        end=date.fromisoformat(args.end_date),
        ticker_batch_size=args.ticker_batch_size,
        chunk_years=args.chunk_years,
        write_to_bq=args.write,
        output=args.output,
        print_head=args.print_head,
    )


if __name__ == "__main__":
    cfg = parse_args()
    df = main(
        start_date=cfg.start.isoformat(),
        end_date=cfg.end.isoformat(),
        ticker_batch_size=cfg.ticker_batch_size,
        chunk_years=cfg.chunk_years,
        write=cfg.write_to_bq,
        output=cfg.output,
        print_head=cfg.print_head,
    )
    # Özet bas (hep)
    print(f"rows={len(df)}")
    if not df.empty:
        dup = int(df.duplicated(subset=['trade_date', 'symbol']).sum())
        print(f"duplicates(trade_date,symbol)={dup}")
        print(f"date_range={df['trade_date'].min()}..{df['trade_date'].max()}")
        print(f"unique_symbols={df['symbol'].nunique()}")
