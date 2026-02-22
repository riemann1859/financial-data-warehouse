from __future__ import annotations
# Future annotations: type hint'lerin string olarak değerlendirilmesini sağlar (daha temiz typing)

import argparse  # Komut satırından parametre almak için kullanılır (örn: --symbols AAPL,MSFT)
import logging   # Production log üretmek için (print yerine logging tercih edilir)
import os        # Environment variable (ortam değişkeni) okumak için
import random    # Retry mekanizmasında jitter (küçük rastgele bekleme) için
import sys       # Program exit kodu döndürmek için
import time      # Retry sırasında sleep için
from datetime import date, datetime, timedelta, timezone  # Tarih hesaplamaları için
from typing import Dict, List
from zoneinfo import ZoneInfo  # Timezone-aware datetime üretmek için (örn Europe/Istanbul)

from google.cloud import bigquery  # BigQuery client library


# ---------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------
def setup_logging() -> None:
    """
    logging.basicConfig:
    Python'da global logging ayarlarını yapar.
    Seviyeyi (INFO, DEBUG vs) ve formatı belirler.
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    # os.getenv(key, default):
    # Environment variable okur.
    # Eğer değişken yoksa ikinci parametre default olarak kullanılır.
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


logger = logging.getLogger("daily_stock_ingestion")
# getLogger:
# İsimli logger oluşturur. Cloud Run loglarında bu isim görünür.


# ---------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """
    argparse:
    Scripti terminalden parametre ile çalıştırabilmemizi sağlar.
    Örn:
    python main.py --symbols AAPL,MSFT
    """
    p = argparse.ArgumentParser(
        description="Daily OHLCV ingestion into BigQuery using partition overwrite."
    )

    p.add_argument("--project-id", default=os.getenv("GCP_PROJECT") or os.getenv("PROJECT_ID"))
    # GCP_PROJECT:
    # Cloud Run ortamında otomatik gelir.
    # Lokal çalıştırırken env olarak set edilebilir.

    p.add_argument("--dataset", default=os.getenv("BQ_DATASET", "market_raw"))
    p.add_argument("--table", default=os.getenv("BQ_TABLE", "stock_prices"))

    p.add_argument("--symbols", default=os.getenv("SYMBOLS", ""))
    # Virgülle ayrılmış sembol listesi (AAPL,MSFT,TSLA)

    p.add_argument("--target-date", default=os.getenv("TARGET_DATE", ""))
    # YYYY-MM-DD formatında manuel backfill için

    p.add_argument("--timezone", default=os.getenv("TZ", "Europe/Istanbul"))
    # Dün hesabını hangi timezone'a göre yapacağımız

    p.add_argument("--max-attempts", type=int, default=4)
    p.add_argument("--base-backoff-sec", type=float, default=1.0)

    return p.parse_args()


# ---------------------------------------------------------
# Date Helpers
# ---------------------------------------------------------
def compute_yesterday(tz_name: str) -> date:
    """
    ZoneInfo:
    Timezone-aware datetime üretir.
    Böylece TR saatine göre 'dün' hesaplanır.
    """
    now_local = datetime.now(ZoneInfo(tz_name))
    return (now_local - timedelta(days=1)).date()


def parse_target_date(s: str) -> date:
    """
    datetime.strptime:
    String'i (YYYY-MM-DD) date objesine çevirir.
    """
    return datetime.strptime(s, "%Y-%m-%d").date()


# ---------------------------------------------------------
# Retry Logic (Minimal Production Pattern)
# ---------------------------------------------------------
def with_retry(fn, *, max_attempts: int, base_backoff_sec: float) -> None:
    """
    Basit exponential backoff.
    Eğer API veya network hatası olursa tekrar dener.
    """
    attempt = 1
    while True:
        try:
            fn()
            return
        except Exception as e:
            if attempt >= max_attempts:
                logger.exception("Max attempts reached; failing.")
                raise

            sleep_s = base_backoff_sec * (2 ** (attempt - 1))
            # 2**(attempt-1):
            # Exponential backoff hesaplaması

            sleep_s += random.uniform(0, 0.5)
            # random.uniform:
            # Retry'lerin aynı anda çakışmaması için küçük rastgele bekleme ekler

            logger.warning(f"Retrying in {sleep_s:.2f} seconds...")
            time.sleep(sleep_s)
            attempt += 1


# ---------------------------------------------------------
# Fetch OHLCV (Example: yfinance)
# ---------------------------------------------------------
def fetch_ohlcv(symbols: List[str], target: date) -> List[Dict]:
    """
    yfinance:
    Yahoo Finance'ten günlük OHLCV verisi çeker.

    Not: yfinance bazen kolonları MultiIndex (örn: ('Open','AAPL')) olarak döndürebilir.
    Bu yüzden her sembol için df'yi "düzleştirip" scalar değer alıyoruz.
    """
    import pandas as pd
    import yfinance as yf

    start = pd.Timestamp(target)
    end = start + pd.Timedelta(days=1)

    rows: List[Dict] = []

    for sym in symbols:
        df = yf.download(
            tickers=sym,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            group_by="column",  # kolon bazında grupla; yine de MultiIndex gelebilir
            auto_adjust=False,
            threads=False,
        )

        if df is None or df.empty:
            continue

        # Eğer MultiIndex kolon geldiyse (örn: ('Open','AAPL')), tek seviyeye indir
        if isinstance(df.columns, pd.MultiIndex):
            # Beklenen şekil: level0=OHLCV, level1=symbol
            # Biz tek symbol istediğimiz için level1'i drop edebiliriz
            if df.columns.nlevels >= 2:
                df.columns = df.columns.get_level_values(0)

        # Bazı durumlarda yine DataFrame dönebilir; ilk satırı alıp scalar'a çevir
        s = df.iloc[0]

        def _scalar(x):
            # x bazen scalar, bazen 1 elemanlı Series olabilir
            if isinstance(x, pd.Series):
                if len(x) == 0:
                    return None
                x = x.iloc[0]
            if pd.isna(x):
                return None
            return x

        open_v = _scalar(s.get("Open"))
        high_v = _scalar(s.get("High"))
        low_v = _scalar(s.get("Low"))
        close_v = _scalar(s.get("Close"))
        vol_v = _scalar(s.get("Volume"))

        rows.append(
            {
                "trade_date": target.isoformat(),
                "symbol": sym,
                "open": float(open_v) if open_v is not None else None,
                "high": float(high_v) if high_v is not None else None,
                "low": float(low_v) if low_v is not None else None,
                "close": float(close_v) if close_v is not None else None,
                "volume": int(vol_v) if vol_v is not None else None,
            }
        )

    return rows




# ---------------------------------------------------------
# BigQuery Write (Partition Overwrite)
# ---------------------------------------------------------
def write_partition_overwrite(
    bq: bigquery.Client,
    project_id: str,
    dataset: str,
    table: str,
    target: date,
    rows: List[Dict],
) -> None:
    """
    BigQuery partition decorator:
    table$YYYYMMDD formatı sadece o partition'ı overwrite eder.
    """
    partition_id = target.strftime("%Y%m%d")
    destination = f"{project_id}.{dataset}.{table}${partition_id}"

    ingested_at = datetime.now(timezone.utc).isoformat()

    for r in rows:
        r["ingested_at"] = ingested_at

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        # WRITE_TRUNCATE:
        # Var olan partition içeriğini siler ve yenisini yazar.
    )

    job = bq.load_table_from_json(rows, destination, job_config=job_config)
    job.result()  # job bitene kadar bekler


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def main() -> int:
    setup_logging()
    args = parse_args()

    if not args.project_id:
        logger.error("Missing project id.")
        return 2

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        logger.error("No symbols provided.")
        return 2

    target = parse_target_date(args.target_date) if args.target_date else compute_yesterday(args.timezone)

    logger.info(f"Target date: {target}")

    bq = bigquery.Client(project=args.project_id)
    # bigquery.Client:
    # BigQuery ile iletişim kurmak için client objesi oluşturur.

    def run_once():
        rows = fetch_ohlcv(symbols, target)

        if not rows:
            logger.info("No data fetched. Skipping BigQuery write.")
            return

        write_partition_overwrite(
            bq,
            args.project_id,
            args.dataset,
            args.table,
            target,
            rows,
        )

    with_retry(run_once, max_attempts=args.max_attempts, base_backoff_sec=args.base_backoff_sec)

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
    # SystemExit:
    # Programı belirli exit code ile sonlandırır.
