-- Create Raw Dataset
CREATE SCHEMA IF NOT EXISTS `gma-prod-001.market_raw`
OPTIONS (
  location = "EU"
);

-- Create Table
CREATE TABLE IF NOT EXISTS `gma-prod-001.market_raw.stock_prices`
(
    symbol STRING NOT NULL,
    trade_date DATE NOT NULL,
    open FLOAT64,
    high FLOAT64,
    low FLOAT64,
    close FLOAT64,
    volume INT64,
    load_timestamp TIMESTAMP NOT NULL
)
PARTITION BY trade_date
CLUSTER BY symbol;
