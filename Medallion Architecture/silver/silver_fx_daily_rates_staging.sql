CREATE OR REFRESH STREAMING TABLE silver_fx_daily_rates_staging
AS
SELECT
  CAST(FROM_UNIXTIME(exploded.t/1000) AS DATE) AS date,
  SPLIT(ticker, ':')[1] AS currency_pair, 
  CAST(exploded.o AS DECIMAL(18,6)) AS open,
  CAST(exploded.c AS DECIMAL(18,6)) AS close,
  CAST(exploded.h AS DECIMAL(18,6)) AS high,
  CAST(exploded.l AS DECIMAL(18,6)) AS low,
  CAST(exploded.n AS INTEGER) AS number_of_transactions,
  CAST(exploded.v AS INTEGER) AS trading_volume,
  CAST(exploded.vw AS DECIMAL(18,6)) AS vwap,
  source_file,
  ingestion_time,
  CURRENT_TIMESTAMP() AS processed_time
FROM STREAM(LIVE.bronze_fx_daily_rates) b
LATERAL VIEW explode(b.results) t AS exploded;