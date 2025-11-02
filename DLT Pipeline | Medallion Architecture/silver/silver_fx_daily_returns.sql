CREATE OR REFRESH MATERIALIZED VIEW silver_fx_daily_returns
(
  date DATE COMMENT "Trading date (includes Sunday evening data when FX markets open)",
  currency_pair STRING COMMENT "Format: EURUSD, GBPUSD, USDJPY, USDCHF, USDBRL",
  close_rate DECIMAL(18,6) COMMENT "End-of-day close rate",
  prev_close_rate DECIMAL(18,6) COMMENT "Previous trading day close (automatically skips Saturday gaps)",
  daily_return DECIMAL(18,16) COMMENT "Percentage return: (today_close / prev_close) - 1",
  processed_time TIMESTAMP NOT NULL,
  CONSTRAINT valid_return EXPECT (daily_return IS NOT NULL OR prev_close_rate IS NULL) 
    ON VIOLATION DROP ROW,
  CONSTRAINT reasonable_return EXPECT (ABS(daily_return) < 0.15 OR daily_return IS NULL)
    ON VIOLATION FAIL UPDATE
)
COMMENT "FX daily returns - calculated using trading days only (Polygon API excludes Saturdays)"
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true
)
AS
SELECT
  date,
  currency_pair,
  CAST(close AS DECIMAL(18,6)) AS close_rate,
  CAST(LAG(close) OVER (PARTITION BY currency_pair ORDER BY date) AS DECIMAL(18,6)) AS prev_close_rate,
  CAST((close / LAG(close) OVER (PARTITION BY currency_pair ORDER BY date)) - 1 AS DECIMAL(18,16)) AS daily_return,
  CURRENT_TIMESTAMP() AS processed_time
FROM LIVE.silver_fx_daily_rates