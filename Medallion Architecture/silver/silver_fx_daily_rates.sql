CREATE OR REFRESH STREAMING TABLE silver_fx_daily_rates
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);

CREATE FLOW silver_fx_daily_rates_flow  
AS AUTO CDC INTO 
  silver_fx_daily_rates
FROM STREAM(LIVE.silver_fx_daily_rates_staging)
  KEYS(date, currency_pair)
  SEQUENCE BY ingestion_time;