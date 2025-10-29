CREATE OR REFRESH STREAMING TABLE silver_daily_positions
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);

CREATE FLOW silver_daily_positions_flow  
AS AUTO CDC INTO 
  silver_daily_positions
FROM STREAM(LIVE.bronze_daily_positions)
  KEYS(date, currency_pair)
  SEQUENCE BY ingestion_time;