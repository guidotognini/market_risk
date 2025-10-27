CREATE OR REFRESH STREAMING TABLE bronze_fx_daily_rates
(
  CONSTRAINT valid_ticker EXPECT (ticker IS NOT NULL),
  CONSTRAINT valid_results EXPECT (results IS NOT NULL)
)
AS
SELECT
  ticker,
  results,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM read_files(
  '/Volumes/tabular/dataexpert/guidotognini/market_risk/raw_fx_daily_rates/*',
  'json',
  MAP(
    'cloudFiles.includeExistingFiles', 'true',
    'cloudFiles.schemaLocation', '/Volumes/tabular/dataexpert/guidotognini/market_risk/_schemas/fx_daily_rates_bronze',
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaEvolutionMode', 'rescue'
  )
);