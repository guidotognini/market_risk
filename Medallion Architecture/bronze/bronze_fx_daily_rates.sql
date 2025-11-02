-- Bronze Layer: FX Daily Rates
-- Ingests raw JSON files from Polygon API using Auto Loader
-- Configuration: Paths are parameterized via DLT pipeline config

CREATE OR REFRESH STREAMING TABLE bronze_fx_daily_rates
(
  CONSTRAINT valid_ticker EXPECT (ticker IS NOT NULL),
  CONSTRAINT valid_results EXPECT (results IS NOT NULL)
)
COMMENT "Raw FX rates from Polygon API - Bronze layer with schema evolution"
AS
SELECT
  ticker,
  results,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM cloud_files(
  '${fx_rates_raw_path}/*',
  'json',
  MAP(
    'cloudFiles.includeExistingFiles', 'true',
    'cloudFiles.schemaLocation', '${fx_rates_schema_path}',
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaEvolutionMode', 'rescue'
  )
);