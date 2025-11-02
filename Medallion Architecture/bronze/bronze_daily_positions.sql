-- Bronze Layer: Daily Trading Positions
-- Ingests position files using Auto Loader
-- Configuration: Paths are parameterized via DLT pipeline config

CREATE STREAMING TABLE bronze_daily_positions (
  CONSTRAINT currency_pair_not_null EXPECT (currency_pair IS NOT NULL),
  CONSTRAINT valid_position EXPECT (position_size IS NOT NULL)
)
COMMENT "Raw trading positions - Bronze layer with schema evolution"
AS SELECT
    date,
    currency_pair,
    desk,
    direction,
    position_size,
    _metadata.file_name AS source_file,
    _metadata.file_modification_time AS ingestion_time
FROM cloud_files(
  '${positions_raw_path}/',
  'json',
  MAP(
    'cloudFiles.includeExistingFiles', 'true',
    'cloudFiles.schemaLocation', '${positions_schema_path}',
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaEvolutionMode', 'rescue'
  )
);