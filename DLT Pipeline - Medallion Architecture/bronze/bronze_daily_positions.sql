CREATE STREAMING TABLE bronze_daily_positions (
  CONSTRAINT currency_pair_not_null EXPECT (currency_pair IS NOT NULL),
  CONSTRAINT valid_position EXPECT (position_size IS NOT NULL)
)
AS SELECT
    date,
    currency_pair,
    desk,
    direction,
    position_size,
    _metadata.file_name AS source_file,
    _metadata.file_modification_time AS ingestion_time
FROM cloud_files(
  "/Volumes/tabular/dataexpert/guidotognini/market_risk/raw_positions/",
  'json',
  MAP(
    'cloudFiles.includeExistingFiles', 'true',
    'cloudFiles.schemaLocation', '/Volumes/tabular/dataexpert/guidotognini/market_risk/_schemas/raw_positions',
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaEvolutionMode', 'rescue'
  )
);