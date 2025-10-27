CREATE OR REFRESH STREAMING TABLE bronze_fx_daily_rates
AS
SELECT *
FROM cloud_files('/Volumes/tabular/dataexpert/guidotognini/market_risk/raw_fx_daily_rates/*', 'json')
