CREATE MATERIALIZED VIEW gold_fct_var AS
WITH aggregated_daily_returns AS (
  SELECT
      date,
      currency_pair,
      daily_return
  FROM LIVE.silver_fx_daily_returns
),

daily_returns_to_calculate_percentiles AS (
  SELECT
    t1.date,
    t2.date AS prev_date,
    t1.currency_pair,
    t1.daily_return,
    t2.daily_return as previous_daily_returns,
    row_number() over (PARTITION BY t1.date, t1.currency_pair ORDER BY t2.date DESC) rn
  FROM aggregated_daily_returns t1
  JOIN aggregated_daily_returns t2
    ON t2.date BETWEEN t1.date - INTERVAL 35 DAYS AND t1.date
      AND t2.currency_pair = t1.currency_pair
),

daily_returns_percentiles AS (
  SELECT
    date,
    currency_pair,
    daily_return,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY previous_daily_returns) 5th_percentile,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY previous_daily_returns) 95th_percentile
  FROM daily_returns_to_calculate_percentiles
  WHERE date >= '2025-01-01' AND rn <= 30
  GROUP BY 1, 2, 3
),

rates AS (
  SELECT
    date,
    currency_pair,
    close AS current_rate
  FROM LIVE.silver_fx_daily_rates
)

SELECT
  dp.date,
  dp.currency_pair,
  desk,
  position_size,
  direction,
  5th_percentile,
  95th_percentile,
  CASE 
    WHEN direction = 'LONG' THEN position_size * ABS(5th_percentile)
    WHEN direction = 'SHORT' THEN position_size * ABS(95th_percentile)
    ELSE 0 
  END AS var_95_base_currency,
  CASE 
    WHEN LEFT(dp.currency_pair, 3) = 'USD' THEN
        CASE
            WHEN direction = 'LONG' THEN position_size * ABS(5th_percentile)
            WHEN direction = 'SHORT' THEN position_size * ABS(95th_percentile)
        ELSE 0 
        END
    ELSE
        CASE
            WHEN direction = 'LONG' THEN position_size * ABS(5th_percentile) * current_rate
            WHEN direction = 'SHORT' THEN position_size * ABS(95th_percentile) * current_rate
        ELSE 0 
        END
  END AS var_95_usd,
  stddev(daily_return) OVER (PARTITION BY dp.currency_pair ORDER BY dp.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) volatility_30d,
  current_rate
FROM LIVE.silver_daily_positions dp
JOIN daily_returns_percentiles rp
  ON dp.date = rp.date AND dp.currency_pair = rp.currency_pair
JOIN rates r
  ON dp.date = r.date AND dp.currency_pair = r.currency_pair 