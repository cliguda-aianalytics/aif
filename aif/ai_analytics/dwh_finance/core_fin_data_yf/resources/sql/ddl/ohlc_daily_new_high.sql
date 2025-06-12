CREATE OR REPLACE VIEW core_fin_data_yf.ohlc_daily_new_high AS (
SELECT
    price_date,
    asset_id,
    high
FROM
    (SELECT
        price_date,
        asset_id,
        high,
        MAX(high) OVER (PARTITION BY asset_id
            ORDER BY price_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS maxHigh
    FROM
        core_fin_data_yf.ohlc_daily) sub
WHERE
    high = maxHigh
);

COMMENT ON VIEW core_fin_data_yf.ohlc_daily_new_high IS '{{ COMMENT }}';
