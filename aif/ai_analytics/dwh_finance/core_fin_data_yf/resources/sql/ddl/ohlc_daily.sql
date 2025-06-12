CREATE MATERIALIZED VIEW IF NOT EXISTS core_fin_data_yf.ohlc_daily AS
SELECT asset_id,
       price_date,
       open,
       high,
       low,
       close,
       volume
FROM raw_fin_data_yf.ohlc_daily;

COMMENT ON MATERIALIZED VIEW core_fin_data_yf.ohlc_daily IS '{{ COMMENT }}';
