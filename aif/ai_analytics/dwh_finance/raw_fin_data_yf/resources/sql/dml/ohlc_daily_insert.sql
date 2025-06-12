INSERT INTO raw_fin_data_yf.ohlc_daily
(asset_id, price_date, open, high, low, close, volume)
VALUES ('{asset_id}', %s, %s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT raw_fin_data_yf_ohlc_daily_pkey do nothing
;