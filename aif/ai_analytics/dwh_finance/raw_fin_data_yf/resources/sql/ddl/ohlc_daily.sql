CREATE TABLE IF NOT EXISTS raw_fin_data_yf.ohlc_daily (
    asset_id varchar(21) NOT NULL,
    price_date date NOT NULL,
    open float NOT NULL CHECK(open >= 0),
    high float NOT NULL CHECK(high >= open),
    low float NOT NULL CHECK(low <= open),
    close float NOT NULL CHECK(close >= low),
    volume float NOT NULL CHECK(volume >= 0),
    CONSTRAINT raw_fin_data_yf_ohlc_daily_pkey PRIMARY KEY (asset_id, price_date)
);

COMMENT ON TABLE raw_fin_data_yf.ohlc_daily IS '{{ COMMENT }}';
