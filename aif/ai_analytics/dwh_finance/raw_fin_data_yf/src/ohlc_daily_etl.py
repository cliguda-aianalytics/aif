"""This modul implements the etl logic for the OHLC table. Data is extracted from Yahoo Finance. We separate the
implementation logic from the asset definition."""

import pandas as pd
import yfinance as yf
import numpy as np

from aif.common.aif.src.data_interfaces.db_interface import DBInterface
from aif.common.dagster.asset_etl import ETLAsset


class OhlcETL(ETLAsset):
    """Implements the etl logic for the OHLC table. Data is extracted from Yahoo Finance."""

    def __init__(self, fail_on_missing_entries: bool, asset_symbol: str):
        super().__init__(fail_on_missing_entries)
        self.asset_symbol = asset_symbol

    def extract(self) -> pd.DataFrame:
        """Extract: Get historical data for asset."""
        return yf.download(tickers=self.asset_symbol)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform: Adjust the Dataframe to DB Schema. For simplification, we do not consider splits/Adj. Close"""
        df = df.reset_index()  # Date as column
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

        # YahooYF sometimes returns invalid values, so we enforce some rules
        # (For real applications, better handling is required)
        df["High"] = np.maximum(df["High"], df["Open"])
        df["Low"] = np.minimum(df["Low"], df["Open"])
        df["Close"] = np.maximum(df["Close"], df["Low"])

        return df

    def load(self, df: pd.DataFrame) -> int:
        """Load data into the DWH_FINANCE"""
        with DBInterface(db_cfg="DWH_FINANCE") as db:
            schema = "raw_fin_data_yf"
            db_res = db.execute_insert(
                data_df=df,
                schema=schema,
                table_name="ohlc_daily",
                filename="aif/ai_analytics/dwh_finance/raw_fin_data_yf/resources/sql/dml/ohlc_daily_insert.sql",
                parameters={"asset_id": self.asset_symbol},
            )

        return db_res.metadata["missing"]
