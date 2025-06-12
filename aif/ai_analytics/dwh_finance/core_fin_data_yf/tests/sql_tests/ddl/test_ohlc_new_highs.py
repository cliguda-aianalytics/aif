"""Tests for ohlc_new_highs function."""

import datetime as dt

import pandas as pd

from aif.common.aif.src.data_interfaces.db_interface import DBInterface
from aif.common.aif.src.initialization import initialize_aif

CONFIG_FILES = [
    "aif/common/aif/resources/config/base.yaml",
    "aif/ai_analytics/resources/config/{ENV}/dwh.yaml",
]


def test_ohlc_new_highs():
    """The test for ohlc_new_highs function."""

    initialize_aif(config_files=CONFIG_FILES)

    with DBInterface(db_cfg="DWH_FINANCE") as db:
        # Create Schemas
        db.create_schema(schema_name="RAW_FIN_DATA_YF", schema_comment="")
        db.create_schema(schema_name="CORE_FIN_DATA_YF", schema_comment="")

        # Drop existing tables and views
        db.drop_view(view_name="core_fin_data_yf.ohlc_daily_new_high", materialized=False)
        db.drop_view(view_name="core_fin_data_yf.ohlc_daily", materialized=True)
        db.drop_table(table_name="raw_fin_data_yf.ohlc_daily")

        # Create new tables and views
        db.execute_statement_from_file(
            filename="aif/ai_analytics/dwh_finance/raw_fin_data_yf/resources/sql/ddl/ohlc_daily.sql",
            parameters={
                "COMMENT": "Raw test data",
            },
        )

        db.execute_statement_from_file(
            filename="aif/ai_analytics/dwh_finance/core_fin_data_yf/resources/sql/ddl/ohlc_daily.sql",
            parameters={
                "COMMENT": "View on raw test data",
            },
        )

        db.execute_statement_from_file(
            filename="aif/ai_analytics/dwh_finance/core_fin_data_yf/resources/sql/ddl/ohlc_daily_new_high.sql"
        )

        # Insert test data
        start_date = dt.datetime.strptime("2020-01-01", "%Y-%m-%d").date()
        ohlc_df = pd.DataFrame(
            {
                "price_date": [
                    start_date,
                    start_date + dt.timedelta(days=1),
                    start_date + dt.timedelta(days=2),
                    start_date + dt.timedelta(days=3),
                    start_date + dt.timedelta(days=4),
                ],
                "open": [10, 10, 10, 10, 10],
                "high": [11, 10.5, 11.5, 11.7, 11.1],
                "low": [10, 10, 10, 10, 10],
                "close": [10, 10, 10, 10, 10],
                "volume": [100, 100, 100, 100, 100],
            }
        )
        db.execute_insert(
            data_df=ohlc_df,
            schema="RAW_FIN_DATA_YF",
            table_name="OHLC_DAILY",
            filename="aif/ai_analytics/dwh_finance/raw_fin_data_yf/resources/sql/dml/ohlc_daily_insert.sql",
            parameters={"asset_id": "Asset1#Test"},
        )

        # Refresh materialized view
        db.refresh_mat_view(view_name="core_fin_data_yf.ohlc_daily")

        # Create test queries
        new_highs_df = db.execute_query(sql_stmt="select * from core_fin_data_yf.ohlc_daily_new_high").result_df
        assert len(new_highs_df) == 3
