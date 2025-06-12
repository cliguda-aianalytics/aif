"""OHLC daily table assets for the RAW_FIN_DATA_YF schema.

This module defines Dagster assets that create and populate the OHLC_DAILY table in the
RAW_FIN_DATA_YF schema. It includes both the table creation asset and the ETL asset that loads
data from Yahoo Finance into the table, serving as the initial data ingestion layer for
daily stock price information (Open, High, Low, Close).

The OHLC data is fetched on a daily schedule and stored in its raw form before any
transformation or normalization occurs in subsequent schema layers.
"""

import dagster as dg

from aif.common.aif.src.initialization import initialize_aif
from aif.common.dagster.asset_db import asset_call_db_method
from aif.common.dagster.util import run_jobs_for_assets
from aif.ai_analytics.dwh_finance.raw_fin_data_yf import DWH_NAME, SCHEMA_NAME
from aif.ai_analytics.dwh_finance.raw_fin_data_yf.src.ohlc_daily_etl import OhlcETL


CONFIG_FILES = [
    "aif/common/aif/resources/config/base.yaml",
    "aif/ai_analytics/resources/config/{ENV}/dwh.yaml",
]


@dg.asset(
    key_prefix=[DWH_NAME, SCHEMA_NAME],
    name="OHLC_DAILY",
    description="Daily OHLC data",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
    deps=[dg.SourceAsset(key=dg.AssetKey([DWH_NAME, SCHEMA_NAME, "SCHEMA"]))],
)
def asset_ohlc_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Create the OHLC_DAILY table in the RAW_FIN_DATA_YF schema.

    This asset creates a table that will store raw daily stock price data (Open, High, Low, Close)
    from Yahoo Finance, forming the initial data storage layer before any transformation or
    normalization occurs.

    Parameters:
        context: The Dagster asset execution context, which contains metadata about the asset.

    Returns:
        dg.MaterializeResult: A Dagster materialization result containing metadata about the operation.

    Raises:
        Exception: Any exception that might occur during database operations.
    """
    description = context.assets_def.descriptions_by_key[context.asset_key]
    return asset_call_db_method(
        config_files=CONFIG_FILES,
        db_name=DWH_NAME,
        method_name="execute_statement_from_file",
        filename=f"aif/ai_analytics/{DWH_NAME}/{SCHEMA_NAME}/resources/sql/ddl/ohlc_daily.sql",
        parameters={"COMMENT": description},
    )


@dg.asset(
    key_prefix=[DWH_NAME, SCHEMA_NAME],
    name="OHLC_DAILY_ETL",
    description="ETL pipeline to load new OHLC data from Yahoo Finance",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
    deps=[dg.SourceAsset(key=dg.AssetKey([DWH_NAME, SCHEMA_NAME, "OHLC_DAILY"]))],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager().with_rules(
        dg.AutoMaterializeRule.materialize_on_cron("0 1 * * *", timezone="UTC"),
    ),
)
def asset_ohlc_daily_etl() -> dg.MaterializeResult:
    """Execute the ETL pipeline to load OHLC data from Yahoo Finance into the OHLC_DAILY table.

    This asset initializes the configuration, extracts daily stock price data from Yahoo Finance,
    and loads it into the OHLC_DAILY table in the RAW_FIN_DATA_YF schema. The asset is configured
    to run automatically at 1:00 AM UTC every day to fetch the latest price data.

    Returns:
        dg.MaterializeResult: A Dagster materialization result containing metadata about the operation.

    Raises:
        Exception: Any exception that might occur during the ETL process, such as API access issues,
                  data transformation errors, or database operations failures.
    """
    initialize_aif(config_files=CONFIG_FILES)

    etl = OhlcETL(fail_on_missing_entries=False, asset_symbol="AAPL")

    return etl.run()


if __name__ == "__main__":
    run_jobs_for_assets([asset_ohlc_daily, asset_ohlc_daily_etl])
