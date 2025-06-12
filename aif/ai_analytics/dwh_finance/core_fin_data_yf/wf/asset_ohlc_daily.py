"""OHLC daily view assets for the CORE_FIN_DATA_YF schema.

This module defines Dagster assets that create and refresh the OHLC_DAILY materialized view
in the CORE_FIN_DATA_YF schema. The view transforms raw OHLC data from the RAW_FIN_DATA_YF schema
into a business-ready format, providing normalized and enriched daily stock price information
for analysis and reporting.

The materialized view needs to be refreshed whenever new raw data becomes available to ensure
that the business-ready layer contains the most up-to-date financial information.
"""

import dagster as dg

from aif.common.dagster.asset_db import asset_call_db_method
from aif.common.dagster.util import run_jobs_for_assets
from aif.ai_analytics.dwh_finance.core_fin_data_yf import DWH_NAME, SCHEMA_NAME


CONFIG_FILES = [
    "aif/common/aif/resources/config/base.yaml",
    "aif/ai_analytics/resources/config/{ENV}/dwh.yaml",
]


@dg.asset(
    key_prefix=[DWH_NAME, SCHEMA_NAME],
    name="OHLC_DAILY",
    description="Daily OHLC Data (View)",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
    deps=[
        dg.SourceAsset(key=dg.AssetKey([DWH_NAME, SCHEMA_NAME, "SCHEMA"])),
        dg.SourceAsset(key=dg.AssetKey([DWH_NAME, "RAW_FIN_DATA_YF", "OHLC_DAILY"])),
    ],
)
def asset_ohlc_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Create the OHLC_DAILY materialized view in the CORE_FIN_DATA_YF schema.

    This asset creates a materialized view that transforms raw daily stock price data (OHLC)
    from the RAW_FIN_DATA_YF schema into a business-ready format. The view applies normalization,
    data type conversions, and other transformations to make the data suitable for direct use
    in business applications and reporting.

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
    name="OHLC_DAILY_REFRESH",
    description="Refresh core OHLC view",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
    deps=[
        asset_ohlc_daily,
        dg.SourceAsset(key=dg.AssetKey([DWH_NAME, "RAW_FIN_DATA_YF", "OHLC_DAILY_ETL"])),
    ],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def asset_ohlc_daily_refresh() -> dg.MaterializeResult:
    """Refresh the OHLC_DAILY materialized view with the latest data.

    This asset refreshes the OHLC_DAILY materialized view to incorporate any new data
    that has been loaded into the RAW_FIN_DATA_YF schema. The refresh ensures that
    the business-ready layer contains the most up-to-date financial information for
    analysis and reporting. This asset is configured to run automatically whenever
    its dependencies are updated.

    Returns:
        dg.MaterializeResult: A Dagster materialization result containing metadata about the operation.

    Raises:
        Exception: Any exception that might occur during the view refresh operation.
    """
    return asset_call_db_method(
        config_files=CONFIG_FILES,
        db_name=DWH_NAME,
        method_name="refresh_mat_view",
        view_name=f"{SCHEMA_NAME}.ohlc_daily",
    )


if __name__ == "__main__":
    run_jobs_for_assets([asset_ohlc_daily, asset_ohlc_daily_refresh])
