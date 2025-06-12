"""New high detection assets for the CORE_FIN_DATA_YF schema.

This module defines Dagster assets that create the OHLC_DAILY_NEW_HIGH materialized view
in the CORE_FIN_DATA_YF schema. The view filters the OHLC_DAILY data to include only
datapoints where a stock reaches a new high price, providing valuable insights for
technical analysis and trend identification.

The SQL statement used to create this view was generated using AI techniques, serving as
an example of how AI can be leveraged to create complex SQL queries based on natural
language descriptions. The corresponding prompt file contains details about how the
SQL was generated.
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
    name="OHLC_DAILY_NEW_HIGH",
    description="Only OHLC data of days, where a new high is hit (View)",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
    deps=[
        dg.SourceAsset(key=dg.AssetKey([DWH_NAME, SCHEMA_NAME, "OHLC_DAILY"])),
    ],
)
def asset_ohlc_daily_new_high(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Create the OHLC_DAILY_NEW_HIGH materialized view in the CORE_FIN_DATA_YF schema.

    This asset creates a materialized view that filters the OHLC_DAILY data to include only
    datapoints where a stock reaches a new high price. This specialized view provides
    valuable insights for technical analysis, trend identification, and trading strategy
    development by isolating significant price movements.

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
        filename=f"aif/ai_analytics/{DWH_NAME}/{SCHEMA_NAME}/resources/sql/ddl/ohlc_daily_new_high.sql",
        parameters={"COMMENT": description},
    )


if __name__ == "__main__":
    run_jobs_for_assets([asset_ohlc_daily_new_high])
