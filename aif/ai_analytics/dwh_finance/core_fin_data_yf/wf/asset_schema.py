"""Schema creation assets for the CORE_FIN_DATA_YF schema.

This module defines Dagster assets responsible for creating the CORE_FIN_DATA_YF schema
and its database objects in the DWH Finance database. It contains the foundational asset
that establishes the schema structure, which is a prerequisite for all other assets
in this schema layer.

The schema serves as the business-ready layer for financial data from Yahoo Finance,
providing normalized and transformed financial information that can be directly used
for analysis, reporting, and other business applications.
"""

import dagster as dg

from aif.common.dagster.asset_db import asset_call_db_method
from aif.common.dagster.util import run_jobs_for_assets
from aif.ai_analytics.dwh_finance.core_fin_data_yf import DWH_NAME, SCHEMA_NAME

# The creating of a schema is always quite similar, but slightly different. Therefore, we ignore the pylint warning.
# pylint: disable=R0801

CONFIG_FILES = [
    "aif/common/aif/resources/config/base.yaml",
    "aif/ai_analytics/resources/config/{ENV}/dwh.yaml",
]


@dg.asset(
    key_prefix=[DWH_NAME, SCHEMA_NAME],
    name="SCHEMA",
    description="Financial data from Yahoo Finance",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
)
def asset_schema(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Create the CORE_FIN_DATA_YF schema in the DWH Finance database.

    This asset is responsible for creating the schema that will store business-ready financial data
    from Yahoo Finance. It serves as the foundation for all other assets in this schema layer
    and must be executed before any tables can be created or data can be loaded.

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
        method_name="create_schema",
        schema_name=SCHEMA_NAME,
        schema_comment=description,
    )


if __name__ == "__main__":
    run_jobs_for_assets([asset_schema])
