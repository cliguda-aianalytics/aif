"""Module with a Dagster asset to create the schema PLACEHOLDER_SCHEMA in the database DWH_USE_CASES."""

import dagster as dg

from aif.common.dagster.asset_db import asset_call_db_method
from aif.common.dagster.util import run_jobs_for_assets
from aif.ai_analytics.dwh_use_cases.placeholder_schema import DWH_NAME, SCHEMA_NAME

# The creating of a schema is always quite similar, but slightly different. Therefore, we ignore the pylint warning.
# pylint: disable=R0801

CONFIG_FILES = [
    "aif/common/aif/resources/config/base.yaml",
    "aif/ai_analytics/resources/config/{ENV}/dwh.yaml",
]


@dg.asset(
    key_prefix=[DWH_NAME, SCHEMA_NAME],
    name="SCHEMA",
    description="Placeholder schema for DWH use cases",
    group_name=f"{DWH_NAME}_{SCHEMA_NAME}",
)
def asset_schema(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Asset to create the schema."""
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