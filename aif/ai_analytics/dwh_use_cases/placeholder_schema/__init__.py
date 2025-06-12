"""Create a definition for this schema."""

import dagster as dg

from aif.common.dagster.util import DagsterSchemaDefinitions

DWH_NAME = "DWH_USE_CASES"  # Needs to match directory name and the name of the database configuration in the yaml file.
SCHEMA_NAME = "PLACEHOLDER_SCHEMA"

# Add all relevant assets here
SCHEMA_DEFINITION = DagsterSchemaDefinitions(
    assets=dg.load_assets_from_package_name(__name__),
    schedules=None,
    sensors=None,
    jobs=None,
    resources=None,
    loggers=None,
    asset_checks=dg.load_asset_checks_from_package_name(__name__),
)