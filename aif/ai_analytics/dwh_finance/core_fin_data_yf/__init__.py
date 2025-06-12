"""CORE_FIN_DATA_YF schema definition for the DWH Finance database.

This module defines the CORE_FIN_DATA_YF schema, which serves as the business-ready layer
for financial data from Yahoo Finance. It contains normalized and transformed financial data
including stock prices, company information, and other market data that has been processed
from the raw data layer.

The schema provides business-ready financial data that can be directly used for analysis,
reporting, and other business applications. It includes assets for creating database tables
and ETL processes for transforming data from the RAW_FIN_DATA_YF schema into normalized formats.

This module exports the schema definition as SCHEMA_DEFINITION, which is imported by the
main definitions module to create a unified Dagster Definitions object for the entire database.
"""

import dagster as dg

from aif.common.dagster.util import DagsterSchemaDefinitions

DWH_NAME = "DWH_FINANCE"  # Needs to match directory name and the name of the database configuration in the yaml file.
SCHEMA_NAME = "CORE_FIN_DATA_YF"

SCHEMA_DEFINITION = DagsterSchemaDefinitions(
    assets=iter(dg.load_assets_from_package_name(__name__)),
    schedules=None,
    sensors=None,
    jobs=None,
    resources=None,
    loggers=None,
    asset_checks=None,
)
