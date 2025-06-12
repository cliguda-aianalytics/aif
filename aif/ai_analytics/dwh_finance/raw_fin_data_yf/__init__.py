"""RAW_FIN_DATA_YF schema definition for the DWH Finance database.

This module defines the RAW_FIN_DATA_YF schema, which serves as the initial data ingestion layer
for financial data from Yahoo Finance. It contains raw, unprocessed financial data including
stock prices, company information, and other market data directly from the source.

The schema provides the foundation for further data processing and transformation in the
CORE_FIN_DATA_YF layer. It includes assets for creating database tables and ETL processes
for loading data from external sources into the raw tables.

This module exports the schema definition as SCHEMA_DEFINITION, which is imported by the
main definitions module to create a unified Dagster Definitions object for the entire database.
"""

import dagster as dg

from aif.common.dagster.util import DagsterSchemaDefinitions

DWH_NAME = "DWH_FINANCE"  # Needs to match directory name and the name of the database configuration in the yaml file.
SCHEMA_NAME = "RAW_FIN_DATA_YF"

SCHEMA_DEFINITION = DagsterSchemaDefinitions(
    assets=iter(dg.load_assets_from_package_name(__name__)),
    schedules=None,
    sensors=None,
    jobs=None,
    resources=None,
    loggers=None,
    asset_checks=None,
)
