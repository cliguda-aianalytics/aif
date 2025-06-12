"""Workflow assets for the CORE_FIN_DATA_YF schema.

This package contains Dagster assets that define the workflow for creating and populating
tables in the CORE_FIN_DATA_YF schema. These assets handle the creation of database tables
and the ETL processes for transforming raw financial data into business-ready formats.

The workflow assets in this package include:
- Schema creation assets for setting up the database schema
- OHLC (Open, High, Low, Close) daily price data transformation assets
- New high detection assets for identifying stocks reaching new price highs
- Price prediction assets for forecasting future stock prices
- Other assets related to financial data transformation and analysis

These assets form the business-ready data layer that transforms raw financial data into
normalized, analyzed formats suitable for direct use in business applications and reporting.
"""
