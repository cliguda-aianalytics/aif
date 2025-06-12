"""Workflow assets for the RAW_FIN_DATA_YF schema.

This package contains Dagster assets that define the workflow for creating and populating
tables in the RAW_FIN_DATA_YF schema. These assets handle the creation of database tables
and the ETL processes for loading financial data from Yahoo Finance into the raw tables.

The workflow assets in this package include:
- Schema creation assets for setting up the database schema
- OHLC (Open, High, Low, Close) daily price data assets for ingesting stock price information
- Other assets related to raw financial data ingestion

These assets form the foundation of the data pipeline, providing the initial data layer
that will be transformed and processed by subsequent layers in the DWH Finance database.
"""
