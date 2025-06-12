"""DWH Finance package for financial data analysis.

This package defines the structure and components of the DWH Finance database, which stores
and processes financial data from various sources. It includes schema definitions, assets,
and workflows for data ingestion, transformation, and analysis of financial information.

The database is organized into multiple schema layers:
- RAW_FIN_DATA_YF: Contains raw financial data from Yahoo Finance
- CORE_FIN_DATA_YF: Contains normalized and business-ready financial data

Each schema layer is defined in its respective subpackage and combined in the definitions module
to create a unified Dagster Definitions object for the entire database.
"""
