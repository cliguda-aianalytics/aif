"""DWH Use Cases package for various business use case data.

This package defines the structure and components of the DWH Use Cases database, which stores
and processes data for different business use cases. It includes schema definitions, assets,
and workflows for data ingestion, transformation, and analysis across multiple domains.

The database is organized into multiple schema layers:
- RAW_PLANNING_DATA: Contains raw planning and forecasting data
- RAW_CUSTOMER_SUPPORT: Contains raw customer support interaction data
- RAW_AI_CUSTOMER_SUPPORT: Contains raw AI-generated customer support data
- CORE_PLANNING: Contains normalized and business-ready planning data

Each schema layer is defined in its respective subpackage and combined in the definitions module
to create a unified Dagster Definitions object for the entire database.
"""
