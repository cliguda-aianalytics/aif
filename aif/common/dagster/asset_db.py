"""Database operation utilities for Dagster assets in AIF.

This module provides methods for assets that carry out standard database operations like creating a
schema, table, etc. It facilitates the integration of database operations within Dagster assets.
"""

from typing import Any

import dagster as dg

from aif.common.aif.src.data_interfaces.db_interface import DBInterface, DBResult
from aif.common.aif.src.initialization import initialize_aif
from aif.common.aif.src import aif_logging as logging


def asset_call_db_method(
    config_files: list[str], db_name: str, method_name: str, *args, **kwargs
) -> dg.MaterializeResult:
    """Execute a database method within a Dagster asset context.

    This function initializes the AIF environment with the provided configuration files,
    establishes a database connection using the specified database configuration,
    and executes the requested database method with the provided arguments.

    Parameters:
        config_files: List of configuration file paths to initialize the AIF environment.
        db_name: Name of the database configuration to use.
        method_name: Name of the database method to call.
        *args: Positional arguments to pass to the database method.
        **kwargs: Keyword arguments to pass to the database method.

    Returns:
        dg.MaterializeResult: A Dagster materialization result containing metadata about the operation.

    Raises:
        Exception: Any exception that might occur during database operations.

    Note:
        This function should only be called from a Dagster asset.
    """
    initialize_aif(config_files=config_files)

    with DBInterface(db_cfg=db_name) as db:
        db_res: DBResult = db.call_method(method_name=method_name, *args, **kwargs)

    metadata: dict[str, Any] = {
        "SQL": dg.MetadataValue.text(db_res.sql_stmt),
    }
    if db_res.result_df is not None:
        msg = (
            "DB method returned a dataframe which will not be stored. Only call methods to execute statements, "
            "not to query."
        )
        logging.get_aif_logger(__name__).warning(msg)

        metadata["WARNING"] = dg.MetadataValue.text(msg)
        metadata["Preview"] = dg.MetadataValue.md(db_res.result_df.head().to_markdown())

    return dg.MaterializeResult(
        metadata=metadata,
    )
