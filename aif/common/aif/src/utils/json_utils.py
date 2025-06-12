"""Utility functions for working with JSON data in the AIF framework.

This module provides helper functions for transforming and processing JSON data,
particularly when working with API responses or nested data structures that
need to be flattened or transformed for analysis.
"""

import pandas as pd

from aif.common.aif.src import aif_logging


def transform_json_dataframe(df, group_key_col, json_col):
    """Transform a DataFrame with a JSON column into a flattened key-value structure.

    This function takes a DataFrame containing a column with JSON/dictionary data and
    converts it into a long-format DataFrame with one row per key-value pair from the
    original JSON. This is particularly useful for processing API responses like those
    from OpenAI.

    Args:
        df (pd.DataFrame): Input DataFrame with group key and JSON columns
        group_key_col (str): Name of the column containing group identifiers
        json_col (str): Name of the column containing JSON/dictionary data

    Returns:
        pd.DataFrame: Transformed DataFrame with columns: Group_Key, Key, Value

    Raises:
        Warning: If any JSON entry is not a dictionary, a warning is logged
    """
    rows = []

    for _, row in df.iterrows():
        group_key = row[group_key_col]
        json_data = row[json_col]

        if isinstance(json_data, dict):
            for key, value in json_data.items():
                rows.append({group_key_col: group_key, "Key": key, "Value": value})
        else:
            aif_logging.get_aif_logger(__name__).warning(
                "JSON entry in column %s is not a dictionary: %s", json_col, json_data
            )

    return pd.DataFrame(rows)
