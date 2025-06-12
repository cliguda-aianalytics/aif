"""Utilities for handling sensitive information in the AIF framework.

This module provides functions to safely handle and display sensitive information
such as passwords, API keys, and other secrets. It helps prevent accidental exposure
of sensitive data in logs, console output, or error messages.
"""


def create_save_dict(d: dict, secret_keys: list[str], secret_value: str = "--SECRET--") -> dict:
    """Create a copy of a dictionary with sensitive values masked.

    This function recursively processes a dictionary and replaces values for keys
    that contain sensitive information (as defined by the secret_keys list) with
    a placeholder value. This is useful for logging configuration or displaying
    settings without exposing sensitive information.

    Args:
        d (dict): The input dictionary containing potential secrets
        secret_keys (list[str]): List of substrings to identify secret keys
                                (e.g., ["password", "key", "token"])
        secret_value (str, optional): The placeholder value to use for secrets.
                                     Defaults to "--SECRET--".

    Returns:
        dict: A new dictionary with the same structure but with sensitive values masked
    """
    new_dict: dict = {}

    for key in d.keys():
        value = d[key]
        if isinstance(value, dict):
            new_dict[key] = create_save_dict(d=value, secret_keys=secret_keys)
        else:
            if any(sp in key.lower() for sp in secret_keys):
                new_dict[key] = secret_value
            else:
                new_dict[key] = value

    return new_dict
