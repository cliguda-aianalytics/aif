"""Utility functions for the AIF framework."""


def safe_merge_dicts(dict1, dict2):
    """Safely merge two dictionaries.

    This function merges two dictionaries, raising an exception if there are
    conflicting keys (keys that exist in both dictionaries with different values).

    Args:
        dict1 (dict): The first dictionary
        dict2 (dict): The second dictionary

    Returns:
        dict: A new dictionary containing all keys from both input dictionaries

    Raises:
        RuntimeError: If there are conflicting keys in the dictionaries
    """
    if dict1 is None:
        return dict2.copy() if dict2 is not None else {}

    if dict2 is None:
        return dict1.copy()

    result = dict1.copy()

    for key, value in dict2.items():
        if key in result:
            # Check if the values are the same
            if result[key] != value:
                raise RuntimeError(f"Cannot merge dictionaries: Conflicting values for key '{key}'")
        else:
            result[key] = value

    return result
