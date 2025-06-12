"""License module for the AIF framework.

This module provides functionality for retrieving and displaying the license information
for the AIF framework. It locates and reads the LICENSE file from the project directory
and makes its contents available to other components of the framework.

The license information is typically displayed during application startup to inform users
about the terms of use and distribution of the software.
"""

import os


def get_license_notice() -> str:
    """Returns the current license notice from the LICENSE file.

    This function attempts to locate and read the LICENSE file from the project directory.
    The project path is determined from the AIF_PATH environment variable.

    If the LICENSE file exists, its contents are returned.
    Otherwise, a message indicating that no license file was found is returned.

    Returns:
        str: The license notice as a string

    Note:
        If there is an error reading the LICENSE file, an error message is returned
        instead of raising an exception.
    """
    project_path = os.getenv("AIF_PATH")
    if project_path is None:
        license_path = "LICENSE"
    else:
        license_path = os.path.join(project_path, "LICENSE")

    if os.path.isfile(license_path):
        try:
            with open(license_path, "r", encoding="utf-8") as f:
                notice = f.read()
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise RuntimeError(f"ERROR READING LICENSE FILE: {str(e)}") from e
    else:
        raise RuntimeError("NO LICENSE FILE WAS FOUND")

    return notice
