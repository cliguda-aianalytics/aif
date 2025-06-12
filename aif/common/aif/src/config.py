"""Configuration module for the AIF framework.

This module manages configuration settings loaded from YAML files. It provides functionality to:
1. Initialize settings from multiple configuration files
2. Support environment-specific configurations
3. Substitute environment variables in configuration values
4. Maintain thread-safe access to settings

All settings are available in the global dictionary 'settings' after initialization.
"""

import json
import os
import re
from typing import Any
import tomllib

import threading
import yaml

from aif.common.aif.src.utils.secret_utils import create_save_dict

BASE_ENV_VARIABLE = "AIF_ENV"  # The environment variable, that stats the current environment 'dev, 'prod', ...
BASE_PATH = "AIF_PATH"

# Call init_settings to update all settings.
settings: dict[str, Any] = {}

# settings is a global variable and the initialization must be thread save
lock = threading.Lock()


def init_settings(config_files: list[str]) -> str:
    """Initialize settings based on the provided configuration files.

    This method should be called from the main workflow at the beginning of execution.
    It loads configuration from YAML files, substitutes environment variables, and
    ensures thread-safety when multiple threads attempt initialization.

    Args:
        config_files: List of paths to YAML configuration files relative to the project root

    Returns:
        str: The complete configuration as a formatted JSON string (for logging purposes)

    Raises:
        RuntimeError: If required environment variables are not set or if duplicate settings are found
    """

    # Since settings is not reassigned, pylint reports a global-variable-not-assigned error.
    global settings  # pylint: disable=global-variable-not-assigned
    with lock:
        if len(settings) == 0:
            # Adding project path and current environment to settings
            env = os.getenv(BASE_ENV_VARIABLE)
            assert env is not None  # Was already checked, so we use assert to let MyPy be chilled here.

            path = os.getenv(BASE_PATH)

            if path is None:
                raise RuntimeError(f"No environment variable {BASE_PATH} is provided")

            settings["environment"] = env
            settings["path"] = path

            # Adding version to settings
            with open(path + "pyproject.toml", "rb") as f_prj:
                pyproject_config: dict = tomllib.load(f_prj)
                settings["version"] = pyproject_config["project"]["version"]

            # Matching configuration files to dev/prod environment
            configs = [c.replace("{ENV}", env) for c in config_files]

            # Setting up yaml to replace environment variables in configuration files
            path_matcher = re.compile(r"\$\{([^}^{]+)\}")

            def path_constructor(loader, node) -> str:  # pylint: disable=unused-argument
                """Extract the matched value, expand env variable, and replace the match"""
                value = node.value

                if (match := path_matcher.match(value)) is None:  # () is needed for mypy check
                    raise ValueError("f() returned None")

                env_var = match.group()[2:-1]
                return os.environ.get(env_var) + value[match.end() :]

            yaml.add_implicit_resolver("!path", path_matcher, None, yaml.SafeLoader)
            yaml.add_constructor("!path", path_constructor, yaml.SafeLoader)

            for c in configs:
                with open(path + c, mode="r", encoding="utf-8") as f_yaml:
                    s = yaml.load(f_yaml, Loader=yaml.SafeLoader)
                    for k, v in s.items():
                        if k in settings:
                            print(f"Settings for {k} overwritten by {c}.")
                            raise RuntimeError(f"Duplicate settings {k} in config.")
                        settings[k] = v

    save_settings = create_save_dict(settings, secret_keys=["key", "password", "token", "secret"])
    settings_formatted = json.dumps(save_settings, indent=4)

    return settings_formatted
