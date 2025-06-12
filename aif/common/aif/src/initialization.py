"""Initialization module for the AIF framework.

This module should be used in any workflow or script. It provides basic initialization of the framework by:
1. Loading environment variables
2. Displaying license information
3. Initializing configuration settings
4. Setting up logging

It ensures that all necessary components are properly configured before the application starts.
"""

import os

from dotenv import load_dotenv

import aif.common.aif.src.config as conf
from aif.common.aif.src import aif_logging as logging
from aif.common.aif.src.license import get_license_notice


def initialize_aif(config_files: list[str], show_licence_notice: bool = True, show_settings: bool = True):
    """Initialize the AIF framework with the provided configuration files.

    This function performs the following steps:
    1. Loads environment variables from .env files
    2. Displays the license notice (if enabled)
    3. Validates the environment configuration
    4. Initializes settings from the provided config files
    5. Sets up logging

    Args:
        config_files: List of configuration YAML files to load
        show_licence_notice: Whether to display the license notice
        show_settings: Whether to log the loaded settings

    Returns:
        None

    Raises:
        RuntimeError: If required environment variables are not set
    """
    load_dotenv()

    if show_licence_notice:
        print(get_license_notice(), flush=True)

    env = os.getenv(conf.BASE_ENV_VARIABLE)
    if env is None:
        raise RuntimeError(f"No environment variable {conf.BASE_ENV_VARIABLE} is provided")

    # We check, if the method is called from a PyTest and change the environment by appending "_test", if
    # "test" is not part of the current environment (Thereby the env "dev" is mapped to "dev_test", while the
    # environment "test" that is used in the CICD pipeline remains the same)
    # This allows us to use different configurations (e.g. DB connections) when running a normal script in
    # vs running a PyTest, which is extremely important for local development and testing.
    if "PYTEST_CURRENT_TEST" in os.environ and "test" not in env:
        env = env + "_test"

    os.environ[conf.BASE_ENV_VARIABLE] = env

    configs_str = conf.init_settings(config_files=config_files)
    logging.init_logging()

    if show_settings:
        logging.get_aif_logger(__name__).info("Loaded settings for environment %s:\n%s", env, configs_str)
