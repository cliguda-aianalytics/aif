"""Logging module for the AIF framework.

This module provides a comprehensive logging system for the AIF framework, including:
1. Colored console output for different log levels
2. File-based logging with rotation
3. Separate loggers for the AIF framework and root logger
4. Configuration based on settings from the config module

The module sets up both file handlers and console handlers with appropriate formatting.
"""

import logging
import sys
from logging.handlers import TimedRotatingFileHandler

from aif.common.aif.src.config import settings

FORMAT_MSG = "%(asctime)s | %(levelname)-8s | %(name)-50s | %(message)s"
FORMAT_DATE = "%Y-%m-%d %H:%M:%S"


LOG_COLOR_GREEN = "\x1b[32m"
LOG_COLOR_GREY = "\x1b[38;20m"
LOG_COLOR_YELLOW = "\x1b[33;20m"
LOG_COLOR_RED = "\x1b[31;20m"
LOG_COLOR_BOLD_RED = "\x1b[31;1m"
LOG_COLOR_RESET = "\x1b[0m"


class AifColorFormatter(logging.Formatter):
    """Custom formatter that applies different colors to log messages based on their level.

    This formatter enhances console output readability by color-coding log messages
    according to their severity level:
    - DEBUG: Green
    - INFO: Grey
    - WARNING: Yellow
    - ERROR: Red
    - CRITICAL: Bold Red
    """

    def __init__(self):
        super().__init__(fmt=FORMAT_MSG, datefmt=FORMAT_DATE)  # Only used as fallback, when no color formatter is found

        self.formatter: dict[int, logging.Formatter] = {
            logging.DEBUG: logging.Formatter(fmt=LOG_COLOR_GREEN + FORMAT_MSG + LOG_COLOR_RESET, datefmt=FORMAT_DATE),
            logging.INFO: logging.Formatter(fmt=LOG_COLOR_GREY + FORMAT_MSG + LOG_COLOR_RESET, datefmt=FORMAT_DATE),
            logging.WARNING: logging.Formatter(
                fmt=LOG_COLOR_YELLOW + FORMAT_MSG + LOG_COLOR_RESET, datefmt=FORMAT_DATE
            ),
            logging.ERROR: logging.Formatter(fmt=LOG_COLOR_RED + FORMAT_MSG + LOG_COLOR_RESET, datefmt=FORMAT_DATE),
            logging.CRITICAL: logging.Formatter(
                fmt=LOG_COLOR_BOLD_RED + FORMAT_MSG + LOG_COLOR_RESET, datefmt=FORMAT_DATE
            ),
        }

    def format(self, record):
        formatter = self.formatter.get(record.levelno)
        if formatter is not None:
            return formatter.format(record)

        return super().format(record)


def init_logging():
    """Set up the logging system for the AIF framework.

    This function configures both the root logger and the AIF-specific logger with:
    1. File handlers with daily rotation
    2. Console output with color formatting
    3. Appropriate log levels based on configuration settings

    The function is idempotent and can be called multiple times without creating duplicate handlers.
    """

    # Setup root logger
    log_root_filename = f"""{settings["path"]}{settings["logging"]["log_root_filename"]}"""
    log_root_file_level_name = settings["logging"]["log_root_file_level"].upper()
    log_root_file_level = logging._nameToLevel[log_root_file_level_name]  # pylint: disable=protected-access

    logging.basicConfig(
        format=FORMAT_MSG,
        datefmt=FORMAT_DATE,
        level=log_root_file_level,
        handlers=[
            TimedRotatingFileHandler(
                filename=log_root_filename, when="midnight", utc=True, backupCount=30, encoding="utf-8"
            )
        ],
    )

    # Setup aif logging
    aif_logger = logging.getLogger("aif")

    if len(aif_logger.handlers) > 0:  # Check if already initialized
        return

    aif_logger.propagate = False
    aif_logger.setLevel(logging.DEBUG)

    # File handler for aif logging
    log_aif_filename = f"""{settings["path"]}{settings["logging"]["log_aif_filename"]}"""
    log_aif_file_level_name = settings["logging"]["log_aif_file_level"].upper()
    log_aif_file_level = logging._nameToLevel[log_aif_file_level_name]  # pylint: disable=protected-access

    aif_logger_file_handler = TimedRotatingFileHandler(
        filename=log_aif_filename, when="midnight", utc=True, backupCount=30, encoding="utf-8"
    )
    aif_logger_file_handler.setLevel(log_aif_file_level)
    aif_logger_file_handler.setFormatter(logging.Formatter(fmt=FORMAT_MSG, datefmt=FORMAT_DATE))

    aif_logger.addHandler(aif_logger_file_handler)

    # Console handler for aif logging
    log_aif_console_level_name = settings["logging"]["log_aif_console_level"].upper()
    log_aif_console_level = logging._nameToLevel[log_aif_console_level_name]  # pylint: disable=protected-access

    aif_logger_console_handler = logging.StreamHandler(stream=sys.stdout)
    aif_logger_console_handler.setLevel(log_aif_console_level)

    aif_logger_console_handler.setFormatter(AifColorFormatter())

    aif_logger.addHandler(aif_logger_console_handler)


def get_aif_logger(name: str):
    """Get a logger for AIF-related components with the appropriate configuration.

    This function ensures that all loggers used within the AIF framework have consistent
    configuration and naming conventions. If the provided name doesn't start with 'aif.',
    it will be prefixed with 'aif.default_logger.' to maintain the hierarchy.

    Args:
        name: The name for the logger, typically __name__ from the calling module

    Returns:
        logging.Logger: A configured logger instance with the appropriate handlers

    Note:
        In workflows and different execution environments, it can happen that the __name__
        in a file is just "__main__" and not the original name of the file. Therefore,
        this function ensures proper configuration regardless of the execution context.
    """
    if not name.startswith("aif."):
        name = f"aif.default_logger.{name}"

    logger = logging.getLogger(name)
    logger.propagate = True

    return logger
