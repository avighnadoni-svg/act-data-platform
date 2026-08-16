# src/common/logging_config.py

import logging
import os
import sys


# ============================================================
# LOG LEVEL
# ============================================================

DEFAULT_LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO"
).upper()


# ============================================================
# LOG FORMAT
# ============================================================

LOG_FORMAT = (
    "%(asctime)s | "
    "%(levelname)s | "
    "%(name)s | "
    "%(message)s"
)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# ============================================================
# CONFIGURE ROOT LOGGING
# ============================================================

def configure_logging(
    log_level: str | None = None,
):
    """
    Configure application logging.

    Airflow will capture stdout/stderr logs,
    therefore StreamHandler is sufficient.
    """

    level_name = (
        log_level
        or DEFAULT_LOG_LEVEL
    ).upper()

    level = getattr(
        logging,
        level_name,
        logging.INFO,
    )

    root_logger = logging.getLogger()

    root_logger.setLevel(level)

    # Avoid duplicate handlers when modules
    # are imported multiple times by Airflow.
    if not root_logger.handlers:

        handler = logging.StreamHandler(
            sys.stdout
        )

        formatter = logging.Formatter(
            fmt=LOG_FORMAT,
            datefmt=DATE_FORMAT,
        )

        handler.setFormatter(formatter)

        root_logger.addHandler(handler)


# ============================================================
# GET LOGGER
# ============================================================

def get_logger(
    name: str,
) -> logging.Logger:
    """
    Return configured logger for a module.
    """

    configure_logging()

    return logging.getLogger(name)