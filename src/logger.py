"""Structured logging for the Sunshine-Gated Closed-Case Pipeline."""

import logging
import os
from datetime import datetime


def setup_logger(pipeline_root: str = "./CrimeDoc-Pipeline") -> logging.Logger:
    """Configure and return the pipeline logger.

    Logs to both console and a date-stamped file under {pipeline_root}/logs/.
    """
    log_dir = os.path.join(pipeline_root, "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"pipeline_{datetime.utcnow().strftime('%Y%m%d')}.log")

    logger = logging.getLogger("crime_pipeline")
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler — DEBUG and above
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler — INFO and above
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def get_logger() -> logging.Logger:
    """Get the pipeline logger (must call setup_logger first)."""
    return logging.getLogger("crime_pipeline")
