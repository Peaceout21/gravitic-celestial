import json
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional


def _build_json_formatter() -> logging.Formatter:
    try:
        from pythonjsonlogger import jsonlogger
    except ImportError:
        return logging.Formatter(
            json.dumps(
                {
                    "timestamp": "%(asctime)s",
                    "level": "%(levelname)s",
                    "name": "%(name)s",
                    "message": "%(message)s",
                }
            )
        )

    return jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )


def _build_plain_formatter() -> logging.Formatter:
    return logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s - %(message)s"
    )


def configure_logging(
    *,
    log_level: str = "INFO",
    log_format: str = "plain",
    log_file: Optional[str] = "logs/poller.log",
    console: bool = True,
) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = (
        _build_json_formatter() if log_format.lower() == "json" else _build_plain_formatter()
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()

    if log_file:
        log_dir = os.path.dirname(log_file) or "."
        os.makedirs(log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
