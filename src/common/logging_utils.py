"""Logging utilities for CitiBike-StationPulse.

Configures standard logging with UTC ISO-8601 timestamps and safe sanitization
of sensitive credentials and headers.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Optional


class UTCFormatter(logging.Formatter):
    """Formatter that outputs timestamps strictly in UTC ISO-8601 format."""

    converter = time.gmtime

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        ct = self.converter(record.created)
        if datefmt:
            return time.strftime(datefmt, ct)
        # Default ISO-8601 with Z suffix: YYYY-MM-DDTHH:MM:SSZ
        t_str = time.strftime("%Y-%m-%dT%H:%M:%S", ct)
        return f"{t_str}.{int(record.msecs):03d}Z"


def sanitize_message(msg: str) -> str:
    """Mask sensitive patterns such as tokens, auth headers, and secrets."""
    sensitive_keywords = ["authorization", "bearer ", "token", "password", "secret", "apikey", "api_key"]
    lower = msg.lower()
    for kw in sensitive_keywords:
        if kw in lower:
            # Basic precaution against accidental secret leaking
            return "[SANITIZED LOG MESSAGE: Potential sensitive credentials omitted]"
    return msg


class SafeLogFilter(logging.Filter):
    """Filter that strips out sensitive credentials before emission."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = sanitize_message(record.msg)
        return True


def setup_logger(
    name: str = "stationpulse",
    log_file: Optional[Path | str] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Configure and return a structured logger.

    Args:
        name: Logger name identifier.
        log_file: Optional file path to persist log output.
        level: Logging level (default: logging.INFO).

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers if setup_logger is called repeatedly
    if logger.handlers:
        return logger

    fmt = "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s"
    formatter = UTCFormatter(fmt)
    safe_filter = SafeLogFilter()

    # Console handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(safe_filter)
    logger.addHandler(console_handler)

    # File handler (if requested)
    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.addFilter(safe_filter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger
