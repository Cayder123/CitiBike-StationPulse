"""Logging utilities for CitiBike-StationPulse.

Configures standard logging with UTC ISO-8601 timestamps and safe sanitization
of sensitive credentials, tokens, request headers, and URL query secrets.
"""

from __future__ import annotations

import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse


class UTCFormatter(logging.Formatter):
    """Formatter that outputs timestamps strictly in UTC ISO-8601 format."""

    converter = time.gmtime

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        ct = self.converter(record.created)
        if datefmt:
            return time.strftime(datefmt, ct)
        # Default ISO-8601 with Z suffix: YYYY-MM-DDTHH:MM:SS.mmmZ
        t_str = time.strftime("%Y-%m-%dT%H:%M:%S", ct)
        return f"{t_str}.{int(record.msecs):03d}Z"


_SENSITIVE_PARAM_NAMES = {
    "token", "access_token", "apikey", "api_key", "secret", "password",
    "signature", "sig", "key", "auth", "authorization", "credential",
    "x-amz-signature", "x-amz-credential", "x-amz-security-token",
}

_URL_PATTERN = re.compile(r"https?://[^\s<>\"'{}|\\^`]+", re.IGNORECASE)
_AUTH_HEADER_PATTERN = re.compile(
    r"(?i)\b(authorization)\s*:\s*(?:bearer\s+)?([^\s,;]+)", re.IGNORECASE
)
_SECRET_ASSIGN_PATTERN = re.compile(
    r"(?i)\b(bearer|token|access_token|apikey|api_key|password|secret|credential)\s*[:=]\s*([^\s,;]+)",
    re.IGNORECASE,
)


def sanitize_url(url: str) -> str:
    """Sanitize URL by masking embedded userinfo (user:pass) and sensitive query parameters."""
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc
        if "@" in netloc:
            # Mask userinfo
            userinfo, host = netloc.split("@", 1)
            netloc = f"***:***@{host}"

        if parsed.query:
            query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
            sanitized_pairs = []
            for k, v in query_pairs:
                if k.lower() in _SENSITIVE_PARAM_NAMES:
                    sanitized_pairs.append((k, "[REDACTED]"))
                else:
                    sanitized_pairs.append((k, v))
            query = urlencode(sanitized_pairs)
        else:
            query = ""

        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, query, parsed.fragment))
    except Exception:
        return "[REDACTED_URL]"


def sanitize_message(msg: str) -> str:
    """Mask sensitive patterns such as tokens, auth headers, and secrets while preserving context."""
    if not isinstance(msg, str) or not msg:
        return msg

    # 1. Sanitize full URLs in message
    def _replace_url(match: re.Match) -> str:
        return sanitize_url(match.group(0))

    sanitized = _URL_PATTERN.sub(_replace_url, msg)

    # 2. Mask Authorization headers (including Bearer <token>)
    sanitized = _AUTH_HEADER_PATTERN.sub(r"\1: [REDACTED]", sanitized)

    # 3. Mask explicit key-value secrets
    sanitized = _SECRET_ASSIGN_PATTERN.sub(r"\1: [REDACTED]", sanitized)

    return sanitized


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

    Guarantees:
    - UTC ISO-8601 timestamps.
    - Sanitization of credentials, secrets, and sensitive URL query params.
    - Safe handler reuse: adding log_file on subsequent calls does not drop the file handler.
    - Avoids duplicate handlers of the same target.

    Args:
        name: Logger name identifier.
        log_file: Optional file path to persist log output.
        level: Logging level (default: logging.INFO).

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    fmt = "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s"
    formatter = UTCFormatter(fmt)
    safe_filter = SafeLogFilter()

    # Ensure at least one console handler exists
    has_console = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in logger.handlers
    )
    if not has_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.addFilter(safe_filter)
        logger.addHandler(console_handler)

    # If log_file requested, check if already added for this specific path
    if log_file:
        resolved_file = Path(log_file).resolve()
        has_file_handler = False
        for h in logger.handlers:
            if isinstance(h, logging.FileHandler):
                try:
                    if Path(h.baseFilename).resolve() == resolved_file:
                        has_file_handler = True
                        break
                except Exception:
                    pass

        if not has_file_handler:
            resolved_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(resolved_file, encoding="utf-8")
            file_handler.setFormatter(formatter)
            file_handler.addFilter(safe_filter)
            logger.addHandler(file_handler)

    logger.propagate = False
    return logger
