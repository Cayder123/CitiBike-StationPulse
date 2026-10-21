"""Unit tests for security, path resolution, logging sanitization, and verification features."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from src.common.logging_utils import sanitize_url, sanitize_message, setup_logger
from src.common.config import PipelineConfig
from src.raw_verify import is_safe_zip_path, resolve_artifact_path
from src.ingest_gbfs import create_unique_gbfs_dir, fetch_and_save_json, MAX_FEED_BYTES


def test_sanitize_url_redacts_credentials_and_query_secrets():
    # Mask userinfo
    url_with_auth = "https://user:mypassword@example.com/feed.json"
    sanitized = sanitize_url(url_with_auth)
    assert "user:mypassword" not in sanitized
    assert "***:***@example.com" in sanitized

    # Mask sensitive query params
    url_with_tokens = "https://api.example.com/v1/data?token=secret123&public_param=ok&api_key=456"
    sanitized_query = sanitize_url(url_with_tokens)
    assert "secret123" not in sanitized_query
    assert "456" not in sanitized_query
    assert "token=%5BREDACTED%5D" in sanitized_query or "token=[REDACTED]" in sanitized_query
    assert "public_param=ok" in sanitized_query


def test_sanitize_message_redacts_headers_and_urls():
    msg = "Request sent to https://user:secret@example.com/api?token=abc with Authorization: Bearer xyz123"
    sanitized = sanitize_message(msg)
    assert "secret@" not in sanitized
    assert "xyz123" not in sanitized


def test_setup_logger_adds_file_handler_on_subsequent_call(tmp_path: Path):
    log_file_1 = tmp_path / "test_1.log"
    log_file_2 = tmp_path / "test_2.log"

    # Call 1: console only
    logger = setup_logger("test_multi_handler", log_file=None)
    assert len(logger.handlers) == 1

    # Call 2: with file 1
    logger = setup_logger("test_multi_handler", log_file=log_file_1)
    file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) == 1

    # Call 3: with file 2
    logger = setup_logger("test_multi_handler", log_file=log_file_2)
    file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) == 2


def test_zip_slip_prevention(tmp_path: Path):
    target_dir = tmp_path / "safe_extract"
    target_dir.mkdir()

    # Unsafe relative traversal
    assert not is_safe_zip_path(target_dir, "../../evil.csv")
    assert not is_safe_zip_path(target_dir, "..\\..\\evil.csv")
    assert not is_safe_zip_path(target_dir, "/etc/passwd")

    # Safe members
    assert is_safe_zip_path(target_dir, "data.csv")
    assert is_safe_zip_path(target_dir, "sub/data.csv")


def test_resolve_artifact_path_wildcard(tmp_path: Path):
    logger = logging.getLogger("test_resolve")

    file_a = tmp_path / "file_202401.zip"
    file_b = tmp_path / "file_202402.zip"
    file_a.write_bytes(b"a")
    file_b.write_bytes(b"b")

    pattern = str(tmp_path / "file_*.zip")
    resolved = resolve_artifact_path(pattern, logger)
    # Picks latest lexicographical match
    assert resolved == file_b


def test_gbfs_response_cap_and_cleanup(tmp_path: Path):
    target_file = tmp_path / "station_info.json"

    # Simulate an oversized response
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.iter_content.return_value = [b"A" * (1024 * 1024)] * 60  # 60 MB > 50 MB limit
    mock_resp.__enter__.return_value = mock_resp

    with patch("requests.Session.get", return_value=mock_resp):
        with pytest.raises(ValueError, match="exceeded safety ceiling"):
            fetch_and_save_json("http://example.com/large.json", target_file)

    assert not target_file.exists()
    assert not target_file.with_suffix(f"{target_file.suffix}.tmp").exists()


def test_create_unique_gbfs_dir_collision_avoidance(tmp_path: Path):
    fixed_time = datetime(2026, 10, 21, 11, 0, 0, 654321, tzinfo=timezone.utc)
    base_dir = tmp_path / "snapshots"

    run_id_1, dir_1 = create_unique_gbfs_dir(base_dir, fixed_time)
    run_id_2, dir_2 = create_unique_gbfs_dir(base_dir, fixed_time)

    assert run_id_1 != run_id_2
    assert dir_1.exists() and dir_2.exists()
    assert dir_1 != dir_2
