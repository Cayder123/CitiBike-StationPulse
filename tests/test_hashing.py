"""Unit tests for hashing utilities."""

import pytest
from pathlib import Path
from src.common.hashing import calculate_sha256_bytes, calculate_sha256_file


def test_calculate_sha256_bytes_known_values():
    # Empty byte string known SHA-256
    empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert calculate_sha256_bytes(b"") == empty_hash

    # "hello world" known SHA-256
    hello_hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    assert calculate_sha256_bytes(b"hello world") == hello_hash


def test_calculate_sha256_file_matches_bytes(tmp_path: Path):
    test_content = b"CitiBike-StationPulse pipeline test content 123456789"
    file_path = tmp_path / "test_artifact.bin"
    file_path.write_bytes(test_content)

    file_hash = calculate_sha256_file(file_path, chunk_size=16)
    bytes_hash = calculate_sha256_bytes(test_content)

    assert file_hash == bytes_hash
    assert len(file_hash) == 64


def test_calculate_sha256_file_not_found(tmp_path: Path):
    non_existent = tmp_path / "missing.txt"
    with pytest.raises(FileNotFoundError):
        calculate_sha256_file(non_existent)


def test_calculate_sha256_directory_error(tmp_path: Path):
    with pytest.raises(IsADirectoryError):
        calculate_sha256_file(tmp_path)
