"""Cryptographic hashing utilities for StationPulse pipeline.

Provides streaming SHA-256 hash calculation for immutable artifact validation.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Union


def calculate_sha256_file(file_path: Union[str, Path], chunk_size: int = 65536) -> str:
    """Calculate the SHA-256 hash of a file on disk using streaming chunks.

    Args:
        file_path: Absolute or relative path to the file.
        chunk_size: Number of bytes to read per chunk (default: 64KB).

    Returns:
        Hexadecimal string of SHA-256 checksum.

    Raises:
        FileNotFoundError: If the target file does not exist.
        IsADirectoryError: If the target path is a directory.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found for hash calculation: {path}")
    if path.is_dir():
        raise IsADirectoryError(f"Target path is a directory, not a file: {path}")

    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def calculate_sha256_bytes(data: bytes) -> str:
    """Calculate the SHA-256 hash of in-memory bytes.

    Args:
        data: Binary payload.

    Returns:
        Hexadecimal string of SHA-256 checksum.
    """
    return hashlib.sha256(data).hexdigest()
