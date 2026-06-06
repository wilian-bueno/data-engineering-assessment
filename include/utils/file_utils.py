"""
File metadata helpers.

Used by the ingestion layer to detect whether a source file has changed
since the last pipeline run. If the MD5 hash matches → skip load.
"""

import hashlib
import os
from datetime import datetime, timezone


def compute_file_hash(file_path: str) -> str:
    """Returns the MD5 hex digest of a file's full contents."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        # Read in 8KB chunks to handle large files without loading all into memory
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def get_file_mtime(file_path: str) -> datetime:
    """Returns the file's last-modified time as a UTC-aware datetime."""
    return datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc)


def get_file_size(file_path: str) -> int:
    """Returns the file size in bytes."""
    return os.path.getsize(file_path)
