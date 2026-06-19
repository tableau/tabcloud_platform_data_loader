"""
Abstract storage backend and shared folder-layout helpers.

All components default to the same base path (DEFAULT_STORAGE_BASE) with dedicated
subfolders: extract/ (Extractor), transform/ (Transformer), loader/ (Loader), and
logs/ (per-run component log files when using the CLIs).

Storage path structure is highly structured and must be maintained.  The extractor
writes under extract/ using a supported layout (date-first or schema-first) derived
from the API path; the transformer relies on folder_pattern/file_pattern in mappings.
If the structure is ad-hoc or mixed beyond the two supported layouts, unpredictable
behavior can result.  Each storage base should span only one Tableau Cloud tenant.
"""

import os
from abc import ABC, abstractmethod


DEFAULT_STORAGE_BASE = "./platform_data"
EXTRACT_FOLDER = "extract"
TRANSFORM_FOLDER = "transform"
LOADER_FOLDER = "loader"
LOGS_FOLDER = "logs"


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def get_extract_path(base_path=None):
    """Return the path for extracted files (Extractor output).

    Layout is either **date-first** (API order: y/m/d/h then eventType=) or
    **schema-first** (eventType=, then y/m/d/h), selected via extractor
    ``--extract-path-layout``.  The transformer discovers files with
    ``folder_pattern``; typical ``eventType=...`` patterns work with both.
    """
    base = base_path or DEFAULT_STORAGE_BASE
    return os.path.join(base, EXTRACT_FOLDER)


def get_transform_path(base_path=None):
    """Return the path for Transformer output (Parquet/CSV files)."""
    base = base_path or DEFAULT_STORAGE_BASE
    return os.path.join(base, TRANSFORM_FOLDER)


def get_parquet_path(base_path=None):
    """Return the path for Transformer output. Alias for get_transform_path()."""
    return get_transform_path(base_path)


def get_loader_path(base_path=None):
    """Return the path for Loader temp/staging files."""
    base = base_path or DEFAULT_STORAGE_BASE
    return os.path.join(base, LOADER_FOLDER)


def get_logs_path(base_path=None):
    """Return the directory for component run logs (CLI ``--log-dir`` default)."""
    base = base_path or DEFAULT_STORAGE_BASE
    return os.path.join(base, LOGS_FOLDER)


def extract_segment_value_from_path(file_path: str, key: str) -> str | None:
    """Extract the value of a ``key=value`` path segment from *file_path*.

    Looks for ``{key}=`` anywhere in the path and returns the value up to
    the next ``/`` (or end of string).  Returns *None* if the segment is
    not present.

    Examples::

        extract_segment_value_from_path(
            "pod=a/siteLuid=b/y=2026/m=06/d=08/h=14/eventType=Login/f.txt",
            "eventType",
        )
        # → "Login"

        extract_segment_value_from_path(
            "entityType=Users/y=2026/m=06/d=08/h=14/part-0.parquet",
            "entityType",
        )
        # → "Users"

    :param file_path: A forward-slash-separated relative path.
    :param key: The segment key to look up (without the trailing ``=``).
    :returns: The segment value, or *None*.
    """
    if not file_path or not key:
        return None
    needle = f"{key}="
    if needle not in file_path:
        return None
    start = file_path.index(needle) + len(needle)
    end = file_path.find("/", start)
    if end == -1:
        return file_path[start:] or None
    return file_path[start:end] or None


def extract_event_type_from_path(file_path: str) -> str | None:
    """Extract the ``eventType`` value from an activity-log file path.

    Thin wrapper around :func:`extract_segment_value_from_path` kept for
    backward compatibility.  New code should call the generic helper directly.
    """
    return extract_segment_value_from_path(file_path, "eventType")


# ---------------------------------------------------------------------------
# Parquet integrity helpers
# ---------------------------------------------------------------------------

_PARQUET_MAGIC = b"PAR1"
_PARQUET_MIN_BYTES = 8  # 4-byte header + 4-byte footer minimum


def is_valid_parquet_bytes(data: bytes) -> bool:
    """Return True if *data* has valid Parquet magic bytes at header and footer.

    A valid Parquet file starts AND ends with the 4-byte sequence ``PAR1``.
    This is a lightweight check (no schema parsing, no pyarrow dependency)
    intended for post-download integrity validation.

    Returns False for:
    - Fewer than 8 bytes (too small for any real Parquet file).
    - Missing ``PAR1`` header or footer (truncated / non-Parquet data).
    """
    if not data or len(data) < _PARQUET_MIN_BYTES:
        return False
    return data[:4] == _PARQUET_MAGIC and data[-4:] == _PARQUET_MAGIC


def is_valid_parquet_file(path: str) -> bool:
    """Return True if the file at *path* has valid Parquet magic bytes.

    Reads only the first and last 4 bytes; does not load the full file.
    Returns False if the file does not exist, is unreadable, or fails the
    magic-byte check.
    """
    try:
        with open(path, "rb") as f:
            header = f.read(4)
            if header != _PARQUET_MAGIC:
                return False
            f.seek(-4, 2)
            footer = f.read(4)
        return footer == _PARQUET_MAGIC
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    @property
    @abstractmethod
    def base_path(self) -> str:
        """Root location of this backend.

        For local storage this is an absolute filesystem path; for S3 it is
        an ``s3://bucket/prefix`` URI.
        """

    @abstractmethod
    def list_files(self, prefix="") -> list:
        """List files whose path begins with *prefix* (relative to base).

        Returns a list of forward-slash-separated relative paths.
        """

    @abstractmethod
    def exists(self, relative_path: str) -> bool:
        """Return *True* if *relative_path* exists under the base."""

    @abstractmethod
    def read_file(self, relative_path: str) -> str:
        """Read and return the full text content (UTF-8) of a file."""

    @abstractmethod
    def read_bytes(self, relative_path: str) -> bytes:
        """Read and return the raw bytes of a file."""

    @abstractmethod
    def read_first_line(self, relative_path: str):
        """Read the first non-empty line without loading the entire file.

        Returns *None* if the file is empty.
        """

    @abstractmethod
    def write_bytes(self, relative_path: str, data: bytes) -> None:
        """Write raw *data* bytes to *relative_path*, creating parents."""

    @abstractmethod
    def write_file(self, relative_path: str, content: str) -> None:
        """Write text *content* (UTF-8) to *relative_path*, creating parents."""

    @abstractmethod
    def full_path(self, relative_path: str) -> str:
        """Return the fully-qualified path/URI for *relative_path*.

        Local: absolute filesystem path.  S3: ``s3://bucket/key``.
        """

    @abstractmethod
    def uri(self, relative_path: str) -> str:
        """Return a URI-like string suitable for logging."""
