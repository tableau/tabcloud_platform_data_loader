"""
Per-run stdout/stderr tee to a rotating log file, plus retention pruning.

Used by extractor, transformer, and loader CLIs. Logs default to ``{storage-path}/logs``.
"""

from __future__ import annotations

import atexit
import logging
import os
import queue
import re
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import BaseRotatingHandler, QueueHandler, QueueListener
from typing import Optional, TextIO

from storage.base import DEFAULT_STORAGE_BASE, get_logs_path

# Each rotated segment within a run is capped at this size.
LOG_MAX_BYTES = 5 * 1024 * 1024
# Total cumulative size of segments retained for a single run. When exceeded,
# the OLDEST (lowest-numbered) segments are deleted. Debug runs can be large.
LOG_MAX_TOTAL_BYTES = 200 * 1024 * 1024


class SequentialRotatingFileHandler(BaseRotatingHandler):
    """Size-based rotating handler with chronological, monotonic numbering.

    Unlike :class:`logging.handlers.RotatingFileHandler` (where the unnumbered
    base file holds the *newest* content and ``.1`` ... ``.N`` are progressively
    older after a rename-shuffle), this handler writes sequentially numbered
    segments in **generation order**: the first segment is ``<base>.1``, the
    second ``<base>.2``, and so on. Numbers only ever increase for the life of
    the run and may grow to double/triple digits.

    Each segment is capped at ``max_bytes``. When the cumulative size of the
    retained segments would exceed ``max_total_bytes``, the oldest (lowest-
    numbered) segments are deleted; surviving segments keep their original
    numbers (no renumbering), so numbering stays stable and chronological.
    """

    def __init__(
        self,
        base_path: str,
        max_bytes: int = LOG_MAX_BYTES,
        max_total_bytes: int = LOG_MAX_TOTAL_BYTES,
        encoding: Optional[str] = None,
    ) -> None:
        self._base_path = os.path.abspath(base_path)
        self.max_bytes = int(max_bytes)
        self.max_total_bytes = int(max_total_bytes)
        # Number of 5MB segments to retain (at least one).
        if self.max_bytes > 0:
            self._max_files = max(1, self.max_total_bytes // self.max_bytes)
        else:
            self._max_files = 1
        self._index = 1
        # Open the first segment (<base>.1) as the active file.
        super().__init__(self._segment_path(self._index), mode="a", encoding=encoding, delay=False)
        self._prune_segments()

    def _segment_path(self, idx: int) -> str:
        return f"{self._base_path}.{idx}"

    def shouldRollover(self, record: logging.LogRecord) -> int:  # noqa: N802 (stdlib name)
        if self.max_bytes <= 0:
            return 0
        if self.stream is None:
            self.stream = self._open()
        msg = "%s%s" % (self.format(record), self.terminator)
        self.stream.seek(0, 2)
        if self.stream.tell() + len(msg) >= self.max_bytes:
            return 1
        return 0

    def doRollover(self) -> None:  # noqa: N802 (stdlib name)
        if self.stream:
            self.stream.close()
            self.stream = None
        self._index += 1
        # BaseRotatingHandler/FileHandler reopen via self.baseFilename.
        self.baseFilename = os.path.abspath(self._segment_path(self._index))
        self.stream = self._open()
        self._prune_segments()

    def _prune_segments(self) -> None:
        """Delete oldest segments so cumulative retained count stays within budget."""
        directory = os.path.dirname(self._base_path)
        prefix = os.path.basename(self._base_path) + "."
        segments: list[tuple[int, str]] = []
        try:
            for name in os.listdir(directory):
                if name.startswith(prefix):
                    suffix = name[len(prefix):]
                    if suffix.isdigit():
                        segments.append((int(suffix), os.path.join(directory, name)))
        except OSError:
            return
        segments.sort(key=lambda t: t[0])
        while len(segments) > self._max_files:
            _, path = segments.pop(0)
            try:
                os.remove(path)
            except OSError:
                pass


def resolve_log_dir(storage_path: Optional[str], log_dir: Optional[str]) -> str:
    """Absolute log directory: explicit ``log_dir`` or ``{storage_path}/logs``."""
    base = storage_path if storage_path is not None else DEFAULT_STORAGE_BASE
    raw = log_dir if log_dir else get_logs_path(base)
    return os.path.abspath(raw)


def _is_managed_log_basename(name: str) -> bool:
    if re.fullmatch(r".+\.log\.\d+", name):
        return True
    if re.fullmatch(r".+\.log", name):
        return True
    return False


def prune_old_log_files(log_dir: str, retention_days: int) -> None:
    """
    Remove files under ``log_dir`` matching ``*.log`` or ``*.log.N`` (rotation)
    whose mtime is older than ``retention_days`` (calendar days from now UTC).
    """
    if retention_days < 0:
        return
    cutoff = time.time() - float(retention_days) * 86400.0
    if not os.path.isdir(log_dir):
        return
    try:
        names = os.listdir(log_dir)
    except OSError:
        return
    for name in names:
        if not _is_managed_log_basename(name):
            continue
        path = os.path.join(log_dir, name)
        try:
            st = os.stat(path)
        except OSError:
            continue
        if not os.path.isfile(path):
            continue
        if st.st_mtime < cutoff:
            try:
                os.remove(path)
            except OSError:
                pass


@dataclass
class RunLogInstallResult:
    """Return value from :func:`install_run_logging`."""

    log_file_path: str
    listener: QueueListener

    def stop(self) -> None:
        """Stop the queue listener (flushes handlers). Safe to call twice."""
        try:
            self.listener.stop()
        except Exception:
            pass


def install_run_logging(
    component: str,
    log_dir_abs: str,
    retention_days: int,
    *,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
) -> RunLogInstallResult:
    """
    Prune old logs, open a new rotating log, tee stdout/stderr into it via a queue.

    ``stdout`` / ``stderr`` default to ``sys.__stdout__`` / ``sys.__stderr__``.
    """
    stdout = stdout if stdout is not None else sys.__stdout__
    stderr = stderr if stderr is not None else sys.__stderr__

    os.makedirs(log_dir_abs, exist_ok=True)
    prune_old_log_files(log_dir_abs, retention_days)

    # Seconds-granularity timestamp avoids cross-run filename collisions when two
    # runs of the same component start within the same minute.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    basename = f"{component}_{stamp}.log"
    log_path = os.path.join(log_dir_abs, basename)

    q: queue.Queue = queue.Queue(-1)
    # Rotate within a run at LOG_MAX_BYTES (5MB) per segment, numbering segments
    # sequentially by generation time (<base>.1, <base>.2, ...). Cumulative
    # retention for the run is capped at LOG_MAX_TOTAL_BYTES (200MB); the oldest
    # segments are dropped first while surviving segment numbers stay stable.
    fh = SequentialRotatingFileHandler(
        log_path,
        max_bytes=LOG_MAX_BYTES,
        max_total_bytes=LOG_MAX_TOTAL_BYTES,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter("%(message)s"))
    listener = QueueListener(q, fh, respect_handler_level=True)
    listener.start()

    tee_logger = logging.getLogger(f"platformdata.run_tee.{component}")
    tee_logger.handlers.clear()
    tee_logger.addHandler(QueueHandler(q))
    tee_logger.setLevel(logging.INFO)
    tee_logger.propagate = False

    emit_lock = threading.Lock()

    class TeeTextIO:
        __slots__ = ("_orig",)

        def __init__(self, orig: TextIO):
            object.__setattr__(self, "_orig", orig)

        def write(self, s: str) -> int:
            self._orig.write(s)
            if s:
                with emit_lock:
                    tee_logger.info("%s", s)
            return len(s)

        def flush(self) -> None:
            self._orig.flush()

        def isatty(self) -> bool:
            try:
                return self._orig.isatty()
            except Exception:
                return False

        def fileno(self) -> int:
            return self._orig.fileno()

        @property
        def encoding(self) -> str:
            enc = getattr(self._orig, "encoding", None)
            return enc if enc else "utf-8"

        def writable(self) -> bool:
            return True

    sys.stdout = TeeTextIO(stdout)
    sys.stderr = TeeTextIO(stderr)

    def _cleanup() -> None:
        try:
            listener.stop()
        except Exception:
            pass

    atexit.register(_cleanup)

    return RunLogInstallResult(log_file_path=log_path, listener=listener)
