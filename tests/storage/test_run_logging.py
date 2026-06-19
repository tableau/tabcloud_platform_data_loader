"""Resolve log paths, prune retention, tee smoke."""

import io
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

from storage.base import get_logs_path
from storage.run_logging import install_run_logging, prune_old_log_files, resolve_log_dir


class TestResolveLogDir(unittest.TestCase):
    def test_defaults_to_logs_under_storage(self):
        base = os.path.abspath(os.path.join(tempfile.gettempdir(), "pd_logs_base"))
        r = resolve_log_dir(base, None)
        self.assertEqual(r, os.path.abspath(get_logs_path(base)))

    def test_explicit_log_dir_absolute(self):
        tmp = tempfile.mkdtemp()
        try:
            r = resolve_log_dir("./other", tmp)
            self.assertEqual(r, os.path.abspath(tmp))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestPruneOldLogFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_removes_old_log_and_rotated(self):
        old_log = os.path.join(self.tmp, "extractor_200001011200.log")
        old_rot = os.path.join(self.tmp, "extractor_200001011200.log.1")
        keep = os.path.join(self.tmp, "extractor_recent.log")
        other = os.path.join(self.tmp, "notes.txt")
        for path in (old_log, old_rot, keep, other):
            with open(path, "w", encoding="utf-8") as f:
                f.write("x")
        ancient = time.time() - 400 * 86400
        os.utime(old_log, (ancient, ancient))
        os.utime(old_rot, (ancient, ancient))
        prune_old_log_files(self.tmp, 60)
        self.assertFalse(os.path.isfile(old_log))
        self.assertFalse(os.path.isfile(old_rot))
        self.assertTrue(os.path.isfile(keep))
        self.assertTrue(os.path.isfile(other))

    def test_keeps_recent_under_cutoff(self):
        path = os.path.join(self.tmp, "loader_209901011200.log")
        with open(path, "w", encoding="utf-8") as f:
            f.write("x")
        prune_old_log_files(self.tmp, 60)
        self.assertTrue(os.path.isfile(path))


class TestInstallRunLogging(unittest.TestCase):
    def tearDown(self):
        if hasattr(self, "_tmp"):
            shutil.rmtree(self._tmp, ignore_errors=True)

    def test_tee_writes_same_content_to_stream_and_file(self):
        self._tmp = tempfile.mkdtemp()
        buf = io.StringIO()
        orig_out, orig_err = sys.stdout, sys.stderr
        try:
            res = install_run_logging(
                "transformer",
                self._tmp,
                60,
                stdout=buf,
                stderr=buf,
            )
            sys.stdout.write("hello_tee\n")
            sys.stdout.flush()
            res.stop()
            # Segments are numbered sequentially: the first is <base>.log.1.
            logs = list(Path(self._tmp).glob("transformer_*.log.*"))
            self.assertEqual(len(logs), 1, "expected one segment for a tiny run")
            self.assertTrue(str(logs[0]).endswith(".log.1"), "first segment must be .log.1")
            text = logs[0].read_text(encoding="utf-8")
            self.assertIn("hello_tee", text)
            self.assertIn("hello_tee", buf.getvalue())
        finally:
            sys.stdout, sys.stderr = orig_out, orig_err

    def _segment_index(self, path: Path) -> int:
        return int(path.name.rsplit(".", 1)[1])

    def test_large_run_rotates_with_sequential_numbering(self):
        """A multi-segment run must number segments chronologically (1, 2, 3, ...)
        with the first segment (.log.1) holding the earliest content."""
        self._tmp = tempfile.mkdtemp()
        buf = io.StringIO()
        orig_out, orig_err = sys.stdout, sys.stderr
        try:
            res = install_run_logging(
                "extractor",
                self._tmp,
                60,
                stdout=buf,
                stderr=buf,
            )
            sys.stdout.write("SCRIPT_START_MARKER\n")
            # ~12MB of output -> at least 3 segments at the 5MB cap.
            chunk = "x" * 1024
            for _ in range(12 * 1024):
                sys.stdout.write(chunk + "\n")
            sys.stdout.write("SCRIPT_END_MARKER\n")
            sys.stdout.flush()
            res.stop()

            segments = sorted(
                Path(self._tmp).glob("extractor_*.log.*"),
                key=self._segment_index,
            )
            self.assertGreaterEqual(len(segments), 3, "expected multiple 5MB segments")
            indices = [self._segment_index(p) for p in segments]
            # Numbering is contiguous and starts at 1.
            self.assertEqual(indices, list(range(1, len(segments) + 1)))
            # Each non-final segment is capped near 5MB.
            for seg in segments[:-1]:
                self.assertLessEqual(seg.stat().st_size, 5 * 1024 * 1024 + 64 * 1024)
            # First segment holds the start; last holds the end.
            self.assertIn("SCRIPT_START_MARKER", segments[0].read_text(encoding="utf-8"))
            self.assertIn("SCRIPT_END_MARKER", segments[-1].read_text(encoding="utf-8"))
        finally:
            sys.stdout, sys.stderr = orig_out, orig_err

    def test_cumulative_cap_drops_oldest_keeps_numbering(self):
        """When cumulative size exceeds the budget, oldest segments are deleted
        while surviving segments keep their original (monotonic) numbers."""
        from storage.run_logging import SequentialRotatingFileHandler

        self._tmp = tempfile.mkdtemp()
        base = os.path.join(self._tmp, "extractor_20260608120000.log")
        # 1KB segments, 3KB total budget -> retain only newest 3 segments.
        h = SequentialRotatingFileHandler(
            base, max_bytes=1024, max_total_bytes=3 * 1024, encoding="utf-8"
        )
        try:
            rec_logger = __import__("logging").getLogger("seqtest")
            rec_logger.handlers.clear()
            rec_logger.addHandler(h)
            rec_logger.setLevel(10)
            rec_logger.propagate = False
            # Write enough ~200-byte lines to force several rollovers.
            for i in range(60):
                rec_logger.info("line-%03d-%s", i, "y" * 180)
        finally:
            h.close()

        segments = sorted(
            Path(self._tmp).glob("extractor_20260608120000.log.*"),
            key=self._segment_index,
        )
        # Never retain more than the budget allows.
        self.assertLessEqual(len(segments), 3)
        indices = [self._segment_index(p) for p in segments]
        # Surviving indices are the highest (newest) and contiguous; numbering
        # was not reset (highest index > number of retained files once pruning
        # has occurred).
        self.assertEqual(indices, sorted(indices))
        self.assertGreater(max(indices), len(segments))


if __name__ == "__main__":
    unittest.main()
