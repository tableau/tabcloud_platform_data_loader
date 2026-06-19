"""
Tests for Parquet integrity validation inside retriever._download_file.

Covers:
  - Truncated body (bad magic footer) → rejected, returns None, no file written
  - Content-Length mismatch → rejected, returns None, no file written
  - Valid Parquet → accepted, written to storage, returns relative path
  - Non-Parquet profile (ACTIVITYLOG) → magic check skipped, file written normally
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

SRC = os.path.join(os.path.dirname(__file__), "..", "..", "src")
sys.path.insert(0, os.path.abspath(SRC))

from extractor.path_layout import ExtractPathLayout
from extractor.retriever import LogRetriever
from common.dataset_profile import ACTIVITYLOG, SNAPSHOTS


MAGIC = b"PAR1"


def _valid_parquet(payload: bytes = b"x" * 200) -> bytes:
    return MAGIC + payload + MAGIC


def _truncated_parquet(payload: bytes = b"x" * 200) -> bytes:
    return MAGIC + payload  # missing footer


def _make_mock_response(body: bytes, content_length: int | None = None) -> MagicMock:
    resp = MagicMock()
    headers = {}
    if content_length is not None:
        headers["Content-Length"] = str(content_length)
    resp.headers = headers
    resp.iter_content = lambda chunk_size: [body]
    return resp


def _make_retriever(profile=SNAPSHOTS) -> LogRetriever:
    storage = MagicMock()
    storage.write_bytes = MagicMock()
    storage.uri = lambda p: f"/fake/{p}"
    retriever = LogRetriever(
        pat_name="n",
        pat_secret="s",
        api_url="https://example.com",
        site_ids=[],
        start_time="2024-01-01T00:00:00Z",
        end_time="2024-01-02T00:00:00Z",
        storage_backend=storage,
        path_layout=ExtractPathLayout.DATE_FIRST,
        dataset_profile=profile,
    )
    retriever.log_level = "info"
    return retriever


class TestDownloadFileParquetValidation(unittest.TestCase):

    def test_valid_parquet_accepted_and_written(self):
        retriever = _make_retriever(SNAPSHOTS)
        body = _valid_parquet()
        mock_resp = _make_mock_response(body)
        rel = "entityType=users/y=2026/m=06/d=07/h=12/part-0.parquet"

        with patch(
            "extractor.retriever.download_get_with_retry", return_value=mock_resp
        ):
            result = retriever._download_file("https://fake.url/file", rel, rel)

        self.assertEqual(result, rel)
        retriever.storage_backend.write_bytes.assert_called_once_with(rel, body)

    def test_truncated_parquet_rejected_no_file_written(self):
        retriever = _make_retriever(SNAPSHOTS)
        body = _truncated_parquet()
        mock_resp = _make_mock_response(body)
        rel = "entityType=users/y=2026/m=06/d=07/h=12/part-0.parquet"

        with patch(
            "extractor.retriever.download_get_with_retry", return_value=mock_resp
        ):
            result = retriever._download_file("https://fake.url/file", rel, rel)

        self.assertIsNone(result)
        retriever.storage_backend.write_bytes.assert_not_called()

    def test_content_length_mismatch_rejected(self):
        retriever = _make_retriever(SNAPSHOTS)
        body = _valid_parquet()
        # Claim body is 9999 bytes but actual is len(body).
        mock_resp = _make_mock_response(body, content_length=9999)
        rel = "entityType=users/y=2026/m=06/d=07/h=12/part-0.parquet"

        with patch(
            "extractor.retriever.download_get_with_retry", return_value=mock_resp
        ):
            result = retriever._download_file("https://fake.url/file", rel, rel)

        self.assertIsNone(result)
        retriever.storage_backend.write_bytes.assert_not_called()

    def test_content_length_matches_valid_body(self):
        retriever = _make_retriever(SNAPSHOTS)
        body = _valid_parquet()
        mock_resp = _make_mock_response(body, content_length=len(body))
        rel = "entityType=users/y=2026/m=06/d=07/h=12/part-0.parquet"

        with patch(
            "extractor.retriever.download_get_with_retry", return_value=mock_resp
        ):
            result = retriever._download_file("https://fake.url/file", rel, rel)

        self.assertEqual(result, rel)
        retriever.storage_backend.write_bytes.assert_called_once()

    def test_activitylog_profile_skips_magic_check(self):
        """Non-Parquet profile: even bad bytes should be written without validation."""
        retriever = _make_retriever(ACTIVITYLOG)
        body = b'{"eventType":"Login"}\n'
        mock_resp = _make_mock_response(body)
        rel = "eventType=Login/y=2026/m=06/d=07/h=00/part-0.ndjson"

        with patch(
            "extractor.retriever.download_get_with_retry", return_value=mock_resp
        ):
            result = retriever._download_file("https://fake.url/file", rel, rel)

        self.assertEqual(result, rel)
        retriever.storage_backend.write_bytes.assert_called_once_with(rel, body)


if __name__ == "__main__":
    unittest.main()
