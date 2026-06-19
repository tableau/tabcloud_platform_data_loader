"""ExtractionResult and run_incremental outcome wiring."""

import unittest
from unittest.mock import MagicMock, patch

from extractor.path_layout import ExtractPathLayout
from extractor.retriever import LogRetriever


class TestRunIncrementalOutcome(unittest.TestCase):
    def _retriever(self):
        return LogRetriever(
            pat_name="n",
            pat_secret="s",
            api_url="https://example.com",
            site_ids=[],
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-01T01:00:00Z",
            storage_backend=MagicMock(),
            path_layout=ExtractPathLayout.DATE_FIRST,
        )

    def test_degraded_when_process_reports_failed_paths(self):
        r = self._retriever()
        r.session_token = "tok"
        r.tenant_id = "t1"
        with patch.object(
            LogRetriever,
            "_get_file_list_for_event_types",
            return_value=[{"path": "y=2024/m=1/d=1/h=0/eventType=Login/a.json"}],
        ), patch.object(
            LogRetriever,
            "_process_files",
            return_value={"failed_paths": ["y=2024/m=1/d=1/h=0/eventType=Login/a.json"], "expected": 1},
        ):
            ex = r.run_incremental(event_types=None, retrieve_tenant_logs=True)
        self.assertEqual(ex.status, "degraded")
        self.assertEqual(ex.expected_downloads, 1)
        self.assertTrue(ex.failed_paths)

    def test_success_when_nothing_to_fetch(self):
        r = self._retriever()
        r.session_token = "tok"
        r.tenant_id = "t1"
        with patch.object(
            LogRetriever, "_get_file_list_for_event_types", return_value=[]
        ), patch.object(LogRetriever, "_process_files") as mock_pf:
            ex = r.run_incremental(event_types=None, retrieve_tenant_logs=True)
        mock_pf.assert_not_called()
        self.assertEqual(ex.status, "success")
        self.assertEqual(ex.failed_paths, [])

    def test_failed_on_login(self):
        r = self._retriever()
        with patch.object(LogRetriever, "_login", return_value=False):
            ex = r.run_incremental(event_types=None, retrieve_tenant_logs=False)
        self.assertEqual(ex.status, "failed")
