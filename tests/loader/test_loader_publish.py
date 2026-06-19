"""
Tests for loader/loader.py: _do_hyper_update return value, and the gating of
load_hyper's return (total rows vs -1 sentinel) on publish success vs failure.

_do_hyper_update imports tableau_publish via local imports, so we patch the module
itself at `loader.targets.tableau_publish` rather than a top-level alias. It also
writes a payload Hyper file via loader.loader.write_table_to_hyper before signing in,
so that is patched out in the unit tests.
"""
import datetime
import os
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_UTC = datetime.timezone.utc


def _make_parquet(path: str, timestamps: list) -> None:
    ts = pa.array(timestamps, type=pa.timestamp("us", tz="UTC"))
    pq.write_table(pa.table({"event_time": ts}), path)


def _publish_options(**kwargs) -> dict:
    base = {
        "server_url": "https://fake.tableau.com",
        "token_name": "tok",
        "token_secret": "sec",
        "site": "mysite",
        "project": "myproject",
        "datasource_name": "my_ds",
    }
    base.update(kwargs)
    return base


def _payload_tables():
    """A single-table delta payload; content is irrelevant since the Hyper write is mocked."""
    tbl = pa.table({"event_time": pa.array([1], type=pa.int64())})
    return [(tbl, "insert", "Extract", "Extract")]


# ---------------------------------------------------------------------------
# _do_hyper_update unit tests
# ---------------------------------------------------------------------------

class TestDoHyperUpdateReturnValue(unittest.TestCase):
    """_do_hyper_update must return True only when the PATCH (update_hyper_data) succeeds."""

    def _call(self, publish_options=None, fallback_publish=False):
        from loader.loader import _do_hyper_update
        with mock.patch("loader.loader.write_table_to_hyper", return_value=1):
            return _do_hyper_update(
                _payload_tables(),
                "/fake/local.hyper",
                publish_options or _publish_options(),
                fallback_publish=fallback_publish,
            )

    def test_returns_true_on_success(self):
        with mock.patch("loader.targets.tableau_publish.sign_in", return_value=("tok", "site")), \
             mock.patch("loader.targets.tableau_publish.resolve_site_id", return_value="site"), \
             mock.patch("loader.targets.tableau_publish.resolve_project_id", return_value="proj"), \
             mock.patch("loader.targets.tableau_publish.initiate_file_upload", return_value="up"), \
             mock.patch("loader.targets.tableau_publish.append_to_file_upload"), \
             mock.patch("loader.targets.tableau_publish.query_datasources",
                        return_value=[{"name": "my_ds", "id": "ds_id"}]), \
             mock.patch("loader.targets.tableau_publish.update_hyper_data", return_value={}):
            result = self._call()
        self.assertTrue(result)

    def test_returns_false_when_datasource_not_found(self):
        with mock.patch("loader.targets.tableau_publish.sign_in", return_value=("tok", "site")), \
             mock.patch("loader.targets.tableau_publish.resolve_site_id", return_value="site"), \
             mock.patch("loader.targets.tableau_publish.resolve_project_id", return_value="proj"), \
             mock.patch("loader.targets.tableau_publish.initiate_file_upload", return_value="up"), \
             mock.patch("loader.targets.tableau_publish.append_to_file_upload"), \
             mock.patch("loader.targets.tableau_publish.query_datasources",
                        return_value=[{"name": "other_ds", "id": "other_id"}]), \
             mock.patch("loader.targets.tableau_publish.update_hyper_data") as mock_update:
            result = self._call()
        self.assertFalse(result)
        mock_update.assert_not_called()

    def test_returns_false_on_tableau_publish_error(self):
        from loader.targets.tableau_publish import TableauPublishError
        err = TableauPublishError("sign_in", "auth failed", {}, "Check credentials.")
        with mock.patch("loader.targets.tableau_publish.sign_in", side_effect=err):
            result = self._call()
        self.assertFalse(result)

    def test_returns_false_on_generic_exception(self):
        with mock.patch("loader.targets.tableau_publish.sign_in",
                        side_effect=RuntimeError("network error")):
            result = self._call()
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# Regression: datasource-not-found (no fallback) should log an error and return False
# ---------------------------------------------------------------------------

class TestDoHyperUpdateDatasourceNotFoundIsError(unittest.TestCase):

    def test_missing_datasource_logs_error_and_returns_false(self):
        from loader.loader import _do_hyper_update
        with mock.patch("loader.targets.tableau_publish.sign_in", return_value=("tok", "site")), \
             mock.patch("loader.targets.tableau_publish.resolve_site_id", return_value="site"), \
             mock.patch("loader.targets.tableau_publish.resolve_project_id", return_value="proj"), \
             mock.patch("loader.targets.tableau_publish.initiate_file_upload", return_value="up"), \
             mock.patch("loader.targets.tableau_publish.append_to_file_upload"), \
             mock.patch("loader.targets.tableau_publish.query_datasources", return_value=[]), \
             mock.patch("loader.targets.tableau_publish.update_hyper_data") as mock_update, \
             mock.patch("loader.loader.write_table_to_hyper", return_value=1), \
             mock.patch("loader.loader.logger") as mock_logger:
            result = _do_hyper_update(
                _payload_tables(),
                "/fake/local.hyper",
                _publish_options(datasource_name="missing_ds"),
                fallback_publish=False,
            )

        self.assertFalse(result)
        mock_update.assert_not_called()
        mock_logger.error.assert_called()


# ---------------------------------------------------------------------------
# load_hyper: return-value gating for transformer_mappings incremental branch
# ---------------------------------------------------------------------------

class TestLoadHyperPublishGating(unittest.TestCase):
    """
    load_hyper must return the positive row count only when publish succeeds, and the
    -1 failure sentinel when publish fails. State (last increment max) lives in the Hyper
    file itself (read_hyper_table_max). These tests target the return-value gating logic,
    so read_hyper_table_max is mocked to a non-None last_max (routing execution through the
    publish path, not first-run) and the Hyper writes are mocked out -- no live Hyper
    process is required. Default publish mode is "upsert"; with no batched payload the
    upsert path performs a full publish via _publish_hyper_if_needed.
    """

    _SEED_MAX = datetime.datetime(2025, 1, 1, tzinfo=_UTC)

    _BASE_LOADER_CONFIG = {
        "target": {"type": "hyper", "hyper_path": "{storage_path}/loader/out.hyper"},
        "tables": [{
            "table": "Extract",
            "load_type": "incremental",
            "increment_field": "event_time",
            "transformer_mappings": ["mappings/transformer/login_analytics.yaml"],
        }],
    }

    def _setup(self, tmp_dir: str) -> str:
        """Write the minimal parquet the loader reads. The Hyper state read is mocked in
        _run, so no real Hyper file is seeded. Return the (mocked) hyper_path."""
        transform_path = os.path.join(tmp_dir, "transform")
        os.makedirs(transform_path, exist_ok=True)

        parquet_path = os.path.join(transform_path, "login_analytics.parquet")
        _make_parquet(parquet_path, [datetime.datetime(2026, 1, 2, tzinfo=_UTC)])

        return os.path.join(tmp_dir, "loader", "out.hyper")

    def _run(self, publish_succeeds: bool, tmp_dir: str):
        from loader.loader import load_hyper
        publish_options = _publish_options()

        # read_hyper_table_max mocked to a non-None last_max routes through the publish
        # path (not first-run); the Hyper writes are mocked so no live Hyper is needed.
        with mock.patch("loader.loader._publish_hyper_if_needed", return_value=publish_succeeds), \
             mock.patch("loader.loader.read_hyper_table_max", return_value=self._SEED_MAX), \
             mock.patch("loader.loader.write_table_to_hyper", return_value=1), \
             mock.patch("loader.loader.write_parquet_to_hyper", return_value=1):
            total = load_hyper(
                self._BASE_LOADER_CONFIG,
                storage_path=tmp_dir,
                transform_path=os.path.join(tmp_dir, "transform"),
                publish_options=publish_options,
            )
        return total

    def test_returns_row_count_on_publish_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._setup(tmp)
            total = self._run(publish_succeeds=True, tmp_dir=tmp)
            self.assertGreater(total, 0, "row count should be positive on successful publish")

    def test_returns_failure_sentinel_on_publish_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._setup(tmp)
            total = self._run(publish_succeeds=False, tmp_dir=tmp)
            self.assertEqual(total, -1, "load_hyper must return -1 when publish fails")


if __name__ == "__main__":
    unittest.main()
