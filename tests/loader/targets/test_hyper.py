"""
Unit tests for loader/targets/hyper: normalize_table_for_hyper (Arrow type normalization)
and that we pass a PyArrow Table to pantab (no pandas dtypes).
"""
import unittest
from unittest import mock

import pyarrow as pa
from loader.targets.hyper import (
    normalize_table_for_hyper,
    write_table_to_hyper,
    _target_arrow_type,
)


class TestNormalizeTableForHyper(unittest.TestCase):
    """Test normalize_table_for_hyper casts columns to Arrow types for pantab."""

    def test_empty_table(self):
        tbl = pa.table({})
        out = normalize_table_for_hyper(tbl)
        self.assertEqual(out.num_rows, 0)
        self.assertEqual(out.num_columns, 0)

    def test_none_table(self):
        self.assertIsNone(normalize_table_for_hyper(None))

    def test_string_column_unchanged(self):
        tbl = pa.table({"a": pa.array(["x", "y"])})
        out = normalize_table_for_hyper(tbl)
        self.assertEqual(out.column("a").type, pa.string())

    def test_int64_pyarrow_to_int64(self):
        tbl = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        out = normalize_table_for_hyper(tbl)
        self.assertEqual(out.column("x").type, pa.int64())

    def test_int32_casted_to_int64(self):
        tbl = pa.table({"x": pa.array([1, 2], type=pa.int32())})
        out = normalize_table_for_hyper(tbl)
        self.assertEqual(out.column("x").type, pa.int64())

    def test_float32_casted_to_float64(self):
        tbl = pa.table({"f": pa.array([1.0, 2.0], type=pa.float32())})
        out = normalize_table_for_hyper(tbl)
        self.assertEqual(out.column("f").type, pa.float64())

    def test_timestamp_casted_to_us_utc(self):
        tbl = pa.table({
            "ts": pa.array([1000000, 2000000], type=pa.timestamp("us", tz="UTC")),
        })
        out = normalize_table_for_hyper(tbl)
        self.assertEqual(out.column("ts").type, pa.timestamp("us", tz="UTC"))

    def test_mixed_types(self):
        tbl = pa.table({
            "s": pa.array(["a", "b"]),
            "i": pa.array([1, 2], type=pa.int32()),
            "f": pa.array([1.0, 2.0], type=pa.float32()),
        })
        out = normalize_table_for_hyper(tbl)
        self.assertEqual(out.column("s").type, pa.string())
        self.assertEqual(out.column("i").type, pa.int64())
        self.assertEqual(out.column("f").type, pa.float64())


class TestWriteTableToHyperPassesTable(unittest.TestCase):
    """Test that write_table_to_hyper passes a PyArrow Table to pantab."""

    def test_frame_to_hyper_receives_table(self):
        with mock.patch("loader.targets.hyper.pt") as mock_pt:
            with mock.patch("loader.targets.hyper.os.makedirs"):
                tbl = pa.table({"a": pa.array([1, 2])})
                write_table_to_hyper(tbl, "/tmp/out.hyper", table_name="t")
                mock_pt.frame_to_hyper.assert_called_once()
                args = mock_pt.frame_to_hyper.call_args[0]
                arg0 = args[0]
                self.assertIsInstance(arg0, pa.Table)
                self.assertEqual(arg0.column_names, ["a"])
                self.assertEqual(arg0.num_rows, 2)


class TestTargetArrowType(unittest.TestCase):
    """Test _target_arrow_type returns correct Arrow type for Hyper."""

    def test_string_field(self):
        self.assertEqual(_target_arrow_type(pa.field("x", pa.string())), pa.string())

    def test_int64_field(self):
        self.assertEqual(_target_arrow_type(pa.field("x", pa.int64())), pa.int64())

    def test_float32_field(self):
        self.assertEqual(_target_arrow_type(pa.field("x", pa.float32())), pa.float64())

    def test_timestamp_field(self):
        t = _target_arrow_type(pa.field("x", pa.timestamp("ns")))
        self.assertEqual(t, pa.timestamp("us", tz="UTC"))


class TestDiffHyperSchema(unittest.TestCase):
    """Test diff_hyper_schema: detects added, removed, and type-changed columns."""

    def _existing_cols(self, **fields):
        """Build a list of (name, pa.DataType) the way get_hyper_table_columns would return."""
        from loader.targets.hyper import normalize_table_for_hyper, _target_arrow_type
        tbl = pa.table({k: pa.array([], type=v) for k, v in fields.items()})
        normalized = normalize_table_for_hyper(tbl)
        return [(field.name, field.type) for field in normalized.schema]

    def test_identical_schemas_returns_none(self):
        from loader.targets.hyper import diff_hyper_schema
        existing = self._existing_cols(a=pa.int64(), b=pa.string())
        incoming = pa.table({"a": pa.array([1], type=pa.int64()), "b": pa.array(["x"])})
        result = diff_hyper_schema(existing, incoming)
        self.assertIsNone(result)

    def test_added_column_detected(self):
        from loader.targets.hyper import diff_hyper_schema
        existing = self._existing_cols(a=pa.int64())
        incoming = pa.table({"a": pa.array([1], type=pa.int64()), "b": pa.array(["x"])})
        result = diff_hyper_schema(existing, incoming)
        self.assertIsNotNone(result)
        self.assertIn("b", result["added"])
        self.assertEqual(result["removed"], [])

    def test_removed_column_detected(self):
        from loader.targets.hyper import diff_hyper_schema
        existing = self._existing_cols(a=pa.int64(), b=pa.string())
        incoming = pa.table({"a": pa.array([1], type=pa.int64())})
        result = diff_hyper_schema(existing, incoming)
        self.assertIsNotNone(result)
        self.assertIn("b", result["removed"])
        self.assertEqual(result["added"], [])

    def test_type_change_detected(self):
        from loader.targets.hyper import diff_hyper_schema
        existing = self._existing_cols(a=pa.int64())
        incoming = pa.table({"a": pa.array(["text"])})
        result = diff_hyper_schema(existing, incoming)
        self.assertIsNotNone(result)
        self.assertEqual(len(result["type_changed"]), 1)
        col_name, _, _ = result["type_changed"][0]
        self.assertEqual(col_name, "a")

    def test_int32_normalises_to_same_as_int64(self):
        """int32 incoming normalises to int64, matching an existing int64 column."""
        from loader.targets.hyper import diff_hyper_schema
        existing = self._existing_cols(x=pa.int64())
        incoming = pa.table({"x": pa.array([1, 2], type=pa.int32())})
        result = diff_hyper_schema(existing, incoming)
        self.assertIsNone(result)


class TestGetHyperTableColumns(unittest.TestCase):
    """Test get_hyper_table_columns returns None for missing files and None for missing tables."""

    def test_returns_none_for_missing_file(self):
        from loader.targets.hyper import get_hyper_table_columns
        result = get_hyper_table_columns("/nonexistent/path.hyper")
        self.assertIsNone(result)

    def test_returns_none_when_pantab_query_fails(self):
        from loader.targets.hyper import get_hyper_table_columns
        with mock.patch("loader.targets.hyper.hyper_file_exists", return_value=True):
            with mock.patch("loader.targets.hyper.pt") as mock_pt:
                mock_pt.frame_from_hyper_query.side_effect = Exception("table not found")
                result = get_hyper_table_columns("/fake.hyper", "Extract", "Extract")
                self.assertIsNone(result)

    def test_returns_column_list_on_success(self):
        from loader.targets.hyper import get_hyper_table_columns
        import pyarrow as pa
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        empty_table = pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())})
        with mock.patch("loader.targets.hyper.hyper_file_exists", return_value=True):
            with mock.patch("loader.targets.hyper.pt") as mock_pt:
                mock_pt.frame_from_hyper_query.return_value = empty_table
                result = get_hyper_table_columns("/fake.hyper")
                self.assertIsNotNone(result)
                self.assertEqual(len(result), 2)
                names = [r[0] for r in result]
                self.assertIn("a", names)
                self.assertIn("b", names)


class TestDropHyperTable(unittest.TestCase):
    """Test drop_hyper_table: graceful no-op when file absent, returns False if tableauhyperapi missing."""

    def test_returns_true_for_missing_file(self):
        from loader.targets.hyper import drop_hyper_table
        result = drop_hyper_table("/nonexistent/path.hyper")
        self.assertTrue(result)

    def test_returns_false_when_tableauhyperapi_missing(self):
        from loader.targets.hyper import drop_hyper_table
        with mock.patch("loader.targets.hyper.hyper_file_exists", return_value=True):
            with mock.patch.dict("sys.modules", {"tableauhyperapi": None}):
                import builtins
                real_import = builtins.__import__

                def mock_import(name, *args, **kwargs):
                    if name == "tableauhyperapi":
                        raise ImportError("No module named 'tableauhyperapi'")
                    return real_import(name, *args, **kwargs)

                with mock.patch("builtins.__import__", side_effect=mock_import):
                    result = drop_hyper_table("/fake.hyper")
                    self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
