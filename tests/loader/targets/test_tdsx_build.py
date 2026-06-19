import os
import tempfile
import unittest
import zipfile
from unittest import mock

from loader.targets.tdsx_build import build_tdsx, build_tdsx_with_auto_captions


class TestBuildTdsx(unittest.TestCase):
    def test_build_tdsx_with_non_namespaced_tds(self):
        with tempfile.TemporaryDirectory() as tmp:
            tds_path = os.path.join(tmp, "sample.tds")
            hyper_path = os.path.join(tmp, "sample.hyper")
            out_path = os.path.join(tmp, "sample.tdsx")

            with open(tds_path, "w", encoding="utf-8") as fh:
                fh.write(
                    """<?xml version="1.0" encoding="utf-8"?>
<datasource name="sample" inline="true">
  <connection class="hyper" dbname="" schema="Extract" tablename="Extract" />
</datasource>
"""
                )
            with open(hyper_path, "wb") as fh:
                fh.write(b"fake-hyper")

            built = build_tdsx(tds_path, hyper_path, output_path=out_path)

            self.assertTrue(os.path.isfile(built))
            with zipfile.ZipFile(built, "r") as zf:
                members = set(zf.namelist())
                self.assertIn("sample.tds", members)
                self.assertIn("Data/Extracts/sample.hyper", members)

    def test_build_tdsx_updates_nested_hyper_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            tds_path = os.path.join(tmp, "sample.tds")
            hyper_path = os.path.join(tmp, "sample.hyper")
            out_path = os.path.join(tmp, "sample.tdsx")
            with open(tds_path, "w", encoding="utf-8") as fh:
                fh.write(
                    """<?xml version="1.0" encoding="utf-8"?>
<datasource name="sample" inline="true">
  <connection class="federated">
    <named-connections>
      <named-connection name="hyper.generated">
        <connection class="hyper" dbname="" />
      </named-connection>
    </named-connections>
  </connection>
</datasource>
"""
                )
            with open(hyper_path, "wb") as fh:
                fh.write(b"fake-hyper")
            build_tdsx(tds_path, hyper_path, output_path=out_path)
            with zipfile.ZipFile(out_path, "r") as zf:
                tds_text = zf.read("sample.tds").decode("utf-8")
            self.assertIn('class="federated"', tds_text)
            self.assertIn('class="hyper"', tds_text)
            self.assertIn('dbname="Data/Extracts/sample.hyper"', tds_text)


class TestAutoCaptionTdsx(unittest.TestCase):
    @mock.patch("loader.targets.tdsx_build._discover_hyper_table_schema")
    def test_auto_caption_tdsx_contains_tableau_column_metadata(self, mock_schema):
        with tempfile.TemporaryDirectory() as tmp:
            hyper_path = os.path.join(tmp, "sample.hyper")
            out_path = os.path.join(tmp, "sample.tdsx")
            with open(hyper_path, "wb") as fh:
                fh.write(b"fake-hyper")
            mock_schema.return_value = [
                ("acceptLanguage", "text"),
                ("duration", "big_int"),
            ]
            built = build_tdsx_with_auto_captions(
                hyper_path,
                datasource_name="Generic Activity Log",
                output_path=out_path,
            )

            with zipfile.ZipFile(built, "r") as zf:
                tds_name = [n for n in zf.namelist() if n.endswith(".tds")][0]
                tds_text = zf.read(tds_name).decode("utf-8")
            self.assertIn('caption="Accept Language"', tds_text)
            self.assertIn('caption="Duration"', tds_text)
            self.assertIn('name="[acceptLanguage]"', tds_text)
            self.assertIn('datatype="string"', tds_text)
            self.assertIn('name="[duration]"', tds_text)
            self.assertIn('datatype="integer"', tds_text)


if __name__ == "__main__":
    unittest.main()
