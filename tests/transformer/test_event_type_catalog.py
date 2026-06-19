import os
import tempfile
import unittest

import yaml

from storage import LocalStorage
from transformer.event_type_catalog import (
    build_coverage_report,
    discover_event_types_in_extract,
    load_mapping_event_type_coverage,
    write_generic_auto_mapping,
)


class TestEventTypeCatalog(unittest.TestCase):
    def test_discover_event_types_in_extract(self):
        with tempfile.TemporaryDirectory() as d:
            storage = LocalStorage(d)
            storage.write_file("y=2026/m=1/d=1/h=0/eventType=Login/a.json", '{"k":1}\n')
            storage.write_file("y=2026/m=1/d=1/h=0/eventType=View/b.json", '{"k":2}\n')
            storage.write_file("y=2026/m=1/d=1/h=0/eventType=Login/c.json", '{"k":3}\n')

            found = discover_event_types_in_extract(storage)
            self.assertEqual(found, ["Login", "View"])

    def test_load_mapping_event_type_coverage_and_report(self):
        with tempfile.TemporaryDirectory() as d:
            mapping_dir = os.path.join(d, "mappings")
            os.makedirs(mapping_dir, exist_ok=True)
            with open(os.path.join(mapping_dir, "a.yaml"), "w", encoding="utf-8") as fh:
                fh.write(
                    "inputs:\n"
                    "  - eventType: Login\n"
                    "  - eventType: Download\n"
                )
            with open(os.path.join(mapping_dir, "b.yml"), "w", encoding="utf-8") as fh:
                fh.write(
                    "inputs:\n"
                    "  - eventType: Login\n"
                    "  - eventType: View\n"
                )

            coverage = load_mapping_event_type_coverage(mapping_dir)
            report = build_coverage_report(["Download", "Login", "Logout"], coverage)

            self.assertEqual(report["covered_event_types"], ["Download", "Login"])
            self.assertEqual(report["uncovered_event_types"], ["Logout"])
            self.assertEqual(len(report["mapping_coverage"]["Login"]), 2)

    def test_write_generic_auto_mapping(self):
        with tempfile.TemporaryDirectory() as d:
            output_path = os.path.join(d, "generated.yaml")
            write_generic_auto_mapping(
                output_path=output_path,
                discovered_event_types=["Login", "View"],
                output_filename="generated_generic",
            )

            with open(output_path, "r", encoding="utf-8") as fh:
                doc = yaml.safe_load(fh)
            self.assertEqual(doc["output"]["schema"], "auto")
            self.assertEqual(doc["output_filename"], "generated_generic")
            self.assertEqual(len(doc["inputs"]), 2)
            self.assertEqual(doc["inputs"][0]["source"]["folder_pattern"], "**/eventType=Login")


if __name__ == "__main__":
    unittest.main()
