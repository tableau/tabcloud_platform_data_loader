"""S3 default and merged config_kwargs for s3fs/botocore."""

import unittest

from storage.s3 import default_s3fs_config_kwargs, merge_s3fs_config_kwargs


class TestMergeConfig(unittest.TestCase):
    def test_default_has_retries(self):
        d = default_s3fs_config_kwargs()
        self.assertIn("retries", d)
        self.assertGreaterEqual(d["retries"].get("max_attempts", 0), 1)

    def test_merge_overrides_retries_mode(self):
        b = default_s3fs_config_kwargs()
        o = {"retries": {"max_attempts": 5, "mode": "standard"}}
        m = merge_s3fs_config_kwargs(b, o)
        self.assertEqual(m["retries"]["max_attempts"], 5)
        self.assertEqual(m["retries"]["mode"], "standard")
        self.assertIn("read_timeout", m)
