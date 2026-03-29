"""Pure-Python tests for FHIR Bulk export manifest parsing (no Spark)."""

import os
import unittest

from dbignite.bulk_manifest import (
    compute_bulk_export_correlation_id,
    is_fhir_bulk_export_manifest,
    ndjson_paths_from_manifest,
    parse_bulk_manifest_file,
)


class TestBulkManifest(unittest.TestCase):
    def test_is_manifest(self):
        m = parse_bulk_manifest_file(
            os.path.join("sampledata", "bulk_export_manifest", "export-manifest.json")
        )
        self.assertTrue(is_fhir_bulk_export_manifest(m))

    def test_correlation_stable(self):
        m = parse_bulk_manifest_file(
            os.path.join("sampledata", "bulk_export_manifest", "export-manifest.json")
        )
        self.assertEqual(
            compute_bulk_export_correlation_id(m),
            compute_bulk_export_correlation_id(m),
        )

    def test_ndjson_paths(self):
        cid, paths, _ = ndjson_paths_from_manifest(
            os.path.join("sampledata", "bulk_export_manifest", "export-manifest.json")
        )
        self.assertEqual(len(paths), 1)
        self.assertTrue(os.path.isfile(paths[0]))
        self.assertEqual(len(cid), 64)


if __name__ == "__main__":
    unittest.main()
