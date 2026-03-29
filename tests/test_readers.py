import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from .test_base import PysparkBaseTest

class TestReaders(PysparkBaseTest):
    def test_FhirFormat(self):
        from dbignite.readers import FhirFormat
        assert (FhirFormat.BUNDLE.name,FhirFormat.BUNDLE.value) == ('BUNDLE', 1)
        assert (FhirFormat.NDJSON.name,FhirFormat.NDJSON.value) == ('NDJSON', 2)
        assert (FhirFormat.BULK.name,FhirFormat.BULK.value) == ('BULK', 3)


    def test_readFromDirectoryBundle(self):
        from dbignite.readers import read_from_directory, FhirFormat
        from  dbignite.fhir_resource import BundleFhirResource
        x = read_from_directory("./sampledata/*json", resource_format = FhirFormat.BUNDLE)
        self.assertTrue(isinstance(type(x), type(BundleFhirResource)))
        self.assertTrue ( x.entry().count() == 3 )
        self.assertTrue([y.asDict()['first'] for y in x.entry().select(col("Patient.name")[0][0].alias("name")).select(col("name.given")[0].alias("first")).collect()].sort() == ['Abe', 'Abraham', 'Abe'].sort() ) 

    def test_readFromDirectoryNDJSON(self):
        from dbignite.readers import read_from_directory, FhirFormat
        from  dbignite.fhir_resource import BundleFhirResource
        x = read_from_directory("./sampledata/ndjson_records/*json", resource_format = FhirFormat.NDJSON)
        self.assertTrue(isinstance(type(x), type(BundleFhirResource)))
        self.assertTrue ( x.entry().count() == 1 )

    def test_readFromDirectoryBULK(self):
        """FHIR Bulk Data export NDJSON uses the same path as NDJSON when no manifest."""
        from dbignite.readers import read_from_directory, FhirFormat
        from dbignite.fhir_resource import BundleFhirResource
        x = read_from_directory("./sampledata/ndjson_records/*json", resource_format = FhirFormat.BULK)
        self.assertTrue(isinstance(type(x), type(BundleFhirResource)))
        self.assertEqual(x.entry().count(), 1)

    def test_bundle_stable_bundle_uuid(self):
        from dbignite.readers import read_from_directory, FhirFormat
        a = read_from_directory("./sampledata/*json", resource_format=FhirFormat.BUNDLE).entry().select("bundleUUID").collect()
        b = read_from_directory("./sampledata/*json", resource_format=FhirFormat.BUNDLE).entry().select("bundleUUID").collect()
        self.assertEqual([r.bundleUUID for r in a], [r.bundleUUID for r in b])

    def test_bulk_manifest_correlation(self):
        from dbignite.readers import read_from_directory, FhirFormat
        x = read_from_directory(
            "./sampledata/ndjson_records/*json",
            resource_format=FhirFormat.BULK,
            bulk_manifest_path="./sampledata/bulk_export_manifest/export-manifest.json",
        )
        df = x.entry()
        self.assertEqual(df.count(), 1)
        self.assertIn("bulkExportCorrelationId", df.columns)
        self.assertIsNotNone(df.select("bulkExportCorrelationId").first()[0])

    def test_read_from_stream_ndjson_is_streaming(self):
        from dbignite.readers import read_from_stream, FhirFormat
        s = read_from_stream("./sampledata/ndjson_records/", resource_format=FhirFormat.NDJSON, spark=self.spark)
        df = s.entry()
        self.assertTrue(df.isStreaming)

if __name__ == '__main__':
    unittest.main()
