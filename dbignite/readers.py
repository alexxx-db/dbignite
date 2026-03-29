import os
from typing import TYPE_CHECKING, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, lit
from enum import Enum

from dbignite.fhir_resource import FhirResource

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FhirFormat(Enum):
    """Input shape for ``read_from_directory`` / ``read_from_stream``."""

    BUNDLE = 1
    NDJSON = 2
    #: FHIR Bulk Data Access IG: NDJSON resource files; optional ``export-manifest.json``-style
    #: manifest (``output`` array) for correlation and file list.
    BULK = 3


def _infer_base_dir(path: str) -> str:
    """Directory used to auto-discover a Bulk export manifest next to the read glob."""
    normalized = os.path.normpath(path.replace("\\", "/"))
    if "*" in normalized:
        d = os.path.dirname(normalized)
        return d if d else "."
    if os.path.isdir(normalized):
        return normalized
    d = os.path.dirname(normalized)
    return d if d else "."


def read_from_directory(
    path: str,
    resource_format: FhirFormat = FhirFormat.BUNDLE,
    spark: SparkSession = SparkSession.getActiveSession(),
    bulk_manifest_path: Optional[str] = None,
    auto_discover_bulk_manifest: bool = False,
) -> "FhirResource":
    """Read FHIR content from ``path`` (glob supported).

    * ``BUNDLE`` — JSON **Bundle** resources (one file may contain a full bundle).
    * ``NDJSON`` — newline-delimited JSON resources.
    * ``BULK`` — same NDJSON parsing; optionally read a FHIR Bulk Data **completed export**
      manifest (JSON with ``transactionTime`` and ``output``) to resolve local NDJSON paths
      and set ``bulkExportCorrelationId`` (stable per export). If ``bulk_manifest_path`` is
      set, or ``auto_discover_bulk_manifest`` finds a manifest under the same base directory
      as ``path``, only files listed in the manifest are read; otherwise ``path`` is read as
      NDJSON (same as before).
    """

    if resource_format == FhirFormat.BUNDLE:
        data = spark.read.text(path, wholetext=True).select(col("value").alias("resource"))
        return FhirResource.from_raw_bundle_resource(data)

    if resource_format == FhirFormat.NDJSON:
        data = spark.read.text(path, wholetext=True).select(col("value").alias("resource"))
        return FhirResource.from_raw_ndjson_resource(data)

    if resource_format == FhirFormat.BULK:
        from dbignite.bulk_manifest import discover_bulk_manifest, ndjson_paths_from_manifest

        manifest = bulk_manifest_path
        if manifest is None and auto_discover_bulk_manifest:
            manifest = discover_bulk_manifest(_infer_base_dir(path))
        if manifest:
            correlation_id, ndjson_paths, _ = ndjson_paths_from_manifest(manifest)
            data = (
                spark.read.text(*ndjson_paths, wholetext=True)
                .select(col("value").alias("resource"))
                .withColumn("bulkExportCorrelationId", lit(correlation_id))
            )
            return FhirResource.from_raw_ndjson_resource(data)
        data = spark.read.text(path, wholetext=True).select(col("value").alias("resource"))
        return FhirResource.from_raw_ndjson_resource(data)

    raise ValueError(f"Unrecognized FhirFormat: {resource_format!r}")


class StreamingFhirResource:
    """Structured Streaming wrapper: ``entry()`` returns a **streaming** DataFrame (``isStreaming``)."""

    def __init__(self, raw_df: "DataFrame", resource_format: FhirFormat):
        self._raw_df = raw_df
        self._resource_format = resource_format

    def entry(self, schemas=None):
        from dbignite.fhir_mapping_model import FhirSchemaModel
        from dbignite.fhir_resource import BundleFhirResource

        if schemas is None:
            schemas = FhirSchemaModel()
        if self._resource_format == FhirFormat.BUNDLE:
            br = BundleFhirResource(self._raw_df, streaming=True)
            return br.read_bundle_data(schemas)
        if self._resource_format in (FhirFormat.NDJSON, FhirFormat.BULK):
            br = BundleFhirResource(self._raw_df, parser="read_ndjson_data", streaming=True)
            return br.read_ndjson_data(schemas)
        raise ValueError(f"Unsupported FhirFormat for streaming: {self._resource_format!r}")


def read_from_stream(
    path: str,
    resource_format: FhirFormat = FhirFormat.BUNDLE,
    spark: SparkSession = SparkSession.getActiveSession(),
    max_files_per_trigger: str = "1",
    **read_stream_options,
) -> StreamingFhirResource:
    """Incremental read (Structured Streaming) with ``wholetext`` lines as for batch.

    Returns :class:`StreamingFhirResource`; call ``entry(schemas)`` to build the same logical
    layout as batch ``BundleFhirResource.entry()``, then attach a sink (e.g. Delta) with
    ``writeStream``. Bulk manifest resolution is not applied here—use batch
    ``read_from_directory`` for manifest-driven paths, or preprocess paths yourself.
    """
    rs = spark.readStream
    for k, v in read_stream_options.items():
        rs = rs.option(k, v)
    raw = (
        rs.option("wholetext", "true")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .text(path)
        .select(col("value").alias("resource"))
    )
    if resource_format == FhirFormat.BUNDLE:
        raw = raw.select(col("resource"), get_json_object("resource", "$.resourceType").alias("resourceType")).filter(
            "upper(resourceType) == 'BUNDLE'"
        )
        return StreamingFhirResource(raw, FhirFormat.BUNDLE)
    if resource_format in (FhirFormat.NDJSON, FhirFormat.BULK):
        return StreamingFhirResource(raw, resource_format)
    raise ValueError(f"Unrecognized FhirFormat: {resource_format!r}")
