import json
from typing import Any, Optional


class Bundle:
    """Build FHIR ``Bundle`` JSON (one entry per mapped resource type per row)."""

    def __init__(self, mm):
        self.mm = mm

    def df_to_fhir(self, df, json_dumps_kwargs: Optional[dict[str, Any]] = None):
        """Encode each row of *df* to a FHIR Bundle JSON string.

        Returns a :class:`~pyspark.sql.DataFrame` with a single ``fhir_bundle``
        string column.  This uses a PySpark UDF internally and is compatible with
        serverless compute (no RDD operations).
        """
        from pyspark.sql.functions import col, struct, udf
        from pyspark.sql.types import StringType

        dumps_kw = {"ensure_ascii": False, "separators": (",", ":")}
        if json_dumps_kwargs:
            dumps_kw.update(json_dumps_kwargs)

        # Capture for closure — avoid serializing `self` in the UDF.
        mm = self.mm
        resource_to_fhir = self._resource_to_fhir

        def _encode_row(row):
            resource_types = mm.fhir_resource_list()
            entries = [
                resource_to_fhir(mm.encode(row, rt))
                for rt in resource_types
            ]
            return json.dumps(
                {"resourceType": "Bundle", "entry": entries}, **dumps_kw
            )

        encode_udf = udf(_encode_row, StringType())
        all_cols = [col(c) for c in df.columns]
        return df.select(encode_udf(struct(*all_cols)).alias("fhir_bundle"))

    def row_to_bundle_dict(self, row: Any) -> dict[str, Any]:
        """Encode a single row to a FHIR Bundle as a Python dict (no JSON string)."""
        resource_types = self.mm.fhir_resource_list()
        entries = [
            self._resource_to_fhir(self.mm.encode(row, rt))
            for rt in resource_types
        ]
        return {"resourceType": "Bundle", "entry": entries}

    def row_to_fhir_json(
        self,
        row: Any,
        json_dumps_kwargs: Optional[dict[str, Any]] = None,
    ) -> str:
        """Encode a single row to a FHIR Bundle JSON string."""
        dumps_kw = {"ensure_ascii": False, "separators": (",", ":")}
        if json_dumps_kwargs:
            dumps_kw.update(json_dumps_kwargs)
        return json.dumps(self.row_to_bundle_dict(row), **dumps_kw)

    @staticmethod
    def _resource_to_fhir(resource: dict) -> dict:
        if not resource:
            raise ValueError("Encoded resource dict is empty")
        if len(resource) != 1:
            raise ValueError(
                f"Expected exactly one resource type key per row; got {list(resource)!r}"
            )
        rtype, body = next(iter(resource.items()))
        return {"resource": {"resourceType": rtype, **body}}
