# Writing FHIR Examples

## Using different versions of FHIR

```python
em = FhirEncoderManager(
    fhir_schema=FhirSchemaModel(schema_version="r4"),
)
# Other packaged versions: "r5", "ci-build"
```

## Source data

DataFrames are the source for converting to FHIR (a single table or the result of SQL).

```python
from dbignite.writer.bundler import Bundle
from dbignite.writer.fhir_encoder import FhirEncoderManager, Mapping, MappingManager
import json

data = spark.createDataFrame(
    [
        ("CLM1", "PRCDR11", "PRCDR12", "PRCDR13", "DX11", "DX12", "DX13"),
        ("CLM1", "PRCDR21", "PRCDR22", "PRCDR23", "DX21", "DX22", "DX23"),
    ],
    ["CLAIM_ID", "PRCDR_CD1", "PRCDR_CD2", "PRCDR_CD3", "DX_CD1", "DX_CD2", "DX_CD3"],
)
# Or: data = spark.sql("SELECT ... FROM ...")
```

## Default type encodings

Transforms are driven by `FhirEncoderManager` default maps (see `dbignite.writer.fhir_encoder.DEFAULT_ENCODERS` / `_build_default_encoders`).

Example: `array<string>` to a single FHIR string uses comma join:

```python
FhirEncoder(False, False, lambda x: ",".join(x))
```

Example: several DataFrame columns to the same FHIR leaf (e.g. procedure codes) share one target path; see tests in `tests/test_writers.py`.

## Specifying transformations

For behavior that defaults don’t cover (e.g. one row per coding entry), use `override_encoders` with a target path key:

```python
em = FhirEncoderManager(
    override_encoders={
        "Claim.procedure.procedureCodeableConcept.coding": FhirEncoder(
            False,
            False,
            lambda x: [{"code": y} for y in x[0].get("code").split(",")],
        )
    }
)
```

You can still mutate shared defaults when not using `strict=True`:

```python
em = FhirEncoderManager()
em.DEFAULT_ENCODERS["array<string>"]["string"] = FhirEncoder(
    False, False, lambda x: "\n".join(x)
)
```

## Single-row helpers (no RDD)

`Bundle.row_to_bundle_dict` / `Bundle.row_to_fhir_json` accept a Spark `Row` or a plain `dict` (same column names as the DataFrame):

```python
m = MappingManager(maps, data.schema, em)
b = Bundle(m)
bundle_dict = b.row_to_bundle_dict({"CLAIM_ID": "c1", ...})
json_str = b.row_to_fhir_json({"CLAIM_ID": "c1", ...})
```

`MappingManager.encode` also accepts a `dict` row for tests and small jobs.

## Strict mode

Use `FhirEncoderManager(strict=True)` so encoder failures surface instead of returning the encoder `default` (usually `""`). Non-strict failures are logged at DEBUG on `dbignite.writer.fhir_encoder`.

## Complex extensions

Combine fields in SQL (e.g. `map(...)`) or in a custom `FhirEncoder`, then map to an extension or array path. See historical examples in git history for multi-entry extension patterns.

# Common errors

## No FhirEncoder for src/target pair

`get_encoder` raises **`KeyError`** with `src_type`, `tgt_path`, and resolved FHIR type. Fix by adding `override_encoders` or extending `map` / `DEFAULT_ENCODERS`.

## Unknown FHIR resource type in path

If the first segment of a mapping target (e.g. `Patient` in `Patient.id`) is not in the packaged schema set, **`KeyError`** lists some available resource names.

## Unknown resource when encoding

Calling `encode(row, "Observation")` when mappings only define `Patient` raises **`ValueError`** with the allowed resource types.

## Legacy: `'dict' object has no attribute 'f'`

Older versions could return an inner type map where an encoder was expected. Current code only returns `FhirEncoder` instances; upgrade if you still see this.
