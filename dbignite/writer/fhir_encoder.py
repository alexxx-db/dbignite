import logging
from collections import ChainMap
from itertools import groupby
from typing import Any, Optional

from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    StructField,
    StructType,
)

from dbignite.fhir_mapping_model import FhirSchemaModel

logger = logging.getLogger(__name__)


class MappingManager:
    def __init__(self, mappings, src_schema, em=None):
        # itertools.groupby requires consecutive keys; sort for stable, correct grouping.
        self.mappings = sorted(mappings, key=lambda m: m.tgt.split("."))
        self.src_schema = src_schema
        self.em = em if em is not None else FhirEncoderManager()

    def _row_as_dict(self, row):
        if hasattr(row, "asDict"):
            return row.asDict()
        if isinstance(row, dict):
            return row
        raise TypeError(
            f"row must be a Spark Row or dict, got {type(row).__name__}"
        )

    def encode(self, row, fhirResourceType):
        matches = [
            (resourceType, ms)
            for resourceType, ms in self.level(0)
            if resourceType[0] == fhirResourceType
        ]
        if not matches:
            known = sorted({x.fhir_resource() for x in self.mappings})
            raise ValueError(
                f"Unknown FHIR resource type {fhirResourceType!r}; "
                f"mapped resources are: {known}"
            )
        data = matches[0]
        return self.to_fhir(data[0], data[1], self._row_as_dict(row))

    def get_src(self, tgt):
        joined = ".".join(tgt)
        return next((x for x in self.mappings if x.tgt == joined), None)

    def get_func(self, tgt):
        src = self.get_src(tgt)
        src_type = (
            None
            if src is None
            else self._source_spark_type_key(src.src)
        )
        return self.em.get_encoder(
            src_type=src_type,
            tgt_name=tgt,
        )

    def fhir_resource_list(self):
        return sorted({x.fhir_resource() for x in self.mappings})

    def level(self, level, resources=None):
        """
        Group mappings that share the same target path prefix up to ``level``.

        ``level`` is 0-based: 0 is the FHIR resource type (e.g. Patient), 1 is
        the first field under it, etc.

        Returns:
            List of (path_prefix_tuple, list_of_Mapping).
        """
        mappings = resources if resources is not None else self.mappings
        return [
            (k, list(g))
            for k, g in groupby(mappings, lambda x: tuple(x.tgt.split(".")[: level + 1]))
            if len(k) >= level + 1
        ]

    def _source_spark_type_key(self, column_name: str) -> Optional[str]:
        if column_name in self.src_schema.names:
            field = self.src_schema[column_name]
            return SchemaDataType.datatype_to_encoder_key(field.dataType)
        parts = column_name.split(".")
        try:
            out = SchemaDataType.traverse_schema(parts, self.src_schema)
            if isinstance(out, str):
                return out
            return SchemaDataType.datatype_to_encoder_key(out)
        except (KeyError, IndexError, TypeError, ValueError):
            pass
        raise ValueError(
            f"Source column {column_name!r} not found in DataFrame schema; "
            f"known columns: {self.src_schema.names}"
        )

    def to_fhir(self, tgt_prefix, resource_list, row_dict):
        field_name = tgt_prefix[-1]
        if (
            len(resource_list) == 1
            and ".".join(tgt_prefix) == resource_list[0].tgt
        ):
            return {
                field_name: (
                    self.get_func(tgt_prefix).f(
                        row_dict.get(resource_list[0].src)
                    )
                    if not resource_list[0].hardcoded
                    else resource_list[0].src
                )
            }
        if (
            len(resource_list) > 1
            and ".".join(tgt_prefix) == resource_list[0].tgt
        ):
            type_keys = {
                self._source_spark_type_key(x.src) for x in resource_list
            }
            if len(type_keys) > 1:
                raise ValueError(
                    "All source columns mapped to the same FHIR leaf must share "
                    f"the same Spark type; got {type_keys!r} for target "
                    f"{resource_list[0].tgt!r}"
                )
            src_key = self._source_spark_type_key(resource_list[0].src)
            list_src_type = f"array<{src_key}>"
            values = [
                row_dict.get(x.src)
                for x in resource_list
                if row_dict.get(x.src) is not None
            ]
            return {
                field_name: self.em.get_encoder(
                    list_src_type, tgt_prefix
                ).f(values)
            }
        return {
            field_name: self.get_func(tgt_prefix).f(
                [
                    self.to_fhir(prefix, resources, row_dict)
                    for prefix, resources in self.level(
                        len(tgt_prefix), resource_list
                    )
                ]
            )
        }


class Mapping:
    def __init__(self, src, tgt, hardcoded=False):
        self.src = src
        self.tgt = tgt
        self.hardcoded = hardcoded

    def fhir_resource(self):
        return self.tgt.split(".")[0]

    def __str__(self):
        return "src:" + str(self.src) + ", tgt:" + str(self.tgt)


class FhirEncoder:
    def __init__(
        self,
        one_to_one,
        precision_loss,
        f,
        default="",
        strict=False,
    ):
        self.one_to_one = one_to_one
        self.precision_loss = precision_loss
        self.default = default
        self.strict = strict
        self._raw_f = f
        self.f = self.handle(self._raw_f)

    def handle(self, f):
        def wrapper_func(*args, **kw):
            try:
                out = f(*args, **kw)
                return "" if out is None else out
            except Exception:
                if self.strict:
                    raise
                logger.debug(
                    "FhirEncoder suppressed transform error; returning default %r",
                    self.default,
                    exc_info=True,
                )
                return self.default

        return wrapper_func


class FhirEncoderManager:
    """
    Resolves encoders for source Spark types -> target FHIR (schema) types.
    """

    def __init__(
        self,
        map=None,
        override_encoders=None,
        fhir_schema=None,
        strict=False,
    ):
        if override_encoders is None:
            override_encoders = {}
        if fhir_schema is None:
            fhir_schema = FhirSchemaModel()
        self.override_encoders = override_encoders
        self.fhir_schema = fhir_schema
        self.strict = strict
        if map is not None:
            self.map = map
        elif strict:
            self.map = self._clone_encoder_map_with_strict(DEFAULT_ENCODERS, True)
        else:
            self.map = DEFAULT_ENCODERS

    @staticmethod
    def _clone_encoder_map_with_strict(enc_map, strict: bool):
        if isinstance(enc_map, FhirEncoder):
            return FhirEncoder(
                enc_map.one_to_one,
                enc_map.precision_loss,
                enc_map._raw_f,
                enc_map.default,
                strict=strict,
            )
        if isinstance(enc_map, dict):
            return {
                k: FhirEncoderManager._clone_encoder_map_with_strict(v, strict)
                for k, v in enc_map.items()
            }
        return enc_map

    def _maybe_apply_strict(self, enc):
        if (
            self.strict
            and isinstance(enc, FhirEncoder)
            and not enc.strict
        ):
            return FhirEncoder(
                enc.one_to_one,
                enc.precision_loss,
                enc._raw_f,
                enc.default,
                strict=True,
            )
        return enc

    def _get_encoder(self, src_type, tgt_type):
        if src_type == tgt_type:
            enc = self.map.get("IDENTITY")
            return enc if isinstance(enc, FhirEncoder) else None
        branch = self.map.get(src_type, {})
        got = branch.get(tgt_type) if isinstance(branch, dict) else None
        if isinstance(got, FhirEncoder):
            return got
        fallback = self.map.get(tgt_type)
        return fallback if isinstance(fallback, FhirEncoder) else None

    def get_encoder(self, src_type, tgt_name):
        path = ".".join(tgt_name)
        ov = self.override_encoders.get(path)
        if ov is not None:
            return self._maybe_apply_strict(ov)
        try:
            resource_schema = self.fhir_schema.schema(tgt_name[0])
        except KeyError as e:
            keys = sorted(self.fhir_schema.fhir_resource_map.keys())
            preview = ", ".join(keys[:25])
            if len(keys) > 25:
                preview += ", …"
            raise KeyError(
                f"Unknown FHIR resource {tgt_name[0]!r} for path {path!r}. "
                f"Some packaged types: {preview}"
            ) from e
        tgt_raw = SchemaDataType.traverse_schema(tgt_name[1:], resource_schema)
        tgt_type = (
            tgt_raw
            if isinstance(tgt_raw, str)
            else SchemaDataType.datatype_to_encoder_key(tgt_raw)
        )
        enc = self._get_encoder(src_type, tgt_type)
        if enc is None:
            raise KeyError(
                "No FhirEncoder for this source/target type pair. "
                f"src_type={src_type!r}, tgt_path={path!r}, "
                f"resolved_fhir_type={tgt_type!r}. "
                "Add an entry via override_encoders or extend FhirEncoderManager.map."
            )
        return self._maybe_apply_strict(enc)


def _build_default_encoders() -> dict[str, Any]:
    return {
        "IDENTITY": FhirEncoder(True, False, lambda x: x),
        "string": {
            "string": FhirEncoder(True, False, lambda x: x),
            "integer": FhirEncoder(False, False, lambda x: int(x.strip())),
            "float": FhirEncoder(False, False, lambda x: float(x.strip())),
            "double": FhirEncoder(False, False, lambda x: float(x.strip())),
            "bool": FhirEncoder(False, False, lambda x: bool(x.strip())),
            "array<string>": FhirEncoder(False, False, lambda x: [x]),
        },
        "array<string>": {
            "string": FhirEncoder(False, False, lambda x: ",".join(x)),
        },
        "integer": {
            "string": FhirEncoder(False, True, lambda x: str(x)),
        },
        "long": {
            "string": FhirEncoder(False, True, lambda x: str(x)),
        },
        "float": {
            "string": FhirEncoder(False, True, lambda x: str(x)),
        },
        "double": {
            "string": FhirEncoder(False, True, lambda x: str(x)),
        },
        "boolean": {
            "string": FhirEncoder(
                False, True, lambda x: "true" if x else "false"
            ),
        },
        "struct": FhirEncoder(
            False, True, lambda l: dict(ChainMap(*l))
        ),
        "array<struct>": FhirEncoder(
            False, True, lambda l: [dict(ChainMap(*l))]
        ),
    }


DEFAULT_ENCODERS = _build_default_encoders()
FhirEncoderManager.DEFAULT_ENCODERS = DEFAULT_ENCODERS


class SchemaDataType:
    @staticmethod
    def traverse_schema(field: list, struct) -> Any:
        if not field and type(struct) != StructField and type(struct) != StructType:
            return struct.dataType
        if not field and type(struct) == StructField:
            if struct.dataType.typeName() == "array":
                return (
                    "array<" + struct.dataType.elementType.typeName() + ">"
                )
            return struct.dataType.typeName()
        if not field and type(struct) == StructType:
            return "struct"
        if type(struct) == StructType:
            return SchemaDataType.traverse_schema(
                field[1:], struct[field[0]]
            )
        if type(struct) == StructField:
            return SchemaDataType.traverse_schema(field, struct.dataType)
        if type(struct) == ArrayType:
            return SchemaDataType.traverse_schema(field, struct.elementType)

    @staticmethod
    def datatype_to_encoder_key(dt: DataType) -> str:
        """Spark DataType -> key aligned with traverse_schema / DEFAULT_ENCODERS."""
        if isinstance(dt, StructField):
            return SchemaDataType.datatype_to_encoder_key(dt.dataType)
        if isinstance(dt, ArrayType):
            inner = SchemaDataType.datatype_to_encoder_key(dt.elementType)
            return "array<" + inner + ">"
        if isinstance(dt, StructType):
            return "struct"
        if isinstance(dt, MapType):
            return "struct"
        return dt.typeName()

    @staticmethod
    def schema_to_python(schema):
        return schema.simpleString()
