import json
import unittest
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from dbignite.fhir_mapping_model import FhirSchemaModel


class TestWritersEncoderLogic(unittest.TestCase):
    """Writer encoder and mapping logic (no Spark cluster / JVM required)."""

    def test_encoder(self):
        from dbignite.writer.fhir_encoder import FhirEncoder

        e = FhirEncoder(False, False, lambda x: int(x.strip()))
        assert e.f("123") == 123
        assert e.f("12a") == ""

    def test_encoder_manager(self):
        from dbignite.writer.fhir_encoder import FhirEncoder, FhirEncoderManager

        em = FhirEncoderManager()
        assert (
            em.get_encoder("string", ["Patient", "multipleBirthInteger"]).f("1234")
            == 1234
        )
        assert (
            em.get_encoder("string", ["Patient", "multipleBirthInteger"]).f("abcdefg")
            == ""
        )

        em = FhirEncoderManager(
            override_encoders={
                "Patient.multipleBirthInteger": FhirEncoder(
                    False, False, lambda x: float(x) + 1
                )
            }
        )
        assert (
            em.get_encoder("string", ["Patient", "multipleBirthInteger"]).f("1234")
            == 1235
        )

    def test_target_datatype(self):
        from dbignite.writer.fhir_encoder import SchemaDataType

        field = "Patient.name.given"
        s = FhirSchemaModel()
        tgt_schema = s.custom_fhir_resource_mapping(
            field.split(".")[0]
        ).schema(field.split(".")[0])
        result = SchemaDataType.traverse_schema(field.split(".")[1:], tgt_schema)
        assert result == "array<string>"

    def test_encoder_strict_propagates_errors(self):
        from dbignite.writer.fhir_encoder import FhirEncoder

        e = FhirEncoder(False, False, lambda x: int(x.strip()), strict=True)
        with self.assertRaises(ValueError):
            e.f("not_an_int")

    def test_encoder_manager_strict_wraps_defaults(self):
        from dbignite.writer.fhir_encoder import FhirEncoderManager

        em = FhirEncoderManager(strict=True)
        with self.assertRaises(ValueError):
            em.get_encoder("string", ["Patient", "multipleBirthInteger"]).f("abcdefg")

    def test_encode_unknown_resource_raises(self):
        from dbignite.writer.fhir_encoder import Mapping, MappingManager

        schema = StructType([StructField("id", StringType())])
        mm = MappingManager([Mapping("id", "Patient.id")], schema)

        with self.assertRaises(ValueError) as ctx:
            mm.encode({"id": "1"}, "Observation")
        self.assertIn("Observation", str(ctx.exception))
        self.assertIn("Patient", str(ctx.exception))

    def test_encode_accepts_plain_dict(self):
        from dbignite.writer.fhir_encoder import Mapping, MappingManager

        schema = StructType([StructField("id", StringType())])
        mm = MappingManager([Mapping("id", "Patient.id")], schema)
        out = mm.encode({"id": "p1"}, "Patient")
        assert out == {"Patient": {"id": "p1"}}

    def test_unknown_fhir_resource_type_in_path_raises(self):
        from dbignite.writer.fhir_encoder import FhirEncoderManager

        em = FhirEncoderManager()
        with self.assertRaises(KeyError) as ctx:
            em.get_encoder("string", ["NotAFhirResourceXyz", "id"])
        self.assertIn("NotAFhirResourceXyz", str(ctx.exception))

    def test_long_encodes_to_string_for_ids(self):
        from dbignite.writer.fhir_encoder import FhirEncoderManager

        em = FhirEncoderManager()
        enc = em.get_encoder(LongType().typeName(), ["Patient", "id"])
        self.assertEqual(enc.f(9223372036854775807), "9223372036854775807")

    def test_bundle_row_to_fhir_json(self):
        from dbignite.writer.bundler import Bundle
        from dbignite.writer.fhir_encoder import Mapping, MappingManager

        schema = StructType([StructField("id", StringType())])
        mm = MappingManager([Mapping("id", "Patient.id")], schema)
        b = Bundle(mm)
        payload = json.loads(b.row_to_fhir_json({"id": "p1"}))
        self.assertEqual(payload["resourceType"], "Bundle")
        self.assertEqual(len(payload["entry"]), 1)
        self.assertEqual(payload["entry"][0]["resource"]["resourceType"], "Patient")
        self.assertEqual(payload["entry"][0]["resource"]["id"], "p1")

    def test_bundle_resource_to_fhir_validation(self):
        from dbignite.writer.bundler import Bundle

        with self.assertRaises(ValueError):
            Bundle._resource_to_fhir({})
        with self.assertRaises(ValueError):
            Bundle._resource_to_fhir({"A": {}, "B": {}})

    def test_mismatched_types_same_fhir_leaf_raises(self):
        from dbignite.writer.fhir_encoder import Mapping, MappingManager

        schema = StructType(
            [
                StructField("A", StringType()),
                StructField("B", IntegerType()),
            ]
        )
        maps = [
            Mapping("A", "Patient.name.given"),
            Mapping("B", "Patient.name.given"),
        ]
        mm = MappingManager(maps, schema)
        leaf = [m for m in mm.mappings if m.tgt == "Patient.name.given"]
        with self.assertRaises(ValueError) as ctx:
            mm.to_fhir(("Patient", "name", "given"), leaf, {"A": "x", "B": 1})
        self.assertIn("same Spark type", str(ctx.exception))

    def test_missing_encoder_raises_keyerror(self):
        from dbignite.writer.fhir_encoder import FhirEncoder, FhirEncoderManager

        em = FhirEncoderManager(
            map={"IDENTITY": FhirEncoder(True, False, lambda x: x)},
        )
        with self.assertRaises(KeyError) as ctx:
            em.get_encoder("boolean", ["Patient", "id"])
        self.assertIn("No FhirEncoder", str(ctx.exception))


from .test_base import PysparkBaseTest


class TestWriters(PysparkBaseTest):
    def test_hardcoded_values(self):
        from dbignite.writer.bundler import Bundle
        from dbignite.writer.fhir_encoder import Mapping, MappingManager

        data = self.spark.createDataFrame(
            [
                ("CLM123", "PAT01", "COH123"),
                ("CLM345", "PAT02", "COH123"),
            ],
            ["CLAIM_ID", "PATIENT_ID", "PATIENT_COHORT_NUM"],
        )
        maps = [
            Mapping("CLAIM_ID", "Claim.id"),
            Mapping("PATIENT_COHORT_NUM", "Patient.identifier.value"),
            Mapping(
                "<url of a hardcoded system reference>",
                "Patient.identifier.system",
                True,
            ),
            Mapping("PATIENT_ID", "Patient.id"),
        ]
        m = MappingManager(maps, data.schema)
        b = Bundle(m)
        import json
        rows = b.df_to_fhir(data).collect()
        bundle = json.loads(rows[0].fhir_bundle)
        # The Patient resource is the second entry (Claim first, Patient second)
        patient = bundle["entry"][1]["resource"]
        assert patient["identifier"][0]["system"] == "<url of a hardcoded system reference>"

    def test_multiple_resources_to_single_value(self):
        from dbignite.writer.bundler import Bundle
        from dbignite.writer.fhir_encoder import Mapping, MappingManager

        data = self.spark.createDataFrame(
            [("CLM123", "Emma", "Maya"), ("CLM345", "E", "J")],
            ["CLAIM_ID", "SECOND_BORN", "FIRST_BORN"],
        )

        maps = [
            Mapping("CLAIM_ID", "Patient.id"),
            Mapping("FIRST_BORN", "Patient.name.given"),
            Mapping("SECOND_BORN", "Patient.name.given"),
        ]

        m = MappingManager(maps, data.schema)
        b = Bundle(m)
        import json
        rows = b.df_to_fhir(data).collect()
        bundle0 = json.loads(rows[0].fhir_bundle)
        bundle1 = json.loads(rows[1].fhir_bundle)
        # Single resource type (Patient), so it's the first entry
        assert bundle0["entry"][0]["resource"]["name"][0]["given"] == [
            "Maya",
            "Emma",
        ]
        assert bundle1["entry"][0]["resource"]["name"][0]["given"] == ["J", "E"]
