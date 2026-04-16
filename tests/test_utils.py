import os
import re

import pytest
from chispa.schema_comparer import assert_schema_equality
from shutil import rmtree

from dbignite.omop.constants import (
    CONDITION_OCCURRENCE_TABLE,
    PERSON_TABLE,
    PROCEDURE_OCCURRENCE_TABLE,
    VISIT_OCCURRENCE_TABLE,
)
from dbignite.omop.data_model import (
    CdmToPersonDashboard,
    FhirBundles,
    FhirBundlesToCdm,
    OmopCdm,
    PersonDashboard,
)
from dbignite.omop.schemas import (
    CONDITION_OCCURRENCE_SCHEMA,
    CONDITION_SUMMARY_SCHEMA,
    JSON_ENTRY_SCHEMA,
    PERSON_SCHEMA,
    PROCEDURE_OCCURRENCE_SCHEMA,
    VISIT_OCCURRENCE_SCHEMA,
)
from dbignite.omop.utils import (
    entries_to_condition,
    entries_to_person,
    entries_to_procedure_occurrence,
    entries_to_visit_occurrence,
)

REPO = os.environ.get("REPO", "dbignite")
BRANCH = re.sub(r"\W+", "", os.environ.get("BRANCH", "local_test"))

TEST_BUNDLE_PATH = "./sampledata/*json"
TEST_DATABASE = f"test_{REPO}_{BRANCH}"


@pytest.fixture
def get_entries_df(spark_session):
    return FhirBundles(path=TEST_BUNDLE_PATH).loadEntries()


@pytest.fixture
def fhir_model():
    return FhirBundles(path=TEST_BUNDLE_PATH)


@pytest.fixture
def cdm_model():
    return OmopCdm(TEST_DATABASE)


class TestUtils:
    def test_setup(self):
        rmtree("./spark-warehouse/", ignore_errors=True)

    def test_entries_to_person(self, get_entries_df) -> None:
        person_df = entries_to_person(get_entries_df)
        assert person_df.count() == 3
        assert_schema_equality(person_df.schema, PERSON_SCHEMA, ignore_nullable=True)

    def test_entries_to_condition(self, get_entries_df) -> None:
        condition_df = entries_to_condition(get_entries_df)
        assert condition_df.count() == 103
        assert_schema_equality(
            condition_df.schema, CONDITION_OCCURRENCE_SCHEMA, ignore_nullable=True
        )

    def test_entries_to_procedure_occurrence(self, get_entries_df) -> None:
        procedure_occurrence_df = entries_to_procedure_occurrence(get_entries_df)
        assert procedure_occurrence_df.count() == 119
        assert_schema_equality(
            procedure_occurrence_df.schema,
            PROCEDURE_OCCURRENCE_SCHEMA,
            ignore_nullable=True,
        )

    def test_entries_to_visit_occurrence(self, get_entries_df) -> None:
        visit_df = entries_to_visit_occurrence(get_entries_df)
        assert visit_df.count() == 128
        assert_schema_equality(
            visit_df.schema, VISIT_OCCURRENCE_SCHEMA, ignore_nullable=True
        )

class TestTransformers:
    def test_loadEntries(self, get_entries_df) -> None:
        assert get_entries_df.count() == 1872
        assert_schema_equality(
            get_entries_df.schema, JSON_ENTRY_SCHEMA, ignore_nullable=True
        )

    def test_fhir_bundles_to_omop_cdm(self, spark_session, fhir_model, cdm_model) -> None:
        FhirBundlesToCdm().transform(fhir_model, cdm_model, True)
        tables = [
            t.tableName
            for t in spark_session.sql(f"SHOW TABLES FROM {TEST_DATABASE}").collect()
        ]

        assert cdm_model.listDatabases()[0] == TEST_DATABASE
        assert PERSON_TABLE in tables
        assert CONDITION_OCCURRENCE_TABLE in tables
        assert PROCEDURE_OCCURRENCE_TABLE in tables
        assert VISIT_OCCURRENCE_TABLE in tables

        assert spark_session.table(f"{TEST_DATABASE}.person").count() == 3

    def test_omop_cdm_to_person_dashboard(self, spark_session, fhir_model, cdm_model) -> None:
        person_dash_model = PersonDashboard()

        FhirBundlesToCdm().transform(fhir_model, cdm_model, True)
        CdmToPersonDashboard().transform(cdm_model, person_dash_model)
        person_dashboard_df = person_dash_model.summary()
        assert_schema_equality(
            CONDITION_SUMMARY_SCHEMA,
            person_dashboard_df.select("conditions").schema,
            ignore_nullable=True,
        )
