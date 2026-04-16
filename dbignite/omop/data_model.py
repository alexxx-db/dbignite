import logging
from abc import ABC, abstractmethod
from typing import Iterable, Optional, Tuple, Union

from pyspark.sql import DataFrame
from pyspark.sql.catalog import Database
from pyspark.sql.functions import explode, from_json, udf

from dbignite.omop.constants import (
    CONDITION_OCCURRENCE_TABLE,
    PERSON_TABLE,
    PROCEDURE_OCCURRENCE_TABLE,
    SOURCE_TO_CONCEPT_MAP_TABLE,
    VISIT_OCCURRENCE_TABLE,
)
from dbignite.omop.schemas import ENTRY_SCHEMA, SOURCE_TO_CONCEPT_MAP_SCHEMA
from dbignite.omop.utils import (
    entries_to_condition,
    entries_to_person,
    entries_to_procedure_occurrence,
    entries_to_visit_occurrence,
    summarize_condition,
    summarize_encounter,
    summarize_procedure_occurrence,
)
from pyspark.sql.types import ArrayType, StringType as ST

import json


class DataModel(ABC):
    @abstractmethod
    def summary(self) -> DataFrame:
        ...

    @abstractmethod
    def listDatabases(self) -> Iterable[Union[Database, str, None]]:
        ...

    @abstractmethod
    def update(self) -> None:
        ...


class FhirBundles:
    """Load FHIR Bundle JSON (whole file per row) and explode to bundle entries."""

    def __init__(self, defaultResource=None, **args):
        from pyspark.sql import SparkSession

        self.spark = SparkSession.getActiveSession()
        self.df = None
        if defaultResource is None:
            self.defaultResource = self.asWholeTextfile
        else:
            self.defaultResource = defaultResource
        self.args = args

    def loadEntries(self):
        if self.df is None:
            self.df = self.defaultResource(**self.args)
        return self.df

    @staticmethod
    @udf(ArrayType(ST()))
    def _entry_json_strings(value):
        bundle_json = json.loads(value)
        return [json.dumps(e) for e in bundle_json["entry"]]

    def asWholeTextfile(self, path):
        return (
            self.spark.read.text(path, wholetext=True)
            .select(explode(FhirBundles._entry_json_strings("value")).alias("entry_json"))
            .withColumn("entry", from_json("entry_json", schema=ENTRY_SCHEMA))
        ).cache()

    def asInlineJson(self, path):
        raise NotImplementedError("asInlineJson is not implemented.")

    def asInlineJsonSingleton(self, path):
        return (
            self.spark.read.json(
                self.spark.read.json(path).rdd.map(
                    lambda x: json.dumps({"entry_json": json.dumps(x.asDict())})
                )
            )
            .withColumn("entry", from_json("entry_json", schema=ENTRY_SCHEMA))
        ).cache()

    def asStream(self, kwargs):
        raise NotImplementedError("asStream is not implemented.")

    def summary(self) -> DataFrame:
        return self.loadEntries()

    def listDatabases(self):
        raise NotImplementedError("FhirBundles is not bound to a Hive/Delta database.")

    def update(self, path: str) -> None:
        self.path = path


class PersonDashboard(DataModel):
    def __init__(self, df: Optional[DataFrame] = None):
        self.df = df

    def summary(self):
        return self.df

    def listDatabases(self):
        raise NotImplementedError()

    def update(self, df: DataFrame) -> None:
        self.df = df


class OmopCdm(DataModel):
    def __init__(self, cdm_database: str, mapping_database: Optional[str] = None):
        self.cdm_database = cdm_database
        self.mapping_database = mapping_database

    def summary(self) -> DataFrame:
        raise NotImplementedError("Use Spark SQL or read tables from the CDM database.")

    def listDatabases(self) -> Tuple[str, Optional[str]]:
        return (self.cdm_database, self.mapping_database)

    def update(self, cdm_database: str, mapping_database: Optional[str] = None) -> None:
        self.cdm_database = cdm_database
        self.mapping_database = mapping_database


class Transformer(ABC):
    @abstractmethod
    def loadEntries(self) -> DataFrame:
        ...

    @abstractmethod
    def transform(self) -> DataModel:
        ...


def _ensure_mapping_stub(spark, mapping_database: str) -> None:
    """Create an empty staging table for future source→standard concept mappings."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{mapping_database}`")
    empty = spark.createDataFrame([], SOURCE_TO_CONCEPT_MAP_SCHEMA)
    empty.write.format("delta").mode("overwrite").saveAsTable(
        f"{mapping_database}.{SOURCE_TO_CONCEPT_MAP_TABLE}"
    )


class FhirBundlesToCdm(Transformer):
    def __init__(self, spark=None):
        from pyspark.sql import SparkSession

        self.spark = spark if spark is not None else SparkSession.getActiveSession()

    def loadEntries(self):
        pass

    def transform(
        self,
        source: FhirBundles,
        target: OmopCdm,
        overwrite: bool = True,
    ) -> OmopCdm:
        cdm_database = target.cdm_database
        mapping_database = target.mapping_database

        entries_df = source.loadEntries()

        person_df = entries_df.transform(entries_to_person)
        condition_df = entries_df.transform(entries_to_condition)
        procedure_occurrence_df = entries_df.transform(entries_to_procedure_occurrence)
        visit_occurrence_df = entries_df.transform(entries_to_visit_occurrence)

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{cdm_database}`")
        if mapping_database:
            _ensure_mapping_stub(self.spark, mapping_database)

        self.spark.catalog.setCurrentDatabase(cdm_database)

        logging.info("Writing OMOP-aligned tables to database %s", cdm_database)

        mode = "overwrite" if overwrite else "append"
        writer = lambda df, name: df.write.format("delta").mode(mode).saveAsTable(name)

        writer(person_df, PERSON_TABLE)
        writer(condition_df, CONDITION_OCCURRENCE_TABLE)
        writer(procedure_occurrence_df, PROCEDURE_OCCURRENCE_TABLE)
        writer(visit_occurrence_df, VISIT_OCCURRENCE_TABLE)

        logging.info(
            "Created tables: %s, %s, %s, %s in %s",
            PERSON_TABLE,
            CONDITION_OCCURRENCE_TABLE,
            PROCEDURE_OCCURRENCE_TABLE,
            VISIT_OCCURRENCE_TABLE,
            cdm_database,
        )

        target.update(cdm_database, mapping_database)
        return target


class CdmToPersonDashboard(Transformer):
    def __init__(self):
        from pyspark.sql import SparkSession

        self.spark = SparkSession.getActiveSession()

    def loadEntries(self):
        raise NotImplementedError()

    def transform(
        self,
        source: OmopCdm,
        target: PersonDashboard,
        overwrite: bool = True,
    ) -> PersonDashboard:
        cdm_database = source.listDatabases()[0]

        self.spark.sql(f"USE `{cdm_database}`")

        person_df = self.spark.read.table(PERSON_TABLE)
        condition_df = self.spark.read.table(CONDITION_OCCURRENCE_TABLE)
        procedure_occurrence_df = self.spark.read.table(PROCEDURE_OCCURRENCE_TABLE)
        visit_occurrence_df = self.spark.read.table(VISIT_OCCURRENCE_TABLE)

        condition_summary_df = condition_df.transform(summarize_condition)
        procedure_occurrence_summary_df = procedure_occurrence_df.transform(
            summarize_procedure_occurrence
        )
        encounter_summary_df = visit_occurrence_df.transform(summarize_encounter)

        person_dashboard_df = (
            person_df.join(condition_summary_df, "person_id", "left")
            .join(procedure_occurrence_summary_df, "person_id", "left")
            .join(encounter_summary_df, "person_id", "left")
        )
        target.update(person_dashboard_df)
        return target


# Backward-compatible names for existing imports
CONDITION_TABLE = CONDITION_OCCURRENCE_TABLE
ENCOUNTER_TABLE = VISIT_OCCURRENCE_TABLE
