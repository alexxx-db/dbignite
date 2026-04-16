from copy import deepcopy

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    coalesce,
    collect_list,
    col,
    concat_ws,
    dayofmonth,
    from_json,
    lit,
    month,
    regexp_replace,
    struct,
    to_timestamp,
    trim,
    when,
    year,
)
from pyspark.sql.types import (
    ArrayType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dbignite.omop.schemas import ENTRY_SCHEMA


def _null_concept_id() -> Column:
    """Nullable OMOP concept_id column (populate via vocabulary mapping)."""
    return lit(None).cast(LongType())


def _strip_fhir_reference(reference_col: Column) -> Column:
    """Normalize FHIR references (urn:uuid, ResourceType/id) to a bare id string."""
    x = coalesce(reference_col, lit(""))
    x = regexp_replace(x, "^urn:uuid:", "")
    for prefix in ("Patient/", "Encounter/", "Condition/", "Procedure/", "Practitioner/", "Organization/"):
        x = regexp_replace(x, f"^{prefix}", "")
    return trim(x)


def _resource_type_filter(resource_type: str) -> Column:
    """Prefer resource.resourceType; fall back to transaction Bundle request.url (Synthea style)."""
    rt = col("entry.resource.resourceType")
    ru = col("entry.request.url")
    return (rt == resource_type) | (ru == resource_type)


def _parse_resource(
    entries_df: DataFrame,
    fhir_type: str,
    extra_fields: list[StructField],
    alias: str,
) -> DataFrame:
    """Filter entries by *fhir_type*, extend the resource schema with *extra_fields*,
    and return a DataFrame with the parsed resource as column *alias*."""
    entry_schema = deepcopy(ENTRY_SCHEMA)
    resource_schema = next(f.dataType for f in entry_schema.fields if f.name == "resource")
    resource_schema.fields.extend(extra_fields)
    return (
        entries_df.where(_resource_type_filter(fhir_type))
        .withColumn(alias, from_json("entry_json", schema=entry_schema)["resource"])
    )


def entries_to_person(entries_df: DataFrame) -> DataFrame:
    df = _parse_resource(entries_df, "Patient", [
        StructField(
            "name",
            ArrayType(
                StructType(
                    [
                        StructField("given", ArrayType(StringType())),
                        StructField("family", StringType()),
                        StructField("text", StringType()),
                    ]
                )
            ),
        ),
        StructField("gender", StringType()),
        StructField("birthDate", DateType()),
        StructField(
            "address",
            ArrayType(
                StructType(
                    [
                        StructField("line", ArrayType(StringType())),
                        StructField("city", StringType()),
                        StructField("state", StringType()),
                    ]
                )
            ),
        ),
        StructField(
            "extension", ArrayType(StructType([StructField("url", StringType())]))
        ),
    ], "patient")
    given0 = col("patient.name").getItem(0)
    person_name = coalesce(
        concat_ws(" ", given0.getField("given").getItem(0), given0.getField("family")),
        given0.getField("text"),
        lit(""),
    )
    addr0 = col("patient.address").getItem(0)
    address_src = concat_ws(", ", addr0.getField("line"))

    return df.select(
        col("patient.id").alias("person_id"),
        person_name.alias("person_name_source_value"),
        col("patient.gender").alias("gender_source_value"),
        year(col("patient.birthDate")).alias("year_of_birth"),
        month(col("patient.birthDate")).alias("month_of_birth"),
        dayofmonth(col("patient.birthDate")).alias("day_of_birth"),
        to_timestamp(col("patient.birthDate")).alias("birth_datetime"),
        col("patient.id").alias("person_source_value"),
        _null_concept_id().alias("gender_concept_id"),
        _null_concept_id().alias("race_concept_id"),
        _null_concept_id().alias("ethnicity_concept_id"),
        _null_concept_id().alias("location_id"),
        coalesce(address_src, lit("")).alias("address_source_value"),
    )


def entries_to_condition(entries_df: DataFrame) -> DataFrame:
    df = _parse_resource(entries_df, "Condition", [
        StructField(
            "subject", StructType([StructField("reference", StringType())])
        ),
        StructField(
            "encounter", StructType([StructField("reference", StringType())])
        ),
        StructField(
            "code",
            StructType(
                [
                    StructField(
                        "coding",
                        ArrayType(
                            StructType(
                                [
                                    StructField("code", StringType()),
                                    StructField("display", StringType()),
                                    StructField("system", StringType()),
                                ]
                            )
                        ),
                    ),
                    StructField("text", StringType()),
                ]
            ),
        ),
        StructField(
            "clinicalStatus",
            StructType(
                [
                    StructField(
                        "coding",
                        ArrayType(
                            StructType(
                                [
                                    StructField("code", StringType()),
                                    StructField("display", StringType()),
                                ]
                            )
                        ),
                    )
                ]
            ),
        ),
        StructField("onsetDateTime", TimestampType()),
        StructField("abatementDateTime", TimestampType()),
    ], "condition")
    c = col("condition")
    coding0 = c.getField("code").getField("coding").getItem(0)
    cs = c.getField("clinicalStatus")
    clin0 = cs.getField("coding").getItem(0)
    condition_status_src = when(
        cs.isNotNull(),
        coalesce(clin0.getField("code"), clin0.getField("display")),
    ).otherwise(lit(None).cast(StringType()))
    return df.select(
        c.getField("id").alias("condition_occurrence_id"),
        _strip_fhir_reference(c.getField("subject").getField("reference")).alias("person_id"),
        _strip_fhir_reference(c.getField("encounter").getField("reference")).alias("visit_occurrence_id"),
        c.getField("onsetDateTime").alias("condition_start_datetime"),
        c.getField("abatementDateTime").alias("condition_end_datetime"),
        _null_concept_id().alias("condition_concept_id"),
        _null_concept_id().alias("condition_type_concept_id"),
        _null_concept_id().alias("condition_status_concept_id"),
        coalesce(coding0.getField("code"), c.getField("code").getField("text")).alias("condition_source_value"),
        _null_concept_id().alias("condition_source_concept_id"),
        condition_status_src.alias("condition_status_source_value"),
    )


def entries_to_procedure_occurrence(entries_df: DataFrame) -> DataFrame:
    df = _parse_resource(entries_df, "Procedure", [
        StructField(
            "subject", StructType([StructField("reference", StringType())])
        ),
        StructField(
            "encounter", StructType([StructField("reference", StringType())])
        ),
        StructField(
            "code",
            StructType(
                [
                    StructField(
                        "coding",
                        ArrayType(
                            StructType(
                                [
                                    StructField("code", StringType()),
                                    StructField("display", StringType()),
                                    StructField("system", StringType()),
                                ]
                            )
                        ),
                    ),
                    StructField("text", StringType()),
                ]
            ),
        ),
        StructField(
            "performedPeriod",
            StructType(
                [
                    StructField("start", TimestampType()),
                    StructField("end", TimestampType()),
                ]
            ),
        ),
        StructField(
            "location",
            StructType(
                [
                    StructField("reference", StringType()),
                    StructField("display", StringType()),
                ]
            ),
        ),
    ], "procedure_occurrence")
    p = col("procedure_occurrence")
    coding0 = p.getField("code").getField("coding").getItem(0)
    return df.select(
        p.getField("id").alias("procedure_occurrence_id"),
        _strip_fhir_reference(p.getField("subject").getField("reference")).alias("person_id"),
        _null_concept_id().alias("procedure_concept_id"),
        _null_concept_id().alias("procedure_type_concept_id"),
        coding0.getField("code").alias("procedure_code"),
        coding0.getField("display").alias("procedure_code_display"),
        coding0.getField("system").alias("procedure_code_system"),
        p.getField("performedPeriod").getField("start").cast("date").alias("procedure_start_date"),
        p.getField("performedPeriod").getField("end").cast("date").alias("procedure_end_date"),
        lit(None).cast(StringType()).alias("procedure_type"),
        lit(None).cast(StringType()).alias("provider_id"),
        _strip_fhir_reference(p.getField("encounter").getField("reference")).alias("visit_occurrence_id"),
        p.getField("location").getField("reference").alias("location_id"),
        p.getField("location").getField("display").alias("location_display"),
    )


def entries_to_visit_occurrence(entries_df: DataFrame) -> DataFrame:
    """Map FHIR Encounter → OMOP visit_occurrence-shaped row (subset of CDM columns)."""
    df = _parse_resource(entries_df, "Encounter", [
        StructField(
            "subject", StructType([StructField("reference", StringType())])
        ),
        StructField(
            "period",
            StructType(
                [
                    StructField("start", TimestampType()),
                    StructField("end", TimestampType()),
                ]
            ),
        ),
        StructField(
            "serviceProvider",
            StructType(
                [
                    StructField("reference", StringType()),
                    StructField("display", StringType()),
                ]
            ),
        ),
        StructField(
            "type",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "coding",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("code", StringType()),
                                        StructField("display", StringType()),
                                        StructField("system", StringType()),
                                    ]
                                )
                            ),
                        ),
                        StructField("text", StringType()),
                    ]
                )
            ),
        ),
        StructField(
            "participant",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "type",
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            "coding",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField("code", StringType()),
                                                        StructField("display", StringType()),
                                                        StructField("system", StringType()),
                                                    ]
                                                )
                                            ),
                                        ),
                                        StructField("text", StringType()),
                                    ]
                                )
                            ),
                        )
                    ]
                )
            ),
        ),
        StructField("status", StringType()),
        StructField(
            "identifier",
            ArrayType(
                StructType(
                    [
                        StructField("use", StringType()),
                        StructField("system", StringType()),
                        StructField("value", StringType()),
                    ]
                )
            ),
        ),
        StructField(
            "location",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "location",
                            StructType(
                                [
                                    StructField("reference", StringType()),
                                    StructField("display", StringType()),
                                ]
                            ),
                        )
                    ]
                )
            ),
        ),
    ], "encounter")
    e = col("encounter")
    type0 = e.getField("type").getItem(0)
    coding0 = type0.getField("coding").getItem(0)
    return df.select(
        e.getField("id").alias("visit_occurrence_id"),
        _strip_fhir_reference(e.getField("subject").getField("reference")).alias("person_id"),
        _null_concept_id().alias("visit_concept_id"),
        _null_concept_id().alias("visit_type_concept_id"),
        e.getField("period").getField("start").alias("visit_start_datetime"),
        e.getField("period").getField("end").alias("visit_end_datetime"),
        coalesce(coding0.getField("code"), type0.getField("text")).alias("visit_source_value"),
        e.getField("serviceProvider").getField("display").alias("service_provider_source_value"),
        coding0.getField("display").alias("encounter_status_display"),
        coding0.getField("code").alias("encounter_code"),
        type0.getField("text").alias("encounter_status_text"),
        e.getField("participant").alias("participant"),
        e.getField("status").alias("status"),
        e.getField("identifier").alias("identifier"),
        e.getField("location").alias("location"),
    )


def entries_to_encounter(entries_df: DataFrame) -> DataFrame:
    """Backward-compatible alias for :func:`entries_to_visit_occurrence`."""
    return entries_to_visit_occurrence(entries_df)


def summarize_condition(condition_df: DataFrame) -> DataFrame:
    return (
        condition_df.orderBy("condition_start_datetime")
        .select(col("person_id"), struct("*").alias("condition"))
        .groupBy("person_id")
        .agg(collect_list("condition").alias("conditions"))
    )


def summarize_procedure_occurrence(procedure_occurrence_df: DataFrame) -> DataFrame:
    return (
        procedure_occurrence_df.orderBy("procedure_start_date")
        .select(col("person_id"), struct("*").alias("procedure_occurrence"))
        .groupBy("person_id")
        .agg(collect_list("procedure_occurrence").alias("procedure_occurrences"))
    )


def summarize_encounter(encounter_df: DataFrame) -> DataFrame:
    return (
        encounter_df.orderBy("visit_start_datetime")
        .select(col("person_id"), struct("*").alias("encounter"))
        .groupBy("person_id")
        .agg(collect_list("encounter").alias("encounters"))
    )


def summarize_visit_occurrence(visit_df: DataFrame) -> DataFrame:
    return summarize_encounter(visit_df)
