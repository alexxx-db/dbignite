from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
    IntegerType,
)

ENTRY_SCHEMA = StructType(
    [
        StructField(
            "resource",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("resourceType", StringType()),
                ]
            ),
        ),
        StructField(
            "request",
            StructType(
                [
                    StructField("url", StringType()),
                ]
            ),
        ),
    ]
)

JSON_ENTRY_SCHEMA = StructType(
    [StructField("entry_json", StringType()), StructField("entry", ENTRY_SCHEMA)]
)

PERSON_SCHEMA = StructType(
    [
        StructField("person_id", StringType()),
        StructField("person_name_source_value", StringType()),
        StructField("gender_source_value", StringType()),
        StructField("year_of_birth", IntegerType()),
        StructField("month_of_birth", IntegerType()),
        StructField("day_of_birth", IntegerType()),
        StructField("birth_datetime", TimestampType()),
        StructField("person_source_value", StringType()),
        StructField("gender_concept_id", LongType()),
        StructField("race_concept_id", LongType()),
        StructField("ethnicity_concept_id", LongType()),
        StructField("location_id", LongType()),
        StructField("address_source_value", StringType()),
    ]
)

CONDITION_OCCURRENCE_SCHEMA = StructType(
    [
        StructField("condition_occurrence_id", StringType()),
        StructField("person_id", StringType()),
        StructField("visit_occurrence_id", StringType()),
        StructField("condition_start_datetime", TimestampType()),
        StructField("condition_end_datetime", TimestampType()),
        StructField("condition_concept_id", LongType()),
        StructField("condition_type_concept_id", LongType()),
        StructField("condition_status_concept_id", LongType()),
        StructField("condition_source_value", StringType()),
        StructField("condition_source_concept_id", LongType()),
        StructField("condition_status_source_value", StringType()),
    ]
)

# Deprecated name for tests referencing the old constant
CONDITION_SCHEMA = CONDITION_OCCURRENCE_SCHEMA

PROCEDURE_OCCURRENCE_SCHEMA = StructType(
    [
        StructField("procedure_occurrence_id", StringType()),
        StructField("person_id", StringType()),
        StructField("procedure_concept_id", LongType()),
        StructField("procedure_type_concept_id", LongType()),
        StructField("procedure_code", StringType()),
        StructField("procedure_code_display", StringType()),
        StructField("procedure_code_system", StringType()),
        StructField("procedure_start_date", DateType()),
        StructField("procedure_end_date", DateType()),
        StructField("procedure_type", StringType()),
        StructField("provider_id", StringType()),
        StructField("visit_occurrence_id", StringType()),
        StructField("location_id", StringType()),
        StructField("location_display", StringType()),
    ]
)

PARTICIPANT_SCHEMA = StructType(
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

IDENTIFIER_SCHEMA = StructType(
    [
        StructField("use", StringType()),
        StructField("system", StringType()),
        StructField("value", StringType()),
    ]
)

LOCATION_SCHEMA = StructType(
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

VISIT_OCCURRENCE_SCHEMA = StructType(
    [
        StructField("visit_occurrence_id", StringType()),
        StructField("person_id", StringType()),
        StructField("visit_concept_id", LongType()),
        StructField("visit_type_concept_id", LongType()),
        StructField("visit_start_datetime", TimestampType()),
        StructField("visit_end_datetime", TimestampType()),
        StructField("visit_source_value", StringType()),
        StructField("service_provider_source_value", StringType()),
        StructField("encounter_status_display", StringType()),
        StructField("encounter_code", StringType()),
        StructField("encounter_status_text", StringType()),
        StructField("participant", ArrayType(PARTICIPANT_SCHEMA)),
        StructField("status", StringType()),
        StructField("identifier", ArrayType(IDENTIFIER_SCHEMA)),
        StructField("location", ArrayType(LOCATION_SCHEMA)),
    ]
)

# Deprecated alias
ENCOUNTER_SCHEMA = VISIT_OCCURRENCE_SCHEMA

CODING_SCHEMA = StructType(
    [
        StructField("code", StringType()),
        StructField("display", StringType()),
        StructField("system", StringType()),
    ]
)

CONDITION_SUMMARY_SCHEMA = StructType(
    [StructField("conditions", ArrayType(CONDITION_OCCURRENCE_SCHEMA, True))]
)

SOURCE_TO_CONCEPT_MAP_SCHEMA = StructType(
    [
        StructField("source_code", StringType()),
        StructField("source_vocabulary_id", StringType()),
        StructField("source_concept_id", LongType()),
        StructField("target_concept_id", LongType()),
    ]
)
