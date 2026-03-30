"""OMOP CDM-aligned table names (subset used by dbignite FHIR → OMOP transforms)."""

# Core clinical tables (OHDSI OMOP CDM naming)
PERSON_TABLE = "person"
CONDITION_OCCURRENCE_TABLE = "condition_occurrence"
PROCEDURE_OCCURRENCE_TABLE = "procedure_occurrence"
VISIT_OCCURRENCE_TABLE = "visit_occurrence"

# Optional vocabulary / mapping staging (populated when mapping_database is set)
SOURCE_TO_CONCEPT_MAP_TABLE = "source_to_concept_map"

# Backward-compatible aliases (deprecated — prefer *_TABLE constants above)
CONDITION_TABLE = CONDITION_OCCURRENCE_TABLE
ENCOUNTER_TABLE = VISIT_OCCURRENCE_TABLE
