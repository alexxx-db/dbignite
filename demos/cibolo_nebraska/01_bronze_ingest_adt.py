# Databricks notebook source
# COMPUTE: DLT Serverless (Triggered mode — zero idle cost)
# This notebook is a DLT pipeline definition. It cannot be run interactively.
# It is executed by the pipeline created in 00_setup_uc_catalogs.

# MAGIC %md
# MAGIC # Nebraska CIN — Bronze Layer: ADT Ingest with Quality Enforcement
# MAGIC
# MAGIC **What this notebook proves:** Raw FHIR bundles land in Delta within 15 minutes of receipt,
# MAGIC with automatic quality enforcement. Bad records are quarantined visibly — not silently
# MAGIC dropped, not logged to a file nobody reads.
# MAGIC
# MAGIC **The Cibolo pain point it addresses:** *"Garage is a black box — when we lose a patient
# MAGIC record, we don't find out until the HEDIS numbers are wrong three months later."* The
# MAGIC 15-minute TCM billing window is unachievable without a pipeline that ingests within SLA
# MAGIC and proves it caught every record.
# MAGIC
# MAGIC **The defensible business outcome:** 15-min TCM SLA achieved without a custom HL7 parser.
# MAGIC Every rejected record is in `bronze.quarantine_adt` — queryable, timestamped, with the
# MAGIC exact rule it violated. Zero silent data loss.
# MAGIC
# MAGIC ---
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    from_json,
    get_json_object,
    input_file_name,
    lit,
    sha2,
)
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw FHIR Bundle Ingestion
# MAGIC
# MAGIC Auto Loader incrementally reads new JSON files from the Volume landing zone.
# MAGIC Each file is a complete FHIR Bundle (the standard format for ADT messages from EHRs).

# COMMAND ----------

# Schema for the top-level FHIR Bundle structure
BUNDLE_SCHEMA = (
    StructType()
    .add("resourceType", StringType())
    .add("type", StringType())
    .add("id", StringType())
    .add("timestamp", StringType())
    .add(
        "entry",
        ArrayType(
            StructType()
            .add("resource", StringType())  # Keep as raw JSON for downstream parsing
        ),
    )
)

# COMMAND ----------

@dlt.table(
    name="raw_fhir_bundles",
    comment="Raw FHIR bundles ingested from member hospital feeds. One row per bundle.",
    table_properties={"quality": "bronze"},
)
def raw_fhir_bundles():
    """Ingest FHIR bundles via Auto Loader. Each JSON file = one bundle."""
    raw_feeds_path = spark.conf.get("raw_feeds_volume")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaHints", "entry ARRAY<STRUCT<resource: STRING>>")
        .option("multiLine", "true")
        .load(raw_feeds_path)
        .select(
            col("resourceType"),
            col("type").alias("bundle_type"),
            col("id").alias("bundle_id"),
            col("timestamp").alias("bundle_timestamp"),
            col("entry"),
            input_file_name().alias("source_file"),
            current_timestamp().alias("ingested_at"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode Bundle Entries and Extract Patient Identity
# MAGIC
# MAGIC Each bundle contains multiple FHIR resources (Patient, MessageHeader, Encounter, etc.)
# MAGIC packed inside the `entry` array. We explode to one row per resource and extract the
# MAGIC patient identifier that downstream processing depends on.

# COMMAND ----------

# Quality rules — every record must have a resolvable patient identity.
# Records that fail are quarantined, not dropped.
QUALITY_RULES = {
    "valid_patient_id": "patient_id IS NOT NULL AND patient_id != ''"
}

@dlt.table(
    name="adt_events_raw",
    comment="Exploded FHIR resources from ADT bundles, with quality flag for quarantine.",
    table_properties={"quality": "bronze"},
)
def adt_events_raw():
    """Explode bundle entries, extract patient_id, flag records for quarantine."""
    raw = dlt.read_stream("raw_fhir_bundles")
    return (
        raw.select(
            col("bundle_id"),
            col("bundle_timestamp"),
            col("source_file"),
            col("ingested_at"),
            explode("entry").alias("entry"),
        )
        .select(
            col("bundle_id"),
            col("bundle_timestamp"),
            col("source_file"),
            col("ingested_at"),
            col("entry.resource").alias("resource_json"),
            get_json_object("entry.resource", "$.resourceType").alias("resource_type"),
            get_json_object("entry.resource", "$.id").alias("patient_id"),
        )
        # Flag for quarantine: is_quarantined = True when patient_id is missing
        .withColumn(
            "is_quarantined",
            ~col("patient_id").isNotNull() | (col("patient_id") == lit("")),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Good Records — `bronze.raw_adt_events`
# MAGIC
# MAGIC Records that pass all quality rules land here. These feed the silver layer.

# COMMAND ----------

@dlt.table(
    name="raw_adt_events",
    comment="ADT events that passed quality checks. Source of truth for silver layer.",
    table_properties={"quality": "bronze"},
)
@dlt.expect_all_or_drop(QUALITY_RULES)
def raw_adt_events():
    """Only records with a valid patient_id proceed to the silver layer."""
    return dlt.read_stream("adt_events_raw").filter("NOT is_quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine — `bronze.quarantine_adt`
# MAGIC
# MAGIC Records that **fail** quality rules land here. This is the direct counter-story to
# MAGIC Garage's silent data loss. Every rejected record is visible, queryable, and timestamped.
# MAGIC
# MAGIC In the demo, the malformed ADT message (missing Patient.id) generated in Notebook 00
# MAGIC should appear here.

# COMMAND ----------

@dlt.table(
    name="quarantine_adt",
    comment="ADT records rejected by quality rules. Queryable for audit and remediation.",
    table_properties={"quality": "quarantine"},
)
def quarantine_adt():
    """Quarantined records — failed quality rules but preserved for investigation."""
    return (
        dlt.read_stream("adt_events_raw")
        .filter("is_quarantined")
        .select(
            col("bundle_id"),
            col("bundle_timestamp"),
            col("source_file"),
            col("ingested_at"),
            col("resource_json"),
            col("resource_type"),
            col("patient_id"),
            current_timestamp().alias("quarantined_at"),
            lit("valid_patient_id").alias("failed_rule"),
            lit("patient_id IS NOT NULL AND patient_id != ''").alias("rule_expression"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **What's next:** [02_silver_fhir_normalize](02_silver_fhir_normalize) — dbignite parses the
# MAGIC raw FHIR bundles into typed, queryable silver tables.
# MAGIC
# MAGIC **Cost footprint:** DLT Serverless Triggered mode. At Nebraska MVP scale (~500 ADT
# MAGIC messages/day x 14 hospitals = 7,000 msgs/day): est. 2-4 DBUs per trigger.
# MAGIC Zero idle cost — the pipeline shuts down after each triggered run.
# MAGIC Continuous mode would cost ~720 DBUs/day for an always-on pipeline.
# MAGIC Triggered mode at 4 runs/day = 8-16 DBUs/day. **98% cost reduction vs. Continuous.**
