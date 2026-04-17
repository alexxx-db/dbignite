# Databricks notebook source
# COMPUTE: Serverless

# MAGIC %md
# MAGIC # Rural CIN — Silver Layer: FHIR Normalization with dbignite
# MAGIC
# MAGIC **What this notebook proves:** dbignite transforms nested FHIR R4 bundles into queryable,
# MAGIC typed Delta tables — Patient, Condition, Encounter, Procedure, Observation — without
# MAGIC custom parsers. New hospital onboarding is configuration, not code.
# MAGIC
# MAGIC **The CIN pain point it addresses:** *"Every report we build requires a custom extract
# MAGIC because nobody can query FHIR JSON natively."* multiple rural hospitals x different EHR vendors
# MAGIC (Cerner, Epic, MEDITECH) = an explosion of one-off parsers that break on every EHR upgrade.
# MAGIC
# MAGIC **The defensible business outcome:** One parser (dbignite) handles all FHIR R4 resources
# MAGIC from any EHR. Adding a 15th hospital to the network is a config change to the ingestion
# MAGIC path, not a new parser. Schema is self-documenting in Unity Catalog.
# MAGIC
# MAGIC ---
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demo_cin_catalog", "CIN Catalog")
catalog = dbutils.widgets.get("catalog")
volume_path = f"/Volumes/{catalog}/bronze/raw_feeds"

spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read FHIR Bundles with dbignite
# MAGIC
# MAGIC `read_from_directory` reads JSON FHIR bundles from the Volume and returns a structured
# MAGIC object. `entry()` explodes the bundles into a DataFrame with one column per FHIR resource
# MAGIC type — Patient, Condition, Encounter, etc.

# COMMAND ----------

# DBTITLE 1,Parse FHIR Bundles
from dbignite.readers import read_from_directory
from dbignite.fhir_mapping_model import FhirSchemaModel
from pyspark.sql.functions import col, explode, size

bundle = read_from_directory(f"{volume_path}/fhir_bundles/*json")
df = bundle.entry()

print(f"Parsed {df.count()} FHIR bundles")
print(f"Resource types found: {[c for c in df.columns if c not in ('id','timestamp','bundleUUID')]}")

# COMMAND ----------

# DBTITLE 1,Side-by-side: Raw JSON vs. Typed Schema
# The "aha" moment — show what dbignite does to nested FHIR
print("=== Raw FHIR JSON (first 500 chars) ===")
raw = spark.read.text(f"{volume_path}/fhir_bundles/", wholetext=True)
print(raw.first().value[:500])
print("\n=== Typed Patient schema from dbignite ===")
df.select("Patient").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write Silver Tables
# MAGIC
# MAGIC Each FHIR resource type becomes a typed Delta table in the silver schema.
# MAGIC Liquid clustering on `patient_id` auto-optimizes reads for downstream HEDIS queries.

# COMMAND ----------

# DBTITLE 1,Silver: Patient
patient_df = (
    df.select(col("bundleUUID"), explode("Patient").alias("patient"))
    .select(
        col("bundleUUID"),
        col("patient.id").alias("patient_id"),
        col("patient.gender").alias("gender"),
        col("patient.birthDate").alias("birth_date"),
        col("patient.name").alias("name"),
        col("patient.address").alias("address"),
        col("patient.identifier").alias("identifier"),
    )
)

(
    patient_df.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"`{catalog}`.`silver`.`patient`")
)

# Liquid clustering — auto-optimized data layout, no manual OPTIMIZE needed
spark.sql(f"ALTER TABLE `{catalog}`.`silver`.`patient` CLUSTER BY (`patient_id`)")
spark.sql(f"ALTER TABLE `{catalog}`.`silver`.`patient` ALTER COLUMN `patient_id` SET NOT NULL")

display(patient_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Silver: Condition
condition_df = (
    df.select(col("bundleUUID"), explode("Patient").alias("patient"), col("Condition"))
    .select(
        col("bundleUUID"),
        col("patient.id").alias("patient_id"),
        explode("Condition").alias("condition"),
    )
    .select(
        col("bundleUUID"),
        col("patient_id"),
        col("condition.id").alias("condition_id"),
        col("condition.code.coding").getItem(0).getField("code").alias("condition_code"),
        col("condition.code.coding").getItem(0).getField("system").alias("code_system"),
        col("condition.code.coding").getItem(0).getField("display").alias("condition_display"),
        col("condition.code.text").alias("condition_text"),
        col("condition.clinicalStatus.coding").getItem(0).getField("code").alias("clinical_status"),
        col("condition.onsetDateTime").alias("onset_date"),
        col("condition.abatementDateTime").alias("abatement_date"),
        col("condition.recordedDate").alias("recorded_date"),
    )
)

(
    condition_df.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"`{catalog}`.`silver`.`condition`")
)
spark.sql(f"ALTER TABLE `{catalog}`.`silver`.`condition` CLUSTER BY (`patient_id`)")

print(f"Conditions: {condition_df.count()} rows")
display(condition_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Silver: Encounter
encounter_df = (
    df.select(col("bundleUUID"), explode("Patient").alias("patient"), col("Encounter"))
    .select(
        col("bundleUUID"),
        col("patient.id").alias("patient_id"),
        explode("Encounter").alias("encounter"),
    )
    .select(
        col("bundleUUID"),
        col("patient_id"),
        col("encounter.id").alias("encounter_id"),
        col("encounter.type").getItem(0).getField("coding").getItem(0).getField("code").alias("encounter_code"),
        col("encounter.type").getItem(0).getField("coding").getItem(0).getField("display").alias("encounter_display"),
        col("encounter.period.start").alias("encounter_start"),
        col("encounter.period.end").alias("encounter_end"),
        col("encounter.status").alias("status"),
    )
)

(
    encounter_df.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"`{catalog}`.`silver`.`encounter`")
)
spark.sql(f"ALTER TABLE `{catalog}`.`silver`.`encounter` CLUSTER BY (`patient_id`)")

print(f"Encounters: {encounter_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Silver: Observation (vitals + labs — feeds HEDIS A1C and CBP)
# Observations include vital signs (blood pressure) and lab results (HbA1c)
# which are critical for two HEDIS measures
observation_df = (
    df.select(col("bundleUUID"), explode("Patient").alias("patient"), col("Observation"))
    .filter(col("Observation").isNotNull())
    .select(
        col("bundleUUID"),
        col("patient.id").alias("patient_id"),
        explode("Observation").alias("obs"),
    )
    .select(
        col("bundleUUID"),
        col("patient_id"),
        col("obs.id").alias("observation_id"),
        col("obs.code.coding").getItem(0).getField("code").alias("obs_code"),
        col("obs.code.coding").getItem(0).getField("system").alias("obs_system"),
        col("obs.code.coding").getItem(0).getField("display").alias("obs_display"),
        col("obs.valueQuantity.value").alias("value_numeric"),
        col("obs.valueQuantity.unit").alias("value_unit"),
        col("obs.effectiveDateTime").alias("effective_date"),
        col("obs.issued").alias("issued_date"),
    )
)

(
    observation_df.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"`{catalog}`.`silver`.`observation`")
)
spark.sql(f"ALTER TABLE `{catalog}`.`silver`.`observation` CLUSTER BY (`patient_id`)")

print(f"Observations: {observation_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Silver: Procedure
procedure_df = (
    df.select(col("bundleUUID"), explode("Patient").alias("patient"), col("Procedure"))
    .filter(col("Procedure").isNotNull())
    .select(
        col("bundleUUID"),
        col("patient.id").alias("patient_id"),
        explode("Procedure").alias("proc"),
    )
    .select(
        col("bundleUUID"),
        col("patient_id"),
        col("proc.id").alias("procedure_id"),
        col("proc.code.coding").getItem(0).getField("code").alias("procedure_code"),
        col("proc.code.coding").getItem(0).getField("system").alias("code_system"),
        col("proc.code.coding").getItem(0).getField("display").alias("procedure_display"),
        col("proc.performedPeriod.start").alias("performed_start"),
        col("proc.performedPeriod.end").alias("performed_end"),
    )
)

(
    procedure_df.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"`{catalog}`.`silver`.`procedure_occurrence`")
)
spark.sql(f"ALTER TABLE `{catalog}`.`silver`.`procedure_occurrence` CLUSTER BY (`patient_id`)")

print(f"Procedures: {procedure_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Silver: MedicationRequest
medication_df = (
    df.select(col("bundleUUID"), explode("Patient").alias("patient"), col("MedicationRequest"))
    .filter(col("MedicationRequest").isNotNull())
    .select(
        col("bundleUUID"),
        col("patient.id").alias("patient_id"),
        explode("MedicationRequest").alias("med"),
    )
    .select(
        col("bundleUUID"),
        col("patient_id"),
        col("med.id").alias("medication_request_id"),
        col("med.status").alias("status"),
        col("med.intent").alias("intent"),
        col("med.authoredOn").alias("authored_on"),
    )
)

(
    medication_df.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"`{catalog}`.`silver`.`medication_request`")
)
spark.sql(f"ALTER TABLE `{catalog}`.`silver`.`medication_request` CLUSTER BY (`patient_id`)")

print(f"MedicationRequests: {medication_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. FHIR Round-Trip: Silver → FHIR Bundle
# MAGIC
# MAGIC This proves the Gold→the legacy EHR re-feed story from the proposal: we can take clean,
# MAGIC normalized data and write it back as a valid FHIR Bundle. If a downstream system
# MAGIC (the legacy EHR, a payer portal, a registry) needs FHIR, we produce it from the silver layer
# MAGIC — not by extracting from the messy source.

# COMMAND ----------

# DBTITLE 1,DataFrame → FHIR Bundle (round-trip proof)
from dbignite.writer.fhir_encoder import FhirEncoderManager, Mapping, MappingManager
from dbignite.writer.bundler import Bundle

# Take 5 patients from the silver layer
sample = spark.sql(f"""
    SELECT patient_id, gender, birth_date
    FROM `{catalog}`.`silver`.`patient`
    LIMIT 5
""")

# Map silver columns to FHIR Patient resource paths
maps = [
    Mapping("patient_id", "Patient.id"),
    Mapping("gender", "Patient.gender"),
    Mapping("birth_date", "Patient.birthDate"),
]

m = MappingManager(maps, sample.schema)
b = Bundle(m)
fhir_output = b.df_to_fhir(sample, json_dumps_kwargs={"indent": 2})

print("=== FHIR Bundle output (silver → FHIR round-trip) ===")
for row in fhir_output.limit(2).collect():
    print(row.fhir_bundle)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Summary

# COMMAND ----------

# DBTITLE 1,Table Row Counts
display(spark.sql(f"""
    SELECT 'patient' as table_name, count(*) as row_count FROM `{catalog}`.`silver`.`patient`
    UNION ALL
    SELECT 'condition', count(*) FROM `{catalog}`.`silver`.`condition`
    UNION ALL
    SELECT 'encounter', count(*) FROM `{catalog}`.`silver`.`encounter`
    UNION ALL
    SELECT 'observation', count(*) FROM `{catalog}`.`silver`.`observation`
    UNION ALL
    SELECT 'procedure_occurrence', count(*) FROM `{catalog}`.`silver`.`procedure_occurrence`
    UNION ALL
    SELECT 'medication_request', count(*) FROM `{catalog}`.`silver`.`medication_request`
    ORDER BY table_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **What's next:** [03_silver_mpi_dedup](03_silver_mpi_dedup) — Deterministic patient matching
# MAGIC across hospitals. The single most impactful step for HEDIS denominator accuracy.
# MAGIC
# MAGIC **Cost footprint:** Serverless notebook compute. At production scale (~50K patients,
# MAGIC ~2M clinical records): est. 5-8 DBUs per full refresh, <1 DBU for incremental.
# MAGIC Monthly steady-state: ~30-60 DBUs ($6-12/month at list price).
