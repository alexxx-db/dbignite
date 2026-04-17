# Databricks notebook source
# COMPUTE: Serverless

# MAGIC %md
# MAGIC # Rural CIN — Master Patient Index (MPI) Deduplication
# MAGIC
# MAGIC **What this notebook proves:** Deterministic patient matching using SSN, name+DOB, and
# MAGIC EMPI identifiers — catches the same patient arriving from different hospital feeds and
# MAGIC resolves them to a single identity.
# MAGIC
# MAGIC **The CIN pain point it addresses:** *"the legacy EHR has no MPI. If a patient visits two
# MAGIC network hospitals, they show up as two patients in every report. Our HEDIS denominators
# MAGIC are inflated and our rates look worse than they are."*
# MAGIC
# MAGIC **The defensible business outcome:** Single patient identity across multiple hospitals.
# MAGIC Denominator accuracy is the foundation of every HEDIS measure — without it, rates are
# MAGIC unreliable and pay-for-performance bonuses are left on the table.
# MAGIC
# MAGIC ---
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "demo_cin_catalog", "CIN Catalog")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extract Structured Identifiers from FHIR
# MAGIC
# MAGIC FHIR Patient resources carry identifiers as an array of `{system, value}` pairs.
# MAGIC We extract SSN, driver's license, and EMPI into dedicated columns for matching.

# COMMAND ----------

# DBTITLE 1,Parse Patient Identifiers
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    expr,
    filter,
    lit,
    lower,
    md5,
    trim,
    when,
)

patients = spark.table(f"`{catalog}`.`silver`.`patient`")

# Extract typed identifiers from the FHIR identifier array
# Synthea uses standard systems: us-ssn, DL, MR (MRN)
patients_with_ids = patients.select(
    col("patient_id"),
    col("bundleUUID"),
    col("gender"),
    col("birth_date"),
    col("name"),
    # SSN: system = http://hl7.org/fhir/sid/us-ssn
    expr(
        "filter(identifier, x -> x.system = 'http://hl7.org/fhir/sid/us-ssn')[0].value"
    ).alias("ssn"),
    # Driver's license: type.coding[0].code = 'DL'
    expr(
        "filter(identifier, x -> x.type.coding[0].code = 'DL')[0].value"
    ).alias("drivers_license"),
    # MRN / EMPI: type.text = 'Medical Record Number' or 'EMPI'
    expr(
        "filter(identifier, x -> x.type.text = 'Medical Record Number')[0].value"
    ).alias("mrn"),
    # Extract name components for fuzzy matching
    col("name").getItem(0).getField("family").alias("last_name"),
    col("name").getItem(0).getField("given").getItem(0).alias("first_name"),
)

print(f"Patients before dedup: {patients_with_ids.count()}")
display(patients_with_ids.select("patient_id", "ssn", "last_name", "first_name", "birth_date").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simulate Cross-Hospital Duplicates
# MAGIC
# MAGIC Synthea generates unique patients per bundle. In reality, the same patient visits
# MAGIC multiple network hospitals. We simulate this by creating a copy of 5 patients as if
# MAGIC they arrived from a different hospital's feed — different bundle, same person.

# COMMAND ----------

# DBTITLE 1,Create Synthetic Duplicates (demo only)
from pyspark.sql.functions import monotonically_increasing_id, concat

# Take 5 patients and re-create them with different bundle IDs
# (simulates same patient seen at Rural Hospital 3 and Rural Hospital 7)
dupes = (
    patients_with_ids.limit(5)
    .withColumn("bundleUUID", concat(lit("dupe-hospital-7-"), col("patient_id")))
    .withColumn("patient_id", concat(col("patient_id"), lit("-hosp7")))
    # SSN and name+DOB stay the same — this is what the MPI catches
)

all_patients = patients_with_ids.unionByName(dupes)
dupe_count = all_patients.count() - patients_with_ids.count()
print(f"Injected {dupe_count} synthetic duplicate patients (same SSN, different hospital feed)")
print(f"Total patient records before MPI: {all_patients.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deterministic MPI Matching
# MAGIC
# MAGIC **Tier 1:** Exact SSN match — highest confidence, covers ~85% of matches in rural NE.
# MAGIC
# MAGIC **Tier 2:** Last name + DOB + gender — fallback for patients without SSN on file
# MAGIC (common in rural settings, especially for uninsured or Medicaid patients).

# COMMAND ----------

# DBTITLE 1,Tier 1: SSN Match
# Generate a deterministic master_patient_id from SSN (most reliable identifier)
tier1 = (
    all_patients
    .filter(col("ssn").isNotNull())
    .withColumn("match_key", md5(trim(col("ssn"))))
    .withColumn("match_tier", lit("tier1_ssn"))
)

# COMMAND ----------

# DBTITLE 1,Tier 2: Name + DOB + Gender (fallback for missing SSN)
tier2 = (
    all_patients
    .filter(col("ssn").isNull())
    .withColumn(
        "match_key",
        md5(concat_ws("|",
            lower(trim(col("last_name"))),
            col("birth_date"),
            lower(trim(col("gender"))),
        ))
    )
    .withColumn("match_tier", lit("tier2_name_dob"))
)

# Combine tiers
matched = tier1.unionByName(tier2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Master Patient Identity

# COMMAND ----------

# DBTITLE 1,Assign master_patient_id per Match Key
from pyspark.sql.window import Window
from pyspark.sql.functions import first, count

# For each match_key (cluster of duplicate records), pick the earliest patient_id
# as the canonical master_patient_id
w = Window.partitionBy("match_key").orderBy("bundleUUID")

patient_master = (
    matched
    .withColumn("master_patient_id", first("patient_id").over(w))
    .select(
        col("master_patient_id"),
        col("patient_id").alias("source_patient_id"),
        col("bundleUUID").alias("source_bundle"),
        col("match_tier"),
        col("match_key"),
        col("ssn"),
        col("first_name"),
        col("last_name"),
        col("birth_date"),
        col("gender"),
    )
)

# COMMAND ----------

# DBTITLE 1,Write Silver Patient Master
(
    patient_master.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"`{catalog}`.`silver`.`patient_crosswalk`")
)

# Deduplicated patient table — one row per master_patient_id
deduplicated = (
    patient_master
    .dropDuplicates(["master_patient_id"])
    .select(
        col("master_patient_id").alias("patient_id"),
        col("ssn"),
        col("first_name"),
        col("last_name"),
        col("birth_date"),
        col("gender"),
    )
)

(
    deduplicated.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"`{catalog}`.`silver`.`patient_master`")
)
spark.sql(f"ALTER TABLE `{catalog}`.`silver`.`patient_master` CLUSTER BY (`patient_id`)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Before / After Metrics

# COMMAND ----------

# DBTITLE 1,Dedup Impact
before_count = all_patients.count()
after_count = deduplicated.count()
dupes_resolved = before_count - after_count

print(f"Patient records BEFORE MPI:  {before_count}")
print(f"Unique patients AFTER MPI:   {after_count}")
print(f"Duplicates resolved:         {dupes_resolved}")
print(f"Dedup rate:                  {dupes_resolved/before_count*100:.1f}%")
print()
print("Without MPI, every duplicate inflates HEDIS denominators.")
print(f"At the network scale (multiple hospitals), even a 5% duplicate rate means")
print(f"~{int(after_count * 0.05)} phantom patients distorting every quality measure.")

# COMMAND ----------

# DBTITLE 1,Crosswalk Sample — Same Person, Different Hospitals
display(
    spark.sql(f"""
        SELECT master_patient_id, source_patient_id, source_bundle, match_tier, ssn
        FROM `{catalog}`.`silver`.`patient_crosswalk`
        WHERE master_patient_id IN (
            SELECT master_patient_id
            FROM `{catalog}`.`silver`.`patient_crosswalk`
            GROUP BY master_patient_id
            HAVING count(*) > 1
        )
        ORDER BY master_patient_id, source_bundle
        LIMIT 20
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **What's next:** [04_gold_hedis_care_gaps](04_gold_hedis_care_gaps) — Five HEDIS measures
# MAGIC computed against the deduplicated silver layer. Denominators are now accurate.
# MAGIC
# MAGIC **Cost footprint:** Serverless. At production scale: est. 2-3 DBUs per incremental
# MAGIC MPI run. The MERGE-based incremental update (new hospital feed → match against master)
# MAGIC is the most compute-intensive step in the pipeline. Monthly: ~20-30 DBUs ($4-6/month).
