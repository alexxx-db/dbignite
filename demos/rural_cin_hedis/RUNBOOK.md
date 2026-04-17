# Rural CIN Demo — Runbook

## Prerequisites

Complete these steps **before** the demo. Estimated setup time: 15 minutes.

### 1. Workspace Requirements

- Databricks workspace with **Unity Catalog** enabled
- **Serverless compute** enabled (workspace admin setting)
- **DLT Serverless** enabled (may require separate enablement)
- Current user has `CREATE CATALOG` privilege, or a workspace admin has
  pre-created `demo_cin_catalog`
- Network access to `s3://hls-eng-data-public/` (public S3 — no credentials needed,
  but some workspaces block cross-account S3 reads)

### 2. Install dbignite

Option A — workspace library (recommended for demo):
```
Workspace UI > Compute > Libraries > Install New > Upload wheel
Upload: dist/dbignite-0.2.4-py3-none-any.whl
```

Option B — per-session install (run once from any notebook):
```python
%pip install /Volumes/demo_cin_catalog/bronze/raw_feeds/dbignite-0.2.4-py3-none-any.whl
```
(Upload the wheel to the Volume first via UI or `dbutils.fs.cp`)

### 3. Import Notebooks

Import the `demos/rural_cin_hedis/` directory into the workspace:
```
Workspace UI > Repos > Add Repo > paste the alexxx-db/dbignite URL
```
Or import the 5 `.py` files directly into a workspace folder.

### 4. Fallback: S3 Access Blocked

If the workspace cannot read from `s3://hls-eng-data-public/`, pre-upload Synthea
data to the Volume:

1. Download 50 Synthea FHIR bundles from
   https://synthetichealth.github.io/synthea-sample-data/downloads/latest/
2. Upload to `/Volumes/demo_cin_catalog/bronze/raw_feeds/fhir_bundles/`
3. Modify `00_setup_uc_catalogs.py` cell "Copy Synthea FHIR Bundles" to skip the S3 copy

---

## Demo Click-Path

### Notebook 00: Setup (3 min)

1. Open `00_setup_uc_catalogs.py`
2. Verify widget default: `catalog = demo_cin_catalog`
3. **Run All** (Cmd+Shift+Enter / Ctrl+Shift+Enter)
4. Checkpoint: last cell shows pipeline state and quarantine table query

**Expected output:**
- `Catalog demo_cin_catalog ready with bronze/silver/gold schemas`
- `Copied 50 FHIR bundles to /Volumes/.../fhir_bundles/`
- `Malformed ADT bundle written to .../MALFORMED_NO_PATIENT_ID.json`
- `Pipeline triggered. Update ID: <uuid>`
- Pipeline reaches `IDLE` state within ~60 seconds
- Quarantine table shows 1 row (the malformed record with missing patient_id)

**If pipeline is slow:** The quarantine query cell retries for up to 5 minutes.
If it times out, check pipeline UI and query the quarantine table manually:
```sql
SELECT * FROM demo_cin_catalog.bronze.quarantine_adt
```

### Notebook 01: Bronze DLT (5 min — code walkthrough, not interactive run)

1. Open `01_bronze_ingest_adt.py` in a new browser tab
2. Walk through the code while the pipeline (triggered in NB00) runs
3. Key talking points:
   - Auto Loader: incremental file discovery (no polling, no cron)
   - `QUALITY_RULES` dict: one line per rule, auditable
   - Explicit quarantine table with `failed_rule` and `rule_expression` columns
   - "This record didn't vanish — it's right here with the reason it was rejected"
4. Switch back to NB00 and show the quarantine table query result

**Do NOT try to Run All on this notebook** — it's a DLT pipeline definition
and will fail with "dlt module not available" outside a pipeline context.

### Notebook 02: Silver (10 min)

1. Open `02_silver_fhir_normalize.py`
2. **Run All**
3. Checkpoints:
   - Cell "Parse FHIR Bundles": shows bundle count and resource types
   - Cell "Side-by-side": raw JSON vs. typed schema (the visual "aha")
   - Cell "FHIR Round-Trip": shows DataFrame → FHIR Bundle conversion
   - Final cell: table row counts for all 6 silver tables

**Expected output:**
- `Parsed 50 FHIR bundles` (or however many were loaded)
- 6 silver tables created with non-zero row counts
- FHIR round-trip produces valid JSON bundles

**If Observation column is missing from dbignite output:**
The default `FhirSchemaModel` may not include Observation in all versions.
If the Observation cell fails, comment it out — the HEDIS A1C and CBP measures
will still work if there are Observation resources in the raw data. Add the schema
manually if needed:
```python
from pyspark.sql.types import StructField, StringType, DoubleType, StructType
# Add Observation schema extension here
```

### Notebook 03: MPI (5 min)

1. Open `03_silver_mpi_dedup.py`
2. **Run All**
3. Checkpoints:
   - Cell "Parse Patient Identifiers": shows patient count before dedup
   - Cell "Create Synthetic Duplicates": confirms 5 duplicates injected
   - Cell "Dedup Impact": shows before/after counts and dedup rate
   - Cell "Crosswalk Sample": shows duplicate clusters

**Expected output:**
- `Patients before dedup: 55` (50 originals + 5 synthetic duplicates)
- `Unique patients AFTER MPI: 50`
- `Duplicates resolved: 5`
- `Dedup rate: 9.1%`
- Crosswalk shows 5 clusters with 2 records each (original + duplicate)

### Notebook 04: Gold HEDIS (7 min)

1. Open `04_gold_hedis_care_gaps.py`
2. **Run All**
3. Checkpoints: each HEDIS measure displays a gap_status breakdown
4. Final cell: unified care gap summary across all 5 measures

**Expected output:**
- All 5 measures show non-zero denominators
- AWV: denominator = all adult patients, numerator = those with wellness encounters
- BCS: denominator = females 50-74, may be small with 50 patients
- COL: denominator = patients 45-75
- A1C: denominator = diabetic patients (Synthea generates diabetes cases)
- CBP: denominator = hypertensive patients (Synthea generates hypertension)
- Some measures may show 0 numerators if the 50-patient sample doesn't include
  the relevant screenings — this is expected with small sample sizes

**If a measure shows 0 denominator:**
- BCS/COL: the 50-patient sample may not include enough patients in the age range.
  Increase the sample size in NB00 (change `[:50]` to `[:200]`).
- A1C/CBP: these depend on condition codes. Synthea uses SNOMED codes; verify
  the LIKE patterns match Synthea's condition display text.

---

## Fallback Plan: Pre-Materialized Tables

If Serverless is slow or the demo workspace has issues, pre-run all notebooks
the day before and switch to a "results walkthrough" mode:

1. Run all 5 notebooks end-to-end the evening before
2. During the demo, open each notebook and scroll through the output cells
3. Run individual cells to show they're live (not screenshots)
4. The gold tables are persistent — query them directly if needed

---

## Likely Questions

### At Notebook 00 (Setup)

**Q: "Can we use our existing catalog instead of creating a new one?"**
A: Yes — change the `catalog` widget to your existing catalog name. The notebooks
only create schemas (bronze/silver/gold) inside whatever catalog you specify. No
structural dependency on the catalog name.

**Q: "What happens if two CINs share a workspace?"**
A: Each CIN gets its own catalog. Show the commented-out GRANT statements — a
Network A analyst querying the Network B catalog gets ACCESS_DENIED at the catalog
level, not a zero-row result. The isolation is structural, not a WHERE clause.

**Q: "How much does the catalog itself cost?"**
A: Zero. Unity Catalog metadata (catalogs, schemas, volumes) has no per-object
charge. Cost is only incurred when compute reads or writes data.

### At Notebook 01 (Bronze)

**Q: "How fast does the ADT feed actually land?"**
A: Auto Loader polls the Volume every 10 seconds by default. With Triggered DLT,
end-to-end latency is: trigger interval + file discovery (~10s) + processing time.
For a single ADT message: under 30 seconds. For the 15-min TCM window, we trigger
every 5 minutes with a 10-minute buffer.

**Q: "What if the malformed record needs to be re-processed after fixing?"**
A: Move the fixed record from quarantine back to the raw_feeds Volume. The next
pipeline trigger picks it up via Auto Loader's incremental processing. No replay of
the entire history.

**Q: "Does this replace our HL7 parser?"**
A: For FHIR R4 feeds, yes — dbignite handles the parsing. For HL7v2 ADT messages
(which most rural hospitals still send), a HL7v2-to-FHIR converter sits upstream
(e.g., Rhapsody, Mirth, or Azure FHIR Converter). The pipeline consumes FHIR
regardless of the source format.

### At Notebook 02 (Silver)

**Q: "What happens when an EHR vendor changes their FHIR schema?"**
A: dbignite parses against the FHIR R4 spec, not vendor-specific schemas. If a
vendor adds extensions (common with Epic/Cerner), they land as additional fields in
the resource JSON. We extend the FhirSchemaModel to map them — no code change to the
pipeline, just a schema config update.

**Q: "Can we write cleaned data back to the EHR?"**
A: Yes — the FHIR round-trip cell proves this. `Bundle.df_to_fhir()` converts any
DataFrame back to FHIR Bundle JSON. For outbound EHR writes, this feeds via the
Redox FHIR API (see `redox-ehr-api` in the dbignite repo).

**Q: "Why not use the OMOP CDM instead of these silver tables?"**
A: OMOP is a research data model — optimized for cohort studies, not care management.
HEDIS measures need encounter-level detail (visit dates, screening results) that
OMOP abstracts away. We can add OMOP as a parallel output for research use cases
without changing the HEDIS pipeline.

### At Notebook 03 (MPI)

**Q: "How accurate is the SSN match?"**
A: SSN is deterministic — if two records have the same SSN, they're the same person.
False positive rate: effectively zero. The risk is false negatives (same person,
different SSN on file), which is why Tier 2 (name+DOB) exists as a fallback. In
production, we'd add probabilistic matching as Tier 3 for remaining unmatched
records.

**Q: "What if a patient's SSN is wrong in one system?"**
A: The crosswalk table preserves the source_patient_id and source_bundle for every
match. If a match is disputed, the linkage can be broken by removing the crosswalk
row — no data is overwritten, only linked.

### At Notebook 04 (Gold HEDIS)

**Q: "Are these measure definitions production-ready?"**
A: The SQL structure (denominator/numerator/exclusion) matches the HEDIS spec. The
code systems (SNOMED in this demo) would be supplemented with ICD-10-CM and CPT
crosswalks in production — this is a terminology mapping task, not a logic change.
The measure logic is identical.

**Q: "Can we add more measures?"**
A: Yes — each measure is a standalone SQL CTE that reads from the same silver tables
and writes to a gold table. Adding a measure (e.g., Statin Therapy for CVD) is a new
SQL file following the same pattern. No pipeline changes.

**Q: "Tableau or Genie — which should we use?"**
A: Both connect to the same `gold.hedis_care_gaps` view via the Serverless SQL
Warehouse. Tableau is the right choice if your hospitals already have Tableau
dashboards and your team knows the tool. Genie is the right choice if you want care
managers to ask natural-language questions without building dashboards. They're not
mutually exclusive — run both against the same data.

---

## Verification Checklist

| Check | Method | Status |
|-------|--------|--------|
| All 5 notebooks parse as valid Python | `python -c "import ast; ast.parse(...)"` | PASS (verified locally) |
| No DBFS paths in any notebook | `grep -r /dbfs/` | PASS (verified locally) |
| No `%sh`, `%fs`, `%pip install` executable cells | `grep -r '%sh\|%fs\|%pip'` | PASS (verified locally) |
| No references to `databrickslabs/dbignite` | `grep -r databrickslabs` | PASS (verified locally) |
| All tables use three-level names | `grep` for all `saveAsTable`/`CREATE TABLE` | PASS (verified locally) |
| `COMPUTE:` tag on every notebook | `grep -r 'COMPUTE:'` | PASS (verified locally) |
| PHI disclaimer on every notebook | `grep -r 'no PHI'` | PASS (verified locally) |
| Opening markdown (proves/pain/outcome) on every NB | Manual review | PASS (verified locally) |
| Closing markdown (next/cost) on every NB | Manual review | PASS (verified locally) |
| Column names consistent silver→gold | `grep` cross-reference | PASS (verified locally) |
| SQL date arithmetic correct | Manual review of lookback windows | PASS (fixed: BCS 27mo, COL 10yr) |
| `network_b_cin_catalog` commented only | `grep network_b` | PASS (verified locally) |
| UC isolation GRANT demo commented | Manual review NB00 | PASS (verified locally) |
| All notebooks complete on Serverless | Run on workspace | **REQUIRES ON-WORKSPACE VERIFICATION** |
| All tables in `demo_cin_catalog.*.*` | Query `SHOW TABLES` per schema | **REQUIRES ON-WORKSPACE VERIFICATION** |
| Total runtime ≤ 35 minutes | End-to-end timing | **REQUIRES ON-WORKSPACE VERIFICATION** |
| Quarantine table has malformed record | Query NB00 output | **REQUIRES ON-WORKSPACE VERIFICATION** |
| 5 HEDIS measures have non-zero denominators | Query NB04 output | **REQUIRES ON-WORKSPACE VERIFICATION** |
| DLT pipeline reaches IDLE state | NB00 pipeline status check | **REQUIRES ON-WORKSPACE VERIFICATION** |
| dbignite Observation schema present | NB02 observation cell | **REQUIRES ON-WORKSPACE VERIFICATION** |
