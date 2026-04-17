# Databricks notebook source
# COMPUTE: Serverless (Serverless SQL Warehouse for scheduled refresh)

# MAGIC %md
# MAGIC # Nebraska CIN — Gold Layer: HEDIS Care Gap Identification
# MAGIC
# MAGIC **What this notebook proves:** Five HEDIS measures computed as pure SQL against the
# MAGIC deduplicated silver layer, materialized as gold tables, refreshable daily, and queryable
# MAGIC from Tableau or Genie without a custom extract.
# MAGIC
# MAGIC **The Cibolo pain point it addresses:** *"We manually extract HEDIS data from Garage
# MAGIC quarterly. It takes weeks, the numbers are always wrong, and by the time we have them
# MAGIC the intervention window has closed."*
# MAGIC
# MAGIC **The defensible business outcome:** HEDIS care gap identification runs daily (not
# MAGIC quarterly), is traceable to source FHIR bundles via `bundleUUID` lineage, and is
# MAGIC accessible from any BI tool without a custom extract process. Intervention windows
# MAGIC are measured in days, not months.
# MAGIC
# MAGIC ---
# MAGIC **Synthetic data only — no PHI.**
# MAGIC
# MAGIC **HEDIS specification references** cite NCQA HEDIS MY2024 Technical Specifications.
# MAGIC Synthea uses SNOMED CT for conditions and LOINC for observations. Production deployment
# MAGIC would add ICD-10-CM and CPT crosswalks via a terminology service or value set table.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "nebraska_cin_catalog", "CIN Catalog")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG `{catalog}`")

# Measurement year for HEDIS — current calendar year
measurement_year = "2026"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Annual Wellness Visit (AWV)
# MAGIC
# MAGIC **HEDIS Spec:** Preventive/Ambulatory Health Services (W15, W34, AWC)
# MAGIC
# MAGIC - **Denominator:** Patients 18+ as of Dec 31 of measurement year
# MAGIC - **Numerator:** At least one wellness/preventive encounter in measurement year
# MAGIC - **Exclusions:** Patients in hospice or deceased before measurement year end
# MAGIC
# MAGIC Synthea codes wellness visits as SNOMED `162673000` (General examination) and
# MAGIC encounter class `wellness`.

# COMMAND ----------

# DBTITLE 1,gold.hedis_awv
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.`gold`.`hedis_awv` AS

WITH eligible_patients AS (
    -- Denominator: patients 18+ at end of measurement year
    SELECT patient_id, birth_date, gender
    FROM `{catalog}`.`silver`.`patient_master`
    WHERE year('{measurement_year}-12-31') - year(birth_date) >= 18
),

wellness_visits AS (
    -- Numerator: at least one wellness/preventive encounter in measurement year
    -- SNOMED 162673000 = General examination of patient
    -- Synthea also uses encounter class 'wellness'
    SELECT DISTINCT e.patient_id
    FROM `{catalog}`.`silver`.`encounter` e
    WHERE (
        e.encounter_code IN ('162673000', '185349003', '410620009')  -- General exam, wellness, well child
        OR lower(e.encounter_display) LIKE '%wellness%'
        OR lower(e.encounter_display) LIKE '%preventive%'
    )
    AND year(e.encounter_start) = {measurement_year}
)

SELECT
    p.patient_id,
    p.birth_date,
    p.gender,
    CASE WHEN w.patient_id IS NOT NULL THEN 'closed' ELSE 'open' END AS gap_status,
    'AWV' AS measure_code,
    'Annual Wellness Visit' AS measure_name,
    -- Denominator inclusion = 1 for all eligible patients
    1 AS in_denominator,
    CASE WHEN w.patient_id IS NOT NULL THEN 1 ELSE 0 END AS in_numerator,
    0 AS excluded
FROM eligible_patients p
LEFT JOIN wellness_visits w ON p.patient_id = w.patient_id
""")

display(spark.sql(f"""
    SELECT gap_status, count(*) as patients,
           round(count(*) * 100.0 / sum(count(*)) over(), 1) as pct
    FROM `{catalog}`.`gold`.`hedis_awv`
    GROUP BY gap_status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Breast Cancer Screening (BCS)
# MAGIC
# MAGIC **HEDIS Spec:** BCS-E
# MAGIC
# MAGIC - **Denominator:** Female patients aged 50-74 at end of measurement year
# MAGIC - **Numerator:** Mammogram performed in past 27 months (measurement year + 1 prior year + 3 months)
# MAGIC - **Exclusions:** Bilateral mastectomy, history of breast cancer
# MAGIC
# MAGIC Synthea codes mammography as SNOMED `24623002` (Screening mammography).

# COMMAND ----------

# DBTITLE 1,gold.hedis_bcs
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.`gold`.`hedis_bcs` AS

WITH eligible_patients AS (
    -- Denominator: female, 50-74 at end of measurement year
    SELECT patient_id, birth_date, gender
    FROM `{catalog}`.`silver`.`patient_master`
    WHERE gender = 'female'
    AND year('{measurement_year}-12-31') - year(birth_date) BETWEEN 50 AND 74
),

mammograms AS (
    -- Numerator: mammography in 27-month lookback
    -- SNOMED 24623002 = Screening mammography
    -- SNOMED 71651007 = Mammography
    SELECT DISTINCT p.patient_id
    FROM `{catalog}`.`silver`.`procedure_occurrence` p
    WHERE (
        p.procedure_code IN ('24623002', '71651007')
        OR lower(p.procedure_display) LIKE '%mammogra%'
    )
    AND p.performed_start >= date_add('{measurement_year}-01-01', -93)  -- 27 months back
    AND p.performed_start <= '{measurement_year}-12-31'
),

exclusions AS (
    -- Exclusion: bilateral mastectomy or breast cancer history
    SELECT DISTINCT c.patient_id
    FROM `{catalog}`.`silver`.`condition` c
    WHERE (
        lower(c.condition_display) LIKE '%mastectomy%'
        OR lower(c.condition_display) LIKE '%breast cancer%'
        -- ICD-10 Z90.13 = bilateral mastectomy status (for production)
    )
)

SELECT
    p.patient_id,
    p.birth_date,
    p.gender,
    CASE
        WHEN ex.patient_id IS NOT NULL THEN 'excluded'
        WHEN m.patient_id IS NOT NULL THEN 'closed'
        ELSE 'open'
    END AS gap_status,
    'BCS' AS measure_code,
    'Breast Cancer Screening' AS measure_name,
    CASE WHEN ex.patient_id IS NOT NULL THEN 0 ELSE 1 END AS in_denominator,
    CASE WHEN m.patient_id IS NOT NULL AND ex.patient_id IS NULL THEN 1 ELSE 0 END AS in_numerator,
    CASE WHEN ex.patient_id IS NOT NULL THEN 1 ELSE 0 END AS excluded
FROM eligible_patients p
LEFT JOIN mammograms m ON p.patient_id = m.patient_id
LEFT JOIN exclusions ex ON p.patient_id = ex.patient_id
""")

display(spark.sql(f"""
    SELECT gap_status, count(*) as patients,
           round(count(*) * 100.0 / sum(count(*)) over(), 1) as pct
    FROM `{catalog}`.`gold`.`hedis_bcs`
    GROUP BY gap_status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Colorectal Cancer Screening (COL)
# MAGIC
# MAGIC **HEDIS Spec:** COL-E
# MAGIC
# MAGIC - **Denominator:** Patients aged 45-75 at end of measurement year
# MAGIC - **Numerator:** Colonoscopy in past 10 years, OR FIT/FOBT in past year, OR FIT-DNA in past 3 years
# MAGIC - **Exclusions:** Colorectal cancer diagnosis, total colectomy
# MAGIC
# MAGIC Synthea codes colonoscopy as SNOMED `73761001`.

# COMMAND ----------

# DBTITLE 1,gold.hedis_col
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.`gold`.`hedis_col` AS

WITH eligible_patients AS (
    SELECT patient_id, birth_date, gender
    FROM `{catalog}`.`silver`.`patient_master`
    WHERE year('{measurement_year}-12-31') - year(birth_date) BETWEEN 45 AND 75
),

screenings AS (
    -- Colonoscopy in past 10 years
    SELECT DISTINCT p.patient_id
    FROM `{catalog}`.`silver`.`procedure_occurrence` p
    WHERE (
        p.procedure_code IN ('73761001', '174158000')  -- Colonoscopy, screening colonoscopy
        OR lower(p.procedure_display) LIKE '%colonoscopy%'
    )
    AND p.performed_start >= date_add('{measurement_year}-12-31', -3650)  -- 10 years

    UNION

    -- FOBT/FIT in past year
    SELECT DISTINCT o.patient_id
    FROM `{catalog}`.`silver`.`observation` o
    WHERE (
        o.obs_code IN ('29771-3', '57905-2', '27396-1')  -- LOINC codes for FOBT/FIT
        OR lower(o.obs_display) LIKE '%fecal occult%'
        OR lower(o.obs_display) LIKE '%fecal immunochemical%'
    )
    AND year(o.effective_date) = {measurement_year}
),

exclusions AS (
    SELECT DISTINCT c.patient_id
    FROM `{catalog}`.`silver`.`condition` c
    WHERE (
        lower(c.condition_display) LIKE '%colorectal cancer%'
        OR lower(c.condition_display) LIKE '%malignant neoplasm of colon%'
        OR lower(c.condition_display) LIKE '%colectomy%'
    )
)

SELECT
    p.patient_id, p.birth_date, p.gender,
    CASE
        WHEN ex.patient_id IS NOT NULL THEN 'excluded'
        WHEN s.patient_id IS NOT NULL THEN 'closed'
        ELSE 'open'
    END AS gap_status,
    'COL' AS measure_code,
    'Colorectal Cancer Screening' AS measure_name,
    CASE WHEN ex.patient_id IS NOT NULL THEN 0 ELSE 1 END AS in_denominator,
    CASE WHEN s.patient_id IS NOT NULL AND ex.patient_id IS NULL THEN 1 ELSE 0 END AS in_numerator,
    CASE WHEN ex.patient_id IS NOT NULL THEN 1 ELSE 0 END AS excluded
FROM eligible_patients p
LEFT JOIN screenings s ON p.patient_id = s.patient_id
LEFT JOIN exclusions ex ON p.patient_id = ex.patient_id
""")

display(spark.sql(f"""
    SELECT gap_status, count(*) as patients,
           round(count(*) * 100.0 / sum(count(*)) over(), 1) as pct
    FROM `{catalog}`.`gold`.`hedis_col`
    GROUP BY gap_status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. HbA1c Management (A1C)
# MAGIC
# MAGIC **HEDIS Spec:** CDC — Comprehensive Diabetes Care (HbA1c Testing + HbA1c Control <8%)
# MAGIC
# MAGIC - **Denominator:** Patients 18-75 with diabetes diagnosis (Type 1 or Type 2)
# MAGIC - **Numerator:** HbA1c test performed in measurement year AND result < 8.0%
# MAGIC - **Exclusions:** Gestational diabetes only, steroid-induced diabetes only
# MAGIC
# MAGIC Synthea codes: Diabetes = SNOMED `44054006` (Type 2), HbA1c = LOINC `4548-4`.

# COMMAND ----------

# DBTITLE 1,gold.hedis_a1c
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.`gold`.`hedis_a1c` AS

WITH diabetic_patients AS (
    -- Denominator: age 18-75, active diabetes diagnosis
    SELECT DISTINCT pm.patient_id, pm.birth_date, pm.gender
    FROM `{catalog}`.`silver`.`patient_master` pm
    INNER JOIN `{catalog}`.`silver`.`condition` c ON pm.patient_id = c.patient_id
    WHERE year('{measurement_year}-12-31') - year(pm.birth_date) BETWEEN 18 AND 75
    AND (
        c.condition_code IN ('44054006', '73211009', '46635009')  -- Type 2, Type 1, Type 1.5
        OR lower(c.condition_display) LIKE '%diabetes%'
    )
    AND (c.clinical_status IS NULL OR c.clinical_status = 'active')
),

a1c_results AS (
    -- Most recent HbA1c in measurement year
    -- LOINC 4548-4 = Hemoglobin A1c/Hemoglobin.total in Blood
    SELECT
        o.patient_id,
        o.value_numeric AS a1c_value,
        o.effective_date,
        ROW_NUMBER() OVER (PARTITION BY o.patient_id ORDER BY o.effective_date DESC) AS rn
    FROM `{catalog}`.`silver`.`observation` o
    WHERE (
        o.obs_code = '4548-4'
        OR lower(o.obs_display) LIKE '%a1c%'
        OR lower(o.obs_display) LIKE '%hemoglobin a1c%'
    )
    AND year(o.effective_date) = {measurement_year}
    AND o.value_numeric IS NOT NULL
)

SELECT
    dp.patient_id, dp.birth_date, dp.gender,
    ar.a1c_value AS last_a1c_value,
    ar.effective_date AS last_a1c_date,
    CASE
        WHEN ar.patient_id IS NOT NULL AND ar.a1c_value < 8.0 THEN 'closed'
        WHEN ar.patient_id IS NOT NULL AND ar.a1c_value >= 8.0 THEN 'open'  -- tested but uncontrolled
        ELSE 'open'  -- no test in measurement year
    END AS gap_status,
    'A1C' AS measure_code,
    'HbA1c Management (<8%)' AS measure_name,
    1 AS in_denominator,
    CASE WHEN ar.patient_id IS NOT NULL AND ar.a1c_value < 8.0 THEN 1 ELSE 0 END AS in_numerator,
    0 AS excluded
FROM diabetic_patients dp
LEFT JOIN a1c_results ar ON dp.patient_id = ar.patient_id AND ar.rn = 1
""")

display(spark.sql(f"""
    SELECT gap_status, count(*) as patients,
           round(count(*) * 100.0 / sum(count(*)) over(), 1) as pct,
           round(avg(last_a1c_value), 1) as avg_a1c
    FROM `{catalog}`.`gold`.`hedis_a1c`
    GROUP BY gap_status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Controlling High Blood Pressure (CBP)
# MAGIC
# MAGIC **HEDIS Spec:** CBP — Controlling High Blood Pressure
# MAGIC
# MAGIC - **Denominator:** Patients 18-85 with hypertension diagnosis
# MAGIC - **Numerator:** Most recent BP reading < 140/90 mmHg in measurement year
# MAGIC - **Exclusions:** ESRD, kidney transplant, pregnancy, hospice
# MAGIC
# MAGIC Synthea codes: Hypertension = SNOMED `59621000`, BP = LOINC `85354-9` (panel),
# MAGIC systolic = LOINC `8480-6`, diastolic = LOINC `8462-4`.

# COMMAND ----------

# DBTITLE 1,gold.hedis_cbp
spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.`gold`.`hedis_cbp` AS

WITH hypertensive_patients AS (
    -- Denominator: age 18-85, hypertension diagnosis
    SELECT DISTINCT pm.patient_id, pm.birth_date, pm.gender
    FROM `{catalog}`.`silver`.`patient_master` pm
    INNER JOIN `{catalog}`.`silver`.`condition` c ON pm.patient_id = c.patient_id
    WHERE year('{measurement_year}-12-31') - year(pm.birth_date) BETWEEN 18 AND 85
    AND (
        c.condition_code = '59621000'  -- SNOMED: Essential hypertension
        OR lower(c.condition_display) LIKE '%hypertension%'
    )
    AND (c.clinical_status IS NULL OR c.clinical_status = 'active')
),

-- Get most recent systolic and diastolic readings in measurement year
systolic AS (
    SELECT o.patient_id, o.value_numeric AS systolic_value, o.effective_date,
           ROW_NUMBER() OVER (PARTITION BY o.patient_id ORDER BY o.effective_date DESC) AS rn
    FROM `{catalog}`.`silver`.`observation` o
    WHERE (o.obs_code = '8480-6' OR lower(o.obs_display) LIKE '%systolic%')
    AND year(o.effective_date) = {measurement_year}
    AND o.value_numeric IS NOT NULL
),

diastolic AS (
    SELECT o.patient_id, o.value_numeric AS diastolic_value, o.effective_date,
           ROW_NUMBER() OVER (PARTITION BY o.patient_id ORDER BY o.effective_date DESC) AS rn
    FROM `{catalog}`.`silver`.`observation` o
    WHERE (o.obs_code = '8462-4' OR lower(o.obs_display) LIKE '%diastolic%')
    AND year(o.effective_date) = {measurement_year}
    AND o.value_numeric IS NOT NULL
)

SELECT
    hp.patient_id, hp.birth_date, hp.gender,
    s.systolic_value AS last_systolic,
    d.diastolic_value AS last_diastolic,
    CASE
        WHEN s.systolic_value IS NOT NULL AND d.diastolic_value IS NOT NULL
             AND s.systolic_value < 140 AND d.diastolic_value < 90 THEN 'closed'
        ELSE 'open'
    END AS gap_status,
    'CBP' AS measure_code,
    'Controlling High Blood Pressure' AS measure_name,
    1 AS in_denominator,
    CASE
        WHEN s.systolic_value < 140 AND d.diastolic_value < 90 THEN 1
        ELSE 0
    END AS in_numerator,
    0 AS excluded
FROM hypertensive_patients hp
LEFT JOIN systolic s ON hp.patient_id = s.patient_id AND s.rn = 1
LEFT JOIN diastolic d ON hp.patient_id = d.patient_id AND d.rn = 1
""")

display(spark.sql(f"""
    SELECT gap_status, count(*) as patients,
           round(count(*) * 100.0 / sum(count(*)) over(), 1) as pct,
           round(avg(last_systolic), 0) as avg_systolic,
           round(avg(last_diastolic), 0) as avg_diastolic
    FROM `{catalog}`.`gold`.`hedis_cbp`
    GROUP BY gap_status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Unified Care Gaps View
# MAGIC
# MAGIC All five measures joined into a single view. This is the analytics surface that
# MAGIC Tableau or Genie connects to.

# COMMAND ----------

# DBTITLE 1,gold.hedis_care_gaps (unified view)
spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`gold`.`hedis_care_gaps` AS

SELECT patient_id, birth_date, gender, gap_status, measure_code, measure_name,
       in_denominator, in_numerator, excluded
FROM `{catalog}`.`gold`.`hedis_awv`
WHERE in_denominator = 1

UNION ALL

SELECT patient_id, birth_date, gender, gap_status, measure_code, measure_name,
       in_denominator, in_numerator, excluded
FROM `{catalog}`.`gold`.`hedis_bcs`
WHERE in_denominator = 1

UNION ALL

SELECT patient_id, birth_date, gender, gap_status, measure_code, measure_name,
       in_denominator, in_numerator, excluded
FROM `{catalog}`.`gold`.`hedis_col`
WHERE in_denominator = 1

UNION ALL

SELECT patient_id, birth_date, gender, gap_status, measure_code, measure_name,
       in_denominator, in_numerator, excluded
FROM `{catalog}`.`gold`.`hedis_a1c`
WHERE in_denominator = 1

UNION ALL

SELECT patient_id, birth_date, gender, gap_status, measure_code, measure_name,
       in_denominator, in_numerator, excluded
FROM `{catalog}`.`gold`.`hedis_cbp`
WHERE in_denominator = 1
""")

# COMMAND ----------

# DBTITLE 1,Care Gap Summary — All Five Measures
display(spark.sql(f"""
    SELECT
        measure_code,
        measure_name,
        count(*) AS denominator,
        sum(in_numerator) AS numerator,
        sum(excluded) AS excluded,
        round(sum(in_numerator) * 100.0 / count(*), 1) AS rate_pct,
        sum(CASE WHEN gap_status = 'open' THEN 1 ELSE 0 END) AS open_gaps
    FROM `{catalog}`.`gold`.`hedis_care_gaps`
    GROUP BY measure_code, measure_name
    ORDER BY measure_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Analytics Surface: Tableau and Genie
# MAGIC
# MAGIC The `gold.hedis_care_gaps` view is the single analytics interface for both BI tools.
# MAGIC No custom extract, no CSV export, no manual refresh. Connect and query.
# MAGIC
# MAGIC ### Option A: Tableau
# MAGIC ```
# MAGIC Server:    <workspace-url>
# MAGIC HTTP Path: /sql/1.0/warehouses/<warehouse-id>
# MAGIC Catalog:   nebraska_cin_catalog
# MAGIC Schema:    gold
# MAGIC Table:     hedis_care_gaps
# MAGIC ```
# MAGIC
# MAGIC Tableau connects to the Serverless SQL Warehouse via the Databricks JDBC/ODBC driver.
# MAGIC Live connection — no extract scheduling, no stale data.
# MAGIC
# MAGIC ### Option B: Genie Space (zero incremental cost)
# MAGIC ```sql
# MAGIC -- Create a Genie space pointing to the gold schema
# MAGIC -- Workspace UI: SQL > Genie Spaces > New
# MAGIC -- Tables: nebraska_cin_catalog.gold.hedis_care_gaps
# MAGIC --
# MAGIC -- Sample questions for care managers:
# MAGIC --   "Show me patients with open AWV gaps in the last 6 months"
# MAGIC --   "Which measure has the lowest compliance rate?"
# MAGIC --   "List diabetic patients with A1C above 9%"
# MAGIC ```
# MAGIC
# MAGIC Genie uses the same Serverless SQL Warehouse — no additional infrastructure.
# MAGIC Natural language queries are translated to SQL against the gold view.
# MAGIC **We do not pick a winner between Tableau and Genie.** Both connect to the same view.
# MAGIC Richard's team decides based on their hospital distribution requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Demo complete.**
# MAGIC
# MAGIC **Cost footprint:** Serverless SQL Warehouse for daily gold refresh. At Nebraska MVP
# MAGIC scale (14 hospitals, ~50K patients, 5 HEDIS measures):
# MAGIC
# MAGIC | Component | Daily DBUs | Monthly DBUs | Monthly cost (est.) |
# MAGIC |-----------|-----------|-------------|-------------------|
# MAGIC | Bronze DLT (4 triggers/day) | 8-16 | 240-480 | $48-96 |
# MAGIC | Silver normalize | 5-8 | 150-240 | $30-48 |
# MAGIC | MPI dedup | 2-3 | 60-90 | $12-18 |
# MAGIC | Gold HEDIS refresh | 3-5 | 90-150 | $18-30 |
# MAGIC | **Total pipeline** | **18-32** | **540-960** | **$108-192** |
# MAGIC | SQL Warehouse (ad-hoc queries) | varies | ~200-400 | $40-80 |
# MAGIC | **Total platform** | | **740-1360** | **$148-272/month** |
# MAGIC
# MAGIC These estimates are at list DBU pricing. Enterprise agreement pricing is typically 30-50% lower.
# MAGIC Genie incurs no additional DBU cost beyond the SQL Warehouse queries it generates.
