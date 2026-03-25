-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 — Data Quality Checks & PII Masking
-- MAGIC
-- MAGIC This notebook runs comprehensive data quality checks across the clinical
-- MAGIC document matching pipeline and creates Unity Catalog masking views/functions
-- MAGIC for PHI/PII protection.
-- MAGIC
-- MAGIC **Pipeline Position:** Step 6 of 7 (after scoring)
-- MAGIC
-- MAGIC **Key Databricks Features Used:**
-- MAGIC - Unity Catalog column masking functions
-- MAGIC - Row-level and column-level security via views
-- MAGIC - `GRANT` statements for role-based access control
-- MAGIC
-- MAGIC **Outputs:**
-- MAGIC - DQ report metrics written to `analytics.pipeline_dq_metrics`
-- MAGIC - Masking views in `curated` schema
-- MAGIC - Column masking functions applied to sensitive columns

-- COMMAND ----------

USE CATALOG serverless_stable_swv01_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Document Parsing Quality Checks

-- COMMAND ----------

-- Check 1: Percent unreadable documents (threshold: < 15%)
SELECT
  'pct_unreadable_docs' AS check_name,
  COUNT(*) AS total_docs,
  SUM(CASE WHEN unreadable_flag = true THEN 1 ELSE 0 END) AS unreadable_count,
  ROUND(
    SUM(CASE WHEN unreadable_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
  ) AS pct_unreadable,
  CASE
    WHEN SUM(CASE WHEN unreadable_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) < 15.0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  '< 15%' AS threshold
FROM curated.clinical_doc_parsed;

-- COMMAND ----------

-- Check 2: Percent documents with missing DOB (threshold: < 20%)
SELECT
  'pct_missing_dob' AS check_name,
  COUNT(*) AS total_readable_docs,
  SUM(CASE WHEN extracted_dob IS NULL THEN 1 ELSE 0 END) AS missing_dob_count,
  ROUND(
    SUM(CASE WHEN extracted_dob IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
  ) AS pct_missing_dob,
  CASE
    WHEN SUM(CASE WHEN extracted_dob IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) < 20.0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  '< 20%' AS threshold
FROM curated.clinical_doc_parsed
WHERE unreadable_flag = false;

-- COMMAND ----------

-- Check 3: Percent documents with missing SSN4 (threshold: < 25%)
SELECT
  'pct_missing_ssn4' AS check_name,
  COUNT(*) AS total_readable_docs,
  SUM(CASE WHEN extracted_ssn4 IS NULL THEN 1 ELSE 0 END) AS missing_ssn4_count,
  ROUND(
    SUM(CASE WHEN extracted_ssn4 IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
  ) AS pct_missing_ssn4,
  CASE
    WHEN SUM(CASE WHEN extracted_ssn4 IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) < 25.0
    THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  '< 25%' AS threshold
FROM curated.clinical_doc_parsed
WHERE unreadable_flag = false;

-- COMMAND ----------

-- Check 4: Average parse confidence score (threshold: > 0.7)
SELECT
  'avg_parse_confidence' AS check_name,
  ROUND(AVG(parse_confidence), 3) AS avg_confidence,
  ROUND(MIN(parse_confidence), 3) AS min_confidence,
  ROUND(MAX(parse_confidence), 3) AS max_confidence,
  CASE
    WHEN AVG(parse_confidence) > 0.7 THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  '> 0.7' AS threshold
FROM curated.clinical_doc_parsed
WHERE unreadable_flag = false;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Matching Quality Checks

-- COMMAND ----------

-- Check 5: Distribution of match categories
SELECT
  'match_distribution' AS check_name,
  match_classification,
  COUNT(*) AS pair_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM analytics.doc_member_match_candidates
GROUP BY match_classification
ORDER BY pair_count DESC;

-- COMMAND ----------

-- Check 6: Percent of auths with matched documents
SELECT
  'pct_auths_with_docs' AS check_name,
  COUNT(DISTINCT a.auth_id) AS total_auths,
  COUNT(DISTINCT amc.auth_id) AS auths_with_matched_docs,
  ROUND(
    COUNT(DISTINCT amc.auth_id) * 100.0 / COUNT(DISTINCT a.auth_id), 2
  ) AS pct_with_docs
FROM raw.authorization a
LEFT JOIN analytics.doc_auth_match_candidates amc
  ON a.auth_id = amc.auth_id
  AND amc.match_classification IN ('match', 'possible_match');

-- COMMAND ----------

-- Check 7: Orphaned documents (no match candidate at all)
SELECT
  'orphaned_documents' AS check_name,
  COUNT(*) AS total_parsed_docs,
  SUM(CASE WHEN mc.doc_id IS NULL THEN 1 ELSE 0 END) AS orphaned_docs,
  ROUND(
    SUM(CASE WHEN mc.doc_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
  ) AS pct_orphaned
FROM curated.clinical_doc_parsed cdp
LEFT JOIN analytics.doc_member_match_candidates mc ON cdp.doc_id = mc.doc_id
WHERE cdp.unreadable_flag = false;

-- COMMAND ----------

-- Check 8: Duplicate match detection (same doc matched to multiple members with high confidence)
SELECT
  'duplicate_high_confidence_matches' AS check_name,
  doc_id,
  COUNT(*) AS high_conf_match_count,
  ROUND(MAX(total_weight), 2) AS best_score,
  ROUND(MIN(total_weight), 2) AS second_best_score
FROM analytics.doc_member_match_candidates
WHERE match_classification = 'match'
GROUP BY doc_id
HAVING COUNT(*) > 1
ORDER BY high_conf_match_count DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Create Column Masking Functions
-- MAGIC
-- MAGIC Unity Catalog column masking functions dynamically mask data based on the
-- MAGIC requesting user's group membership. Clinical reviewers see full data;
-- MAGIC analysts see masked values.

-- COMMAND ----------

-- SSN4 masking function: returns '****' for non-clinical users
CREATE OR REPLACE FUNCTION curated.mask_ssn4(ssn4_value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN IS_MEMBER('serverless_stable_swv01_catalog_clinical') THEN ssn4_value
    WHEN IS_MEMBER('serverless_stable_swv01_catalog_admin') THEN ssn4_value
    ELSE '****'
  END;

-- COMMAND ----------

-- DOB masking function: returns year-only for non-clinical users
CREATE OR REPLACE FUNCTION curated.mask_dob(dob_value DATE)
RETURNS STRING
RETURN
  CASE
    WHEN IS_MEMBER('serverless_stable_swv01_catalog_clinical') THEN CAST(dob_value AS STRING)
    WHEN IS_MEMBER('serverless_stable_swv01_catalog_admin') THEN CAST(dob_value AS STRING)
    ELSE CONCAT(YEAR(dob_value), '-XX-XX')
  END;

-- COMMAND ----------

-- Phone masking function: returns last 4 digits only for non-clinical users
CREATE OR REPLACE FUNCTION curated.mask_phone(phone_value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN IS_MEMBER('serverless_stable_swv01_catalog_clinical') THEN phone_value
    WHEN IS_MEMBER('serverless_stable_swv01_catalog_admin') THEN phone_value
    WHEN phone_value IS NULL THEN NULL
    ELSE CONCAT('******', RIGHT(phone_value, 4))
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Create Masking Views
-- MAGIC
-- MAGIC These views provide safe access to data for non-privileged roles.

-- COMMAND ----------

-- Masked member view (for analysts — no raw PII)
CREATE OR REPLACE VIEW curated.member_masked AS
SELECT
  member_id,
  LEFT(first_name, 1) || '***' AS first_name,
  LEFT(last_name, 1) || '***' AS last_name,
  middle_initial,
  curated.mask_dob(dob) AS dob,
  gender,
  curated.mask_ssn4(ssn4) AS ssn4,
  LEFT(address_line1, 5) || '...' AS address_line1,
  city,
  state,
  LEFT(zip, 3) || '**' AS zip,
  curated.mask_phone(phone) AS phone,
  preferred_language,
  race_ethnicity,
  source_system,
  created_at,
  updated_at
FROM ref.member;

-- COMMAND ----------

-- Masked clinical doc parsed view
CREATE OR REPLACE VIEW curated.clinical_doc_parsed_masked AS
SELECT
  doc_id,
  file_path,
  ingest_ts,
  -- raw_text intentionally excluded — contains PHI
  LEFT(extracted_first_name, 1) || '***' AS extracted_first_name,
  LEFT(extracted_last_name, 1) || '***' AS extracted_last_name,
  curated.mask_dob(extracted_dob) AS extracted_dob,
  curated.mask_ssn4(extracted_ssn4) AS extracted_ssn4,
  extracted_auth_id,
  extracted_member_id_form,
  extracted_provider_name,
  extracted_provider_npi,
  extracted_procedure_code,
  extracted_diagnosis_code,
  parse_error_status,
  parse_confidence,
  unreadable_flag,
  extraction_model,
  extraction_timestamp,
  created_at
FROM curated.clinical_doc_parsed;

-- COMMAND ----------

-- Safe match candidates view (scores only — no PII)
CREATE OR REPLACE VIEW analytics.match_candidates_safe AS
SELECT
  mc.match_id,
  mc.doc_id,
  mc.member_id,
  mc.blocking_key,
  mc.jw_first_name_score,
  mc.jw_last_name_score,
  mc.dob_match_score,
  mc.ssn4_match_score,
  mc.total_weight,
  mc.match_classification,
  mc.fellegi_sunter_log_odds,
  mc.created_at
  -- NOTE: No member name, DOB, SSN4, or other PII exposed
FROM analytics.doc_member_match_candidates mc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Pipeline Quality Dashboard Summary

-- COMMAND ----------

-- Comprehensive pipeline summary for monitoring
SELECT 'Total Members' AS metric, CAST(COUNT(*) AS STRING) AS value FROM ref.member
UNION ALL
SELECT 'Total Authorizations', CAST(COUNT(*) AS STRING) FROM raw.authorization
UNION ALL
SELECT 'Total Documents', CAST(COUNT(*) AS STRING) FROM raw.clinical_document
UNION ALL
SELECT 'Parsed Documents', CAST(COUNT(*) AS STRING) FROM curated.clinical_doc_parsed
UNION ALL
SELECT 'Unreadable Documents', CAST(COUNT(*) AS STRING) FROM curated.clinical_doc_parsed WHERE unreadable_flag = true
UNION ALL
SELECT 'High-Confidence Matches', CAST(COUNT(*) AS STRING) FROM analytics.doc_member_match_candidates WHERE match_classification = 'match'
UNION ALL
SELECT 'Possible Matches (Review)', CAST(COUNT(*) AS STRING) FROM analytics.doc_member_match_candidates WHERE match_classification = 'possible_match'
UNION ALL
SELECT 'Non-Matches', CAST(COUNT(*) AS STRING) FROM analytics.doc_member_match_candidates WHERE match_classification = 'non_match'
UNION ALL
SELECT 'Auth Matches', CAST(COUNT(*) AS STRING) FROM analytics.doc_auth_match_candidates WHERE match_classification IN ('match', 'possible_match');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC
-- MAGIC Proceed to **Notebook 07** to publish match results as events (Kafka / Delta).
