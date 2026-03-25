-- ============================================================================
-- Dashboard-safe views for Clinical Document Matching
-- Catalog: serverless_stable_swv01_catalog
-- Schema: dashboard
--
-- These views expose only aggregated, PII-free data for operational dashboards.
-- Business users should be granted SELECT on the dashboard schema only.
-- ============================================================================

USE CATALOG serverless_stable_swv01_catalog;

CREATE SCHEMA IF NOT EXISTS dashboard
COMMENT 'Dashboard-safe views for clinical matching ops - no PII exposed';

-- ============================================================================
-- View 1: Intake Summary (documents received by day and type)
-- ============================================================================
CREATE OR REPLACE VIEW dashboard.v_intake_summary AS
SELECT
  DATE(ingestion_timestamp)                                      AS intake_date,
  COUNT(*)                                                       AS total_docs_received,
  SUM(CASE WHEN document_type = 'prior_auth_form'    THEN 1 ELSE 0 END) AS prior_auth_forms,
  SUM(CASE WHEN document_type = 'clinical_note'      THEN 1 ELSE 0 END) AS clinical_notes,
  SUM(CASE WHEN document_type = 'lab_result'         THEN 1 ELSE 0 END) AS lab_results,
  SUM(CASE WHEN document_type = 'imaging_report'     THEN 1 ELSE 0 END) AS imaging_reports,
  SUM(CASE WHEN document_type = 'discharge_summary'  THEN 1 ELSE 0 END) AS discharge_summaries,
  SUM(CASE WHEN is_readable = false THEN 1 ELSE 0 END)          AS unreadable_docs,
  ROUND(AVG(quality_score), 3)                                   AS avg_quality_score,
  SUM(CASE WHEN source_channel = 'fax'        THEN 1 ELSE 0 END) AS via_fax,
  SUM(CASE WHEN source_channel = 'electronic' THEN 1 ELSE 0 END) AS via_electronic,
  SUM(CASE WHEN source_channel = 'upload'     THEN 1 ELSE 0 END) AS via_upload,
  SUM(CASE WHEN source_channel = 'mail'       THEN 1 ELSE 0 END) AS via_mail
FROM raw.clinical_document
GROUP BY DATE(ingestion_timestamp);

-- ============================================================================
-- View 2: Data Quality Metrics (parsing completeness and accuracy)
-- ============================================================================
CREATE OR REPLACE VIEW dashboard.v_dq_metrics AS
SELECT
  DATE(p.ingest_ts)                                               AS parse_date,
  COUNT(*)                                                        AS total_docs,
  SUM(CASE WHEN p.unreadable_flag = true THEN 1 ELSE 0 END)      AS unreadable_count,
  ROUND(100.0 * SUM(CASE WHEN p.unreadable_flag = true THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_unreadable,
  SUM(CASE WHEN p.extracted_dob IS NULL THEN 1 ELSE 0 END)       AS missing_dob_count,
  ROUND(100.0 * SUM(CASE WHEN p.extracted_dob IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_dob,
  SUM(CASE WHEN p.extracted_ssn4 IS NULL OR p.extracted_ssn4 = '' THEN 1 ELSE 0 END) AS missing_ssn4_count,
  ROUND(100.0 * SUM(CASE WHEN p.extracted_ssn4 IS NULL OR p.extracted_ssn4 = '' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ssn4,
  SUM(CASE WHEN p.parse_error_status IS NOT NULL THEN 1 ELSE 0 END) AS docs_with_parse_error,
  ROUND(AVG(p.parse_confidence), 3)                               AS avg_parse_confidence
FROM curated.clinical_doc_parsed p
GROUP BY DATE(p.ingest_ts);

-- ============================================================================
-- View 3: Match Outcomes Summary (Fellegi-Sunter results by classification)
-- ============================================================================
CREATE OR REPLACE VIEW dashboard.v_match_summary AS
SELECT
  match_classification                    AS match_class,
  COUNT(*)                                AS candidate_count,
  ROUND(AVG(total_weight), 4)             AS avg_weight,
  ROUND(MIN(total_weight), 4)             AS min_weight,
  ROUND(MAX(total_weight), 4)             AS max_weight,
  ROUND(AVG(ssn4_match_score), 4)         AS avg_ssn4_agreement,
  ROUND(AVG(dob_match_score), 4)          AS avg_dob_agreement,
  ROUND(AVG(jw_first_name_score), 4)      AS avg_first_name_sim,
  ROUND(AVG(jw_last_name_score), 4)       AS avg_last_name_sim
FROM analytics.doc_member_match_candidates
GROUP BY match_classification;

-- ============================================================================
-- View 4: Per-Document Match Status with Risk Tier (no PII)
-- ============================================================================
CREATE OR REPLACE VIEW dashboard.v_doc_match_status AS
SELECT
  m.doc_id,
  d.document_type,
  d.source_channel,
  m.match_classification                  AS match_class,
  m.total_weight,
  p.unreadable_flag,
  CASE WHEN p.extracted_dob IS NULL THEN true ELSE false END AS missing_dob,
  CASE WHEN p.extracted_ssn4 IS NULL OR p.extracted_ssn4 = '' THEN true ELSE false END AS missing_ssn4,
  CASE
    WHEN p.unreadable_flag = true                                                      THEN 'Unreadable'
    WHEN p.extracted_dob IS NULL AND (p.extracted_ssn4 IS NULL OR p.extracted_ssn4 = '') THEN 'High Risk - No Anchors'
    WHEN p.extracted_dob IS NULL OR  p.extracted_ssn4 IS NULL OR p.extracted_ssn4 = ''  THEN 'Medium Risk - One Anchor'
    ELSE                                                                                     'Low Risk - Both Anchors'
  END AS doc_risk_tier
FROM analytics.doc_member_match_candidates m
LEFT JOIN curated.clinical_doc_parsed p ON m.doc_id = p.doc_id
LEFT JOIN raw.clinical_document d ON m.doc_id = d.doc_id;

-- ============================================================================
-- View 5: Score Distribution (histogram-ready buckets)
-- ============================================================================
CREATE OR REPLACE VIEW dashboard.v_score_distribution AS
SELECT
  match_classification                    AS match_class,
  ROUND(total_weight, 1)                  AS weight_bucket,
  COUNT(*)                                AS pair_count
FROM analytics.doc_member_match_candidates
GROUP BY match_classification, ROUND(total_weight, 1);

-- ============================================================================
-- View 6: Authorization Match Summary
-- ============================================================================
CREATE OR REPLACE VIEW dashboard.v_auth_match_summary AS
SELECT
  match_classification                    AS match_class,
  COUNT(*)                                AS auth_candidate_count,
  ROUND(AVG(total_weight), 4)             AS avg_weight,
  SUM(CASE WHEN auth_number_match     = true THEN 1 ELSE 0 END) AS direct_auth_matches,
  SUM(CASE WHEN provider_npi_match    = true THEN 1 ELSE 0 END) AS provider_npi_matches,
  SUM(CASE WHEN procedure_code_match  = true THEN 1 ELSE 0 END) AS procedure_code_matches,
  SUM(CASE WHEN service_date_overlap  = true THEN 1 ELSE 0 END) AS service_date_overlaps
FROM analytics.doc_auth_match_candidates
GROUP BY match_classification;

-- ============================================================================
-- View 7: Pipeline KPIs (single-row summary for counter widgets)
-- ============================================================================
CREATE OR REPLACE VIEW dashboard.v_pipeline_kpis AS
SELECT
  (SELECT COUNT(*) FROM ref.member)                                                                       AS total_members,
  (SELECT COUNT(*) FROM raw.authorization)                                                                AS total_authorizations,
  (SELECT COUNT(*) FROM raw.clinical_document)                                                            AS total_documents,
  (SELECT COUNT(*) FROM curated.clinical_doc_parsed)                                                      AS total_parsed,
  (SELECT SUM(CASE WHEN unreadable_flag = true THEN 1 ELSE 0 END) FROM curated.clinical_doc_parsed)      AS unreadable_docs,
  (SELECT ROUND(AVG(parse_confidence), 3) FROM curated.clinical_doc_parsed WHERE unreadable_flag = false) AS avg_confidence,
  (SELECT COUNT(*) FROM analytics.doc_member_match_candidates WHERE match_classification = 'match')       AS high_confidence_matches,
  (SELECT COUNT(*) FROM analytics.doc_member_match_candidates WHERE match_classification = 'possible_match') AS possible_matches,
  (SELECT COUNT(*) FROM analytics.doc_member_match_candidates WHERE match_classification = 'non_match')   AS non_matches,
  (SELECT COUNT(*) FROM analytics.doc_auth_match_candidates WHERE match_classification IN ('match','possible_match')) AS auth_matches,
  (SELECT COUNT(*) FROM analytics.match_events)                                                           AS total_events;
