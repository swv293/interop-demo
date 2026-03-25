-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 — Fellegi-Sunter Probabilistic Scoring
-- MAGIC
-- MAGIC This notebook applies the Fellegi-Sunter probabilistic record linkage model
-- MAGIC to the candidate pair features computed in Notebook 04. It computes per-field
-- MAGIC log-likelihood ratio weights and total match scores, then classifies each pair
-- MAGIC as `match`, `possible_match`, or `non_match`.
-- MAGIC
-- MAGIC **Key Concepts:**
-- MAGIC - **m-probability**: P(fields agree | true match) — estimated from training pairs
-- MAGIC - **u-probability**: P(fields agree | non-match) — estimated from random pairs
-- MAGIC - **Agreement weight**: log2(m/u) × multiplier — high when field is discriminating
-- MAGIC - **Disagreement weight**: log2((1-m)/(1-u)) × multiplier — penalty for mismatch
-- MAGIC - **SSN4 and DOB get 3-5× the weight of name fields** per clinical matching requirements
-- MAGIC
-- MAGIC **Inputs:**
-- MAGIC - `healthcare_demo.analytics.doc_member_pairs_features` — feature vectors
-- MAGIC - `healthcare_demo.analytics.fellegi_sunter_parameters` — m/u probabilities
-- MAGIC
-- MAGIC **Outputs:**
-- MAGIC - `healthcare_demo.analytics.doc_member_match_candidates` — scored and classified matches

-- COMMAND ----------

USE CATALOG healthcare_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Initialize Fellegi-Sunter Parameters
-- MAGIC
-- MAGIC These m/u probabilities are tuned for healthcare member matching.
-- MAGIC SSN4 (5× multiplier) and DOB (4× multiplier) carry the heaviest weights
-- MAGIC because they are the most discriminating identifiers in clinical data.

-- COMMAND ----------

-- Insert default Fellegi-Sunter parameters if the table is empty
MERGE INTO analytics.fellegi_sunter_parameters AS target
USING (
  SELECT * FROM VALUES
    -- field_name, m_prob, u_prob, agree_wt, disagree_wt, multiplier, estimated_at
    ('first_name',  0.90, 0.010, 0.0, 0.0, 1.0, CURRENT_TIMESTAMP()),
    ('last_name',   0.92, 0.008, 0.0, 0.0, 1.0, CURRENT_TIMESTAMP()),
    ('dob',         0.95, 0.0001, 0.0, 0.0, 4.0, CURRENT_TIMESTAMP()),
    ('ssn4',        0.98, 0.001, 0.0, 0.0, 5.0, CURRENT_TIMESTAMP()),
    ('gender',      0.98, 0.500, 0.0, 0.0, 0.3, CURRENT_TIMESTAMP()),
    ('phone',       0.85, 0.0001, 0.0, 0.0, 1.5, CURRENT_TIMESTAMP()),
    ('city',        0.88, 0.020, 0.0, 0.0, 0.5, CURRENT_TIMESTAMP()),
    ('state',       0.95, 0.050, 0.0, 0.0, 0.3, CURRENT_TIMESTAMP()),
    ('zip',         0.90, 0.010, 0.0, 0.0, 0.8, CURRENT_TIMESTAMP())
  AS params(field_name, m_probability, u_probability, agreement_weight, disagreement_weight,
            field_weight_multiplier, last_estimated_at)
) AS source
ON target.field_name = source.field_name
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- Pre-compute agreement and disagreement weights from m/u probabilities
-- agreement_weight = log2(m/u) * multiplier
-- disagreement_weight = log2((1-m)/(1-u)) * multiplier
UPDATE analytics.fellegi_sunter_parameters
SET
  agreement_weight = LOG(2, m_probability / u_probability) * field_weight_multiplier,
  disagreement_weight = LOG(2, (1 - m_probability) / (1 - u_probability)) * field_weight_multiplier;

-- COMMAND ----------

-- Review computed weights
SELECT
  field_name,
  m_probability,
  u_probability,
  field_weight_multiplier,
  ROUND(agreement_weight, 3) AS agreement_weight,
  ROUND(disagreement_weight, 3) AS disagreement_weight,
  ROUND(agreement_weight - disagreement_weight, 3) AS weight_range
FROM analytics.fellegi_sunter_parameters
ORDER BY agreement_weight DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Compute Per-Field Weights for Each Candidate Pair
-- MAGIC
-- MAGIC For each field in a candidate pair:
-- MAGIC - If similarity score >= threshold → apply **agreement weight** × similarity score
-- MAGIC - If similarity score < threshold → apply **disagreement weight** × (1 - similarity)
-- MAGIC
-- MAGIC This produces a weighted contribution from each field, which we then sum.

-- COMMAND ----------

-- Compute per-field weights and total Fellegi-Sunter score
CREATE OR REPLACE TEMPORARY VIEW scored_pairs AS
WITH params AS (
  SELECT * FROM analytics.fellegi_sunter_parameters
),
fn_params AS (SELECT * FROM params WHERE field_name = 'first_name'),
ln_params AS (SELECT * FROM params WHERE field_name = 'last_name'),
dob_params AS (SELECT * FROM params WHERE field_name = 'dob'),
ssn4_params AS (SELECT * FROM params WHERE field_name = 'ssn4'),
gender_params AS (SELECT * FROM params WHERE field_name = 'gender'),
phone_params AS (SELECT * FROM params WHERE field_name = 'phone'),
city_params AS (SELECT * FROM params WHERE field_name = 'city'),
state_params AS (SELECT * FROM params WHERE field_name = 'state'),
zip_params AS (SELECT * FROM params WHERE field_name = 'zip')

SELECT
  f.pair_id,
  f.doc_id,
  f.member_id,
  f.blocking_key,

  -- Per-field similarity scores (from features table)
  f.jw_first_name,
  f.jw_last_name,
  f.dob_exact_match,
  f.ssn4_exact_match,

  -- Per-field Fellegi-Sunter weights
  -- First name weight
  CASE
    WHEN f.jw_first_name >= 0.85
    THEN fn_p.agreement_weight * f.jw_first_name
    ELSE fn_p.disagreement_weight * (1 - f.jw_first_name)
  END AS w_first_name,

  -- Last name weight
  CASE
    WHEN f.jw_last_name >= 0.85
    THEN ln_p.agreement_weight * f.jw_last_name
    ELSE ln_p.disagreement_weight * (1 - f.jw_last_name)
  END AS w_last_name,

  -- DOB weight (4× multiplier — heavy)
  CASE
    WHEN f.dob_exact_match = 1
    THEN dob_p.agreement_weight * 1.0
    WHEN f.dob_partial_match = 1
    THEN dob_p.agreement_weight * 0.5
    ELSE dob_p.disagreement_weight * 1.0
  END AS w_dob,

  -- SSN4 weight (5× multiplier — heaviest)
  CASE
    WHEN f.ssn4_exact_match = 1
    THEN ssn4_p.agreement_weight * 1.0
    ELSE ssn4_p.disagreement_weight * 1.0
  END AS w_ssn4,

  -- Gender weight
  CASE
    WHEN f.gender_match = 1 THEN gender_p.agreement_weight
    ELSE gender_p.disagreement_weight
  END AS w_gender,

  -- Phone weight
  CASE
    WHEN f.phone_match = 1 THEN phone_p.agreement_weight
    ELSE phone_p.disagreement_weight
  END AS w_phone,

  -- City weight
  CASE
    WHEN f.city_match = 1 THEN city_p.agreement_weight
    ELSE city_p.disagreement_weight
  END AS w_city,

  -- State weight
  CASE
    WHEN f.state_match = 1 THEN state_p.agreement_weight
    ELSE state_p.disagreement_weight
  END AS w_state,

  -- Zip weight
  CASE
    WHEN f.zip_match = 1 THEN zip_p.agreement_weight
    ELSE zip_p.disagreement_weight
  END AS w_zip

FROM analytics.doc_member_pairs_features f
CROSS JOIN fn_params fn_p
CROSS JOIN ln_params ln_p
CROSS JOIN dob_params dob_p
CROSS JOIN ssn4_params ssn4_p
CROSS JOIN gender_params gender_p
CROSS JOIN phone_params phone_p
CROSS JOIN city_params city_p
CROSS JOIN state_params state_p
CROSS JOIN zip_params zip_p;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Compute Total Weight & Classify Matches
-- MAGIC
-- MAGIC **Decision thresholds (Fellegi-Sunter):**
-- MAGIC - Total weight ≥ 12.0 → **match** (high confidence)
-- MAGIC - Total weight ≥ 6.0 → **possible_match** (needs review)
-- MAGIC - Total weight < 6.0 → **non_match**

-- COMMAND ----------

-- Final scored and classified match candidates
CREATE OR REPLACE TEMPORARY VIEW classified_matches AS
SELECT
  pair_id AS match_id,
  doc_id,
  member_id,
  blocking_key,

  -- Individual similarity scores
  jw_first_name AS jw_first_name_score,
  jw_last_name AS jw_last_name_score,
  CAST(dob_exact_match AS FLOAT) AS dob_match_score,
  CAST(ssn4_exact_match AS FLOAT) AS ssn4_match_score,
  CAST(0.0 AS FLOAT) AS gender_match_score,  -- placeholder
  CAST(0.0 AS FLOAT) AS phone_match_score,   -- placeholder
  CAST(0.0 AS FLOAT) AS city_match_score,    -- placeholder
  CAST(0.0 AS FLOAT) AS state_match_score,   -- placeholder
  CAST(0.0 AS FLOAT) AS zip_match_score,     -- placeholder

  -- Total Fellegi-Sunter weight (sum of all per-field weights)
  ROUND(
    w_first_name + w_last_name + w_dob + w_ssn4 +
    w_gender + w_phone + w_city + w_state + w_zip,
    4
  ) AS total_weight,

  -- Log-odds ratio (same as total weight in this formulation)
  ROUND(
    w_first_name + w_last_name + w_dob + w_ssn4 +
    w_gender + w_phone + w_city + w_state + w_zip,
    4
  ) AS fellegi_sunter_log_odds,

  -- Classification based on thresholds
  CASE
    WHEN (w_first_name + w_last_name + w_dob + w_ssn4 +
          w_gender + w_phone + w_city + w_state + w_zip) >= 12.0
    THEN 'match'
    WHEN (w_first_name + w_last_name + w_dob + w_ssn4 +
          w_gender + w_phone + w_city + w_state + w_zip) >= 6.0
    THEN 'possible_match'
    ELSE 'non_match'
  END AS match_classification,

  CURRENT_TIMESTAMP() AS created_at

FROM scored_pairs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Write Match Candidates to Delta Table

-- COMMAND ----------

-- Write classified matches to the output table
INSERT OVERWRITE analytics.doc_member_match_candidates
SELECT * FROM classified_matches;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Match Classification Summary

-- COMMAND ----------

-- Distribution of match classifications
SELECT
  match_classification,
  COUNT(*) AS pair_count,
  ROUND(AVG(total_weight), 2) AS avg_weight,
  ROUND(MIN(total_weight), 2) AS min_weight,
  ROUND(MAX(total_weight), 2) AS max_weight,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM analytics.doc_member_match_candidates
GROUP BY match_classification
ORDER BY avg_weight DESC;

-- COMMAND ----------

-- Top 20 highest-confidence matches
SELECT
  match_id,
  doc_id,
  member_id,
  ROUND(jw_first_name_score, 3) AS fn_sim,
  ROUND(jw_last_name_score, 3) AS ln_sim,
  dob_match_score,
  ssn4_match_score,
  ROUND(total_weight, 2) AS total_wt,
  match_classification
FROM analytics.doc_member_match_candidates
WHERE match_classification = 'match'
ORDER BY total_weight DESC
LIMIT 20;

-- COMMAND ----------

-- Documents with multiple possible matches (review queue)
SELECT
  doc_id,
  COUNT(*) AS match_count,
  MAX(total_weight) AS best_score,
  MIN(total_weight) AS worst_score
FROM analytics.doc_member_match_candidates
WHERE match_classification IN ('match', 'possible_match')
GROUP BY doc_id
HAVING COUNT(*) > 1
ORDER BY match_count DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Generate Authorization Match Candidates
-- MAGIC
-- MAGIC For documents matched to a member, check if they also match to an open authorization.

-- COMMAND ----------

-- Cross-reference matched documents against authorization records
INSERT OVERWRITE analytics.doc_auth_match_candidates
SELECT
  MD5(CONCAT(mc.doc_id, '|', a.auth_id)) AS match_id,
  mc.doc_id,
  a.auth_id,
  mc.match_id AS member_match_id,

  -- Auth-level matching features
  CASE WHEN cdp.extracted_auth_id = a.auth_number THEN true ELSE false END AS auth_number_match,

  -- Service date overlap check
  CASE
    WHEN a.service_from_date IS NOT NULL
     AND a.service_to_date IS NOT NULL
     AND cd.received_timestamp BETWEEN a.service_from_date AND DATE_ADD(a.service_to_date, 3)
    THEN true
    ELSE false
  END AS service_date_overlap,

  -- Procedure code match
  CASE WHEN cdp.extracted_procedure_code = a.procedure_code THEN true ELSE false END AS procedure_code_match,

  -- Provider NPI match
  CASE WHEN cdp.extracted_provider_npi = a.rendering_provider_npi THEN true ELSE false END AS provider_npi_match,

  -- Total auth match weight
  ROUND(
    (CASE WHEN cdp.extracted_auth_id = a.auth_number THEN 10.0 ELSE 0.0 END) +
    (CASE WHEN cdp.extracted_procedure_code = a.procedure_code THEN 3.0 ELSE 0.0 END) +
    (CASE WHEN cdp.extracted_provider_npi = a.rendering_provider_npi THEN 4.0 ELSE 0.0 END) +
    (CASE WHEN a.service_from_date IS NOT NULL
          AND cd.received_timestamp BETWEEN a.service_from_date AND DATE_ADD(a.service_to_date, 3)
     THEN 2.0 ELSE 0.0 END),
    2
  ) AS total_weight,

  -- Classification
  CASE
    WHEN cdp.extracted_auth_id = a.auth_number THEN 'match'
    WHEN (CASE WHEN cdp.extracted_procedure_code = a.procedure_code THEN 3.0 ELSE 0.0 END) +
         (CASE WHEN cdp.extracted_provider_npi = a.rendering_provider_npi THEN 4.0 ELSE 0.0 END) >= 5.0
    THEN 'possible_match'
    ELSE 'non_match'
  END AS match_classification,

  CURRENT_TIMESTAMP() AS created_at

FROM analytics.doc_member_match_candidates mc
JOIN raw.authorization a ON mc.member_id = a.member_id
JOIN curated.clinical_doc_parsed cdp ON mc.doc_id = cdp.doc_id
LEFT JOIN raw.clinical_document cd ON mc.doc_id = cd.doc_id
WHERE mc.match_classification IN ('match', 'possible_match')
  AND a.status IN ('Pending', 'Approved', 'Pended');

-- COMMAND ----------

-- Auth match summary
SELECT
  match_classification,
  COUNT(*) AS cnt,
  SUM(CASE WHEN auth_number_match THEN 1 ELSE 0 END) AS direct_auth_matches,
  SUM(CASE WHEN provider_npi_match THEN 1 ELSE 0 END) AS provider_matches,
  SUM(CASE WHEN procedure_code_match THEN 1 ELSE 0 END) AS procedure_matches
FROM analytics.doc_auth_match_candidates
GROUP BY match_classification;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC
-- MAGIC Proceed to **Notebook 06** for data quality checks and PII masking,
-- MAGIC then **Notebook 07** to publish match results as events.
