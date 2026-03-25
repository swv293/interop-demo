-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 — Extract Structured Fields from Parsed Documents
-- MAGIC
-- MAGIC This notebook uses `ai_query()` to extract structured clinical fields
-- MAGIC (member name, DOB, SSN4, auth ID, provider, codes) from the `raw_text`
-- MAGIC column in `clinical_doc_parsed` using an LLM.
-- MAGIC
-- MAGIC **Pipeline Position:** Step 3 of 7 (after document parsing)
-- MAGIC
-- MAGIC **Key Databricks Features Used:**
-- MAGIC - `ai_query()` with `databricks-claude-sonnet-4` for structured JSON extraction
-- MAGIC - JSON parsing with `from_json()` / `:`  (colon) JSON path notation
-- MAGIC - Delta table MERGE for incremental updates
-- MAGIC
-- MAGIC **Inputs:**
-- MAGIC - `healthcare_demo.curated.clinical_doc_parsed` (raw_text column)
-- MAGIC
-- MAGIC **Outputs:**
-- MAGIC - Updated `clinical_doc_parsed` with extracted_* columns populated

-- COMMAND ----------

USE CATALOG healthcare_demo;
USE SCHEMA curated;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Extract Structured Fields Using ai_query
-- MAGIC
-- MAGIC We send the `raw_text` from each parsed document to an LLM with a carefully
-- MAGIC engineered prompt that asks for structured JSON output containing:
-- MAGIC - first_name, last_name, dob, ssn4
-- MAGIC - member_id, auth_id
-- MAGIC - provider_name, provider_npi
-- MAGIC - procedure_code, diagnosis_code

-- COMMAND ----------

-- Create a temporary view with LLM-extracted JSON from raw_text
-- Only process documents that haven't been extracted yet and are readable
CREATE OR REPLACE TEMPORARY VIEW extracted_fields_raw AS
SELECT
  doc_id,
  raw_text,
  ai_query(
    'databricks-claude-sonnet-4',
    CONCAT(
      'Extract the following fields from this clinical document text. ',
      'Return ONLY a valid JSON object with these exact keys. ',
      'If a field is not found, set its value to null. ',
      'Do not include any explanation, markdown formatting, or text outside the JSON object.\n\n',
      'Required JSON keys:\n',
      '- first_name: patient/member first name (string)\n',
      '- last_name: patient/member last name (string)\n',
      '- dob: date of birth in YYYY-MM-DD format (string)\n',
      '- ssn4: last 4 digits of SSN (string, preserve leading zeros)\n',
      '- member_id: member ID or MRN number (string)\n',
      '- auth_id: authorization number or auth reference (string)\n',
      '- provider_name: rendering/attending provider name (string)\n',
      '- provider_npi: provider NPI number, 10 digits (string)\n',
      '- procedure_code: CPT or HCPCS procedure code (string)\n',
      '- diagnosis_code: ICD-10 diagnosis code (string)\n\n',
      'Document text:\n',
      raw_text
    )
  ) AS extraction_json
FROM clinical_doc_parsed
WHERE unreadable_flag = false
  AND extracted_first_name IS NULL;  -- Only process un-extracted documents

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Parse the JSON Extraction Results
-- MAGIC
-- MAGIC Parse the LLM's JSON response into individual columns with data quality flags.

-- COMMAND ----------

-- Parse extracted JSON into structured columns with DQ flags
CREATE OR REPLACE TEMPORARY VIEW extracted_fields_parsed AS
SELECT
  doc_id,

  -- Parse JSON fields using colon notation
  extraction_json:first_name::STRING AS ext_first_name,
  extraction_json:last_name::STRING AS ext_last_name,
  extraction_json:dob::STRING AS ext_dob_str,
  extraction_json:ssn4::STRING AS ext_ssn4,
  extraction_json:member_id::STRING AS ext_member_id,
  extraction_json:auth_id::STRING AS ext_auth_id,
  extraction_json:provider_name::STRING AS ext_provider_name,
  extraction_json:provider_npi::STRING AS ext_provider_npi,
  extraction_json:procedure_code::STRING AS ext_procedure_code,
  extraction_json:diagnosis_code::STRING AS ext_diagnosis_code,

  -- Data quality flags
  CASE
    WHEN extraction_json:dob IS NULL THEN 'missing_dob'
    WHEN TRY_TO_DATE(extraction_json:dob::STRING, 'yyyy-MM-dd') IS NULL THEN 'invalid_dob_format'
    WHEN TRY_TO_DATE(extraction_json:dob::STRING, 'yyyy-MM-dd') > CURRENT_DATE() THEN 'future_dob'
    WHEN TRY_TO_DATE(extraction_json:dob::STRING, 'yyyy-MM-dd') < DATE '1900-01-01' THEN 'ancient_dob'
    ELSE 'valid'
  END AS dob_quality_flag,

  CASE
    WHEN extraction_json:ssn4 IS NULL THEN 'missing_ssn4'
    WHEN LENGTH(extraction_json:ssn4::STRING) != 4 THEN 'invalid_ssn4_length'
    WHEN extraction_json:ssn4::STRING NOT RLIKE '^[0-9]{4}$' THEN 'non_numeric_ssn4'
    ELSE 'valid'
  END AS ssn4_quality_flag,

  CASE
    WHEN extraction_json:diagnosis_code IS NULL THEN 'missing'
    WHEN extraction_json:diagnosis_code::STRING NOT RLIKE '^[A-Z][0-9A-Z]{2,6}$' THEN 'invalid_format'
    ELSE 'valid'
  END AS diagnosis_code_quality_flag

FROM extracted_fields_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Update clinical_doc_parsed with Extracted Fields
-- MAGIC
-- MAGIC Use MERGE to update the existing parsed document records with the newly extracted fields.

-- COMMAND ----------

-- MERGE extracted fields back into the main parsed table
MERGE INTO healthcare_demo.curated.clinical_doc_parsed AS target
USING extracted_fields_parsed AS source
ON target.doc_id = source.doc_id
WHEN MATCHED THEN UPDATE SET
  target.extracted_first_name      = TRIM(source.ext_first_name),
  target.extracted_last_name       = TRIM(source.ext_last_name),
  target.extracted_dob             = TRY_TO_DATE(source.ext_dob_str, 'yyyy-MM-dd'),
  target.extracted_ssn4            = LPAD(source.ext_ssn4, 4, '0'),
  target.extracted_auth_id         = TRIM(source.ext_auth_id),
  target.extracted_member_id_form  = TRIM(source.ext_member_id),
  target.extracted_provider_name   = TRIM(source.ext_provider_name),
  target.extracted_provider_npi    = TRIM(source.ext_provider_npi),
  target.extracted_procedure_code  = TRIM(source.ext_procedure_code),
  target.extracted_diagnosis_code  = TRIM(source.ext_diagnosis_code),
  target.extraction_model          = 'databricks-claude-sonnet-4',
  target.extraction_timestamp      = CURRENT_TIMESTAMP();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Data Quality Summary for Extracted Fields

-- COMMAND ----------

-- Overall extraction completeness
SELECT
  COUNT(*) AS total_docs,
  SUM(CASE WHEN extracted_first_name IS NOT NULL THEN 1 ELSE 0 END) AS has_first_name,
  SUM(CASE WHEN extracted_last_name IS NOT NULL THEN 1 ELSE 0 END) AS has_last_name,
  SUM(CASE WHEN extracted_dob IS NOT NULL THEN 1 ELSE 0 END) AS has_dob,
  SUM(CASE WHEN extracted_ssn4 IS NOT NULL THEN 1 ELSE 0 END) AS has_ssn4,
  SUM(CASE WHEN extracted_auth_id IS NOT NULL THEN 1 ELSE 0 END) AS has_auth_id,
  SUM(CASE WHEN extracted_member_id_form IS NOT NULL THEN 1 ELSE 0 END) AS has_member_id,
  SUM(CASE WHEN extracted_provider_npi IS NOT NULL THEN 1 ELSE 0 END) AS has_provider_npi,
  SUM(CASE WHEN extracted_procedure_code IS NOT NULL THEN 1 ELSE 0 END) AS has_procedure_code,
  SUM(CASE WHEN extracted_diagnosis_code IS NOT NULL THEN 1 ELSE 0 END) AS has_diagnosis_code
FROM healthcare_demo.curated.clinical_doc_parsed
WHERE unreadable_flag = false;

-- COMMAND ----------

-- DOB quality breakdown
SELECT
  dob_quality_flag,
  COUNT(*) AS cnt,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM extracted_fields_parsed
GROUP BY dob_quality_flag
ORDER BY cnt DESC;

-- COMMAND ----------

-- SSN4 quality breakdown
SELECT
  ssn4_quality_flag,
  COUNT(*) AS cnt,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM extracted_fields_parsed
GROUP BY ssn4_quality_flag
ORDER BY cnt DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Flag Documents Needing Manual Review
-- MAGIC
-- MAGIC Documents with missing critical fields (both DOB and SSN4 missing) are flagged
-- MAGIC for the manual intake queue.

-- COMMAND ----------

-- Identify documents that need manual review due to poor extraction
SELECT
  doc_id,
  extracted_first_name,
  extracted_last_name,
  extracted_dob,
  extracted_ssn4,
  parse_confidence,
  'missing_critical_fields' AS review_reason
FROM healthcare_demo.curated.clinical_doc_parsed
WHERE unreadable_flag = false
  AND extracted_dob IS NULL
  AND extracted_ssn4 IS NULL
ORDER BY ingest_ts DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC
-- MAGIC Proceed to **Notebook 04** to compute matching features (Jaro-Winkler similarity,
-- MAGIC blocking keys) between parsed documents and the member golden master.
