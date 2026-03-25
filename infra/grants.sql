-- ============================================================================
-- Healthcare Clinical Document Matching System
-- Roles, Grants, Column Masking, and Masking Views
-- Target: fevm-serverless-stable-swv01.cloud.databricks.com
-- Catalog: healthcare_demo
-- ============================================================================

USE CATALOG healthcare_demo;

-- ============================================================================
-- SECTION 1: GROUP CREATION (Databricks account-level groups)
-- NOTE: Groups are created at the account level via SCIM, Databricks UI,
-- or Terraform. The statements below document the expected groups.
-- Uncomment if running via account-level admin API.
-- ============================================================================
-- CREATE GROUP IF NOT EXISTS healthcare_demo_admin;
-- CREATE GROUP IF NOT EXISTS healthcare_demo_analyst;
-- CREATE GROUP IF NOT EXISTS healthcare_demo_clinical;
-- CREATE GROUP IF NOT EXISTS healthcare_demo_intake;

-- ============================================================================
-- SECTION 2: COLUMN MASKING FUNCTIONS
-- Databricks SQL UDFs used with column masking policies to protect PHI/PII.
-- ============================================================================

-- Mask SSN4: returns '****' for non-privileged users
CREATE OR REPLACE FUNCTION healthcare_demo.ref.mask_ssn4(ssn4_value STRING)
RETURNS STRING
COMMENT 'Column masking function for SSN last-4. Returns the original value for members of healthcare_demo_clinical group; returns **** for all others.'
RETURN
  CASE
    WHEN is_account_group_member('healthcare_demo_clinical') THEN ssn4_value
    WHEN is_account_group_member('healthcare_demo_admin') THEN ssn4_value
    ELSE '****'
  END;

-- Mask DOB: returns year only for non-privileged users
CREATE OR REPLACE FUNCTION healthcare_demo.ref.mask_dob(dob_value DATE)
RETURNS DATE
COMMENT 'Column masking function for date of birth. Returns the exact date for clinical/admin users; returns January 1 of the birth year for all others.'
RETURN
  CASE
    WHEN is_account_group_member('healthcare_demo_clinical') THEN dob_value
    WHEN is_account_group_member('healthcare_demo_admin') THEN dob_value
    ELSE MAKE_DATE(YEAR(dob_value), 1, 1)
  END;

-- Mask phone: returns last 4 digits only for non-privileged users
CREATE OR REPLACE FUNCTION healthcare_demo.ref.mask_phone(phone_value STRING)
RETURNS STRING
COMMENT 'Column masking function for phone numbers. Returns the full phone for clinical/admin users; returns ***-***-XXXX (last 4 digits) for all others.'
RETURN
  CASE
    WHEN is_account_group_member('healthcare_demo_clinical') THEN phone_value
    WHEN is_account_group_member('healthcare_demo_admin') THEN phone_value
    ELSE CONCAT('***-***-', RIGHT(phone_value, 4))
  END;

-- Mask email: returns masked email for non-privileged users
CREATE OR REPLACE FUNCTION healthcare_demo.ref.mask_email(email_value STRING)
RETURNS STRING
COMMENT 'Column masking function for email addresses. Returns full email for clinical/admin; returns first character + ***@domain for others.'
RETURN
  CASE
    WHEN is_account_group_member('healthcare_demo_clinical') THEN email_value
    WHEN is_account_group_member('healthcare_demo_admin') THEN email_value
    ELSE CONCAT(LEFT(email_value, 1), '***@', SPLIT(email_value, '@')[1])
  END;

-- Mask free-text name fields: returns first initial + asterisks
CREATE OR REPLACE FUNCTION healthcare_demo.ref.mask_name(name_value STRING)
RETURNS STRING
COMMENT 'Column masking function for name fields. Returns full name for clinical/admin; returns first initial followed by asterisks for others.'
RETURN
  CASE
    WHEN is_account_group_member('healthcare_demo_clinical') THEN name_value
    WHEN is_account_group_member('healthcare_demo_admin') THEN name_value
    ELSE CONCAT(LEFT(name_value, 1), '****')
  END;

-- ============================================================================
-- SECTION 3: APPLY COLUMN MASKS TO TABLES
-- ============================================================================

-- ref.member column masks
ALTER TABLE healthcare_demo.ref.member
  ALTER COLUMN ssn4 SET MASK healthcare_demo.ref.mask_ssn4;

ALTER TABLE healthcare_demo.ref.member
  ALTER COLUMN dob SET MASK healthcare_demo.ref.mask_dob;

ALTER TABLE healthcare_demo.ref.member
  ALTER COLUMN phone SET MASK healthcare_demo.ref.mask_phone;

ALTER TABLE healthcare_demo.ref.member
  ALTER COLUMN email SET MASK healthcare_demo.ref.mask_email;

ALTER TABLE healthcare_demo.ref.member
  ALTER COLUMN first_name SET MASK healthcare_demo.ref.mask_name;

ALTER TABLE healthcare_demo.ref.member
  ALTER COLUMN last_name SET MASK healthcare_demo.ref.mask_name;

-- curated.clinical_doc_parsed column masks
ALTER TABLE healthcare_demo.curated.clinical_doc_parsed
  ALTER COLUMN extracted_ssn4 SET MASK healthcare_demo.ref.mask_ssn4;

ALTER TABLE healthcare_demo.curated.clinical_doc_parsed
  ALTER COLUMN extracted_dob SET MASK healthcare_demo.ref.mask_dob;

ALTER TABLE healthcare_demo.curated.clinical_doc_parsed
  ALTER COLUMN extracted_first_name SET MASK healthcare_demo.ref.mask_name;

ALTER TABLE healthcare_demo.curated.clinical_doc_parsed
  ALTER COLUMN extracted_last_name SET MASK healthcare_demo.ref.mask_name;

-- ============================================================================
-- SECTION 4: MASKING VIEWS FOR NON-PRIVILEGED ROLES
-- These views provide an additional layer of access control and can be
-- granted to roles that should never see raw PHI columns at all.
-- ============================================================================

CREATE OR REPLACE VIEW healthcare_demo.analytics.v_member_masked AS
SELECT
  member_id,
  healthcare_demo.ref.mask_name(first_name)   AS first_name,
  healthcare_demo.ref.mask_name(last_name)    AS last_name,
  middle_initial,
  healthcare_demo.ref.mask_dob(dob)           AS dob,
  gender,
  healthcare_demo.ref.mask_ssn4(ssn4)         AS ssn4,
  address_line1,
  city,
  state,
  zip,
  healthcare_demo.ref.mask_phone(phone)       AS phone,
  healthcare_demo.ref.mask_email(email)       AS email,
  preferred_language,
  race_ethnicity,
  source_system
FROM healthcare_demo.ref.member;

CREATE OR REPLACE VIEW healthcare_demo.analytics.v_clinical_doc_parsed_masked AS
SELECT
  doc_id,
  file_path,
  ingest_ts,
  -- raw_text intentionally excluded (contains full PHI)
  healthcare_demo.ref.mask_name(extracted_first_name)   AS extracted_first_name,
  healthcare_demo.ref.mask_name(extracted_last_name)    AS extracted_last_name,
  healthcare_demo.ref.mask_dob(extracted_dob)           AS extracted_dob,
  healthcare_demo.ref.mask_ssn4(extracted_ssn4)         AS extracted_ssn4,
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
FROM healthcare_demo.curated.clinical_doc_parsed;

CREATE OR REPLACE VIEW healthcare_demo.analytics.v_doc_member_pairs_masked AS
SELECT
  pair_id,
  doc_id,
  member_id,
  blocking_key,
  healthcare_demo.ref.mask_name(doc_first_name)     AS doc_first_name,
  healthcare_demo.ref.mask_name(doc_last_name)      AS doc_last_name,
  '****'                                             AS doc_dob,
  healthcare_demo.ref.mask_ssn4(doc_ssn4)           AS doc_ssn4,
  healthcare_demo.ref.mask_name(member_first_name)  AS member_first_name,
  healthcare_demo.ref.mask_name(member_last_name)   AS member_last_name,
  '****'                                             AS member_dob,
  healthcare_demo.ref.mask_ssn4(member_ssn4)        AS member_ssn4,
  jw_first_name,
  jw_last_name,
  dob_exact_match,
  dob_partial_match,
  ssn4_exact_match,
  gender_match,
  phone_match,
  city_match,
  state_match,
  zip_match,
  created_at
FROM healthcare_demo.analytics.doc_member_pairs_features;

-- ============================================================================
-- SECTION 5: GRANTS - healthcare_demo_admin
-- Full access to catalog, all schemas, all tables, and volumes.
-- ============================================================================
GRANT USE CATALOG ON CATALOG healthcare_demo TO healthcare_demo_admin;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.ref TO healthcare_demo_admin;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.raw TO healthcare_demo_admin;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.curated TO healthcare_demo_admin;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.analytics TO healthcare_demo_admin;

GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA healthcare_demo.ref TO healthcare_demo_admin;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA healthcare_demo.raw TO healthcare_demo_admin;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA healthcare_demo.curated TO healthcare_demo_admin;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA healthcare_demo.analytics TO healthcare_demo_admin;

GRANT ALL PRIVILEGES ON VOLUME healthcare_demo.raw.raw_docs TO healthcare_demo_admin;

GRANT EXECUTE ON FUNCTION healthcare_demo.ref.mask_ssn4 TO healthcare_demo_admin;
GRANT EXECUTE ON FUNCTION healthcare_demo.ref.mask_dob TO healthcare_demo_admin;
GRANT EXECUTE ON FUNCTION healthcare_demo.ref.mask_phone TO healthcare_demo_admin;
GRANT EXECUTE ON FUNCTION healthcare_demo.ref.mask_email TO healthcare_demo_admin;
GRANT EXECUTE ON FUNCTION healthcare_demo.ref.mask_name TO healthcare_demo_admin;

-- ============================================================================
-- SECTION 6: GRANTS - healthcare_demo_analyst
-- Read access to curated + analytics only. No access to raw PII.
-- Uses masking views instead of direct table access for PHI fields.
-- ============================================================================
GRANT USE CATALOG ON CATALOG healthcare_demo TO healthcare_demo_analyst;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.curated TO healthcare_demo_analyst;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.analytics TO healthcare_demo_analyst;

-- Curated: read via masking view, not direct table
GRANT SELECT ON VIEW healthcare_demo.analytics.v_clinical_doc_parsed_masked TO healthcare_demo_analyst;

-- Analytics: read all analytics tables (match scores, not raw PII)
GRANT SELECT ON TABLE healthcare_demo.analytics.doc_member_match_candidates TO healthcare_demo_analyst;
GRANT SELECT ON TABLE healthcare_demo.analytics.doc_auth_match_candidates TO healthcare_demo_analyst;
GRANT SELECT ON TABLE healthcare_demo.analytics.match_events TO healthcare_demo_analyst;
GRANT SELECT ON TABLE healthcare_demo.analytics.fellegi_sunter_parameters TO healthcare_demo_analyst;

-- Feature pairs via masking view only
GRANT SELECT ON VIEW healthcare_demo.analytics.v_doc_member_pairs_masked TO healthcare_demo_analyst;

-- Member data via masking view only
GRANT SELECT ON VIEW healthcare_demo.analytics.v_member_masked TO healthcare_demo_analyst;

-- ============================================================================
-- SECTION 7: GRANTS - healthcare_demo_clinical
-- Read access to ALL schemas including raw PHI. Clinical reviewers need
-- full visibility for manual matching and quality review.
-- ============================================================================
GRANT USE CATALOG ON CATALOG healthcare_demo TO healthcare_demo_clinical;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.ref TO healthcare_demo_clinical;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.raw TO healthcare_demo_clinical;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.curated TO healthcare_demo_clinical;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.analytics TO healthcare_demo_clinical;

GRANT SELECT ON TABLE healthcare_demo.ref.member TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.raw.authorization TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.raw.auth_procedure TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.raw.auth_status_history TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.raw.clinical_document TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.curated.clinical_doc_parsed TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.analytics.doc_member_match_candidates TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.analytics.doc_auth_match_candidates TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.analytics.doc_member_pairs_features TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.analytics.match_events TO healthcare_demo_clinical;
GRANT SELECT ON TABLE healthcare_demo.analytics.fellegi_sunter_parameters TO healthcare_demo_clinical;

GRANT READ VOLUME ON VOLUME healthcare_demo.raw.raw_docs TO healthcare_demo_clinical;

-- ============================================================================
-- SECTION 8: GRANTS - healthcare_demo_intake
-- Read/write access to raw schema only. Used by intake pipelines and
-- document ingestion processes.
-- ============================================================================
GRANT USE CATALOG ON CATALOG healthcare_demo TO healthcare_demo_intake;
GRANT USE SCHEMA ON SCHEMA healthcare_demo.raw TO healthcare_demo_intake;

GRANT SELECT, MODIFY ON TABLE healthcare_demo.raw.authorization TO healthcare_demo_intake;
GRANT SELECT, MODIFY ON TABLE healthcare_demo.raw.auth_procedure TO healthcare_demo_intake;
GRANT SELECT, MODIFY ON TABLE healthcare_demo.raw.auth_status_history TO healthcare_demo_intake;
GRANT SELECT, MODIFY ON TABLE healthcare_demo.raw.clinical_document TO healthcare_demo_intake;

GRANT READ VOLUME, WRITE VOLUME ON VOLUME healthcare_demo.raw.raw_docs TO healthcare_demo_intake;

-- ============================================================================
-- END OF GRANTS
-- ============================================================================
