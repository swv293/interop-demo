-- ============================================================================
-- Healthcare Clinical Document Matching System
-- Unity Catalog DDL
-- Target: fevm-serverless-stable-swv01.cloud.databricks.com
-- Catalog: healthcare_demo
-- Schemas: ref, raw, curated, analytics
-- ============================================================================

-- ============================================================================
-- CATALOG
-- ============================================================================
CREATE CATALOG IF NOT EXISTS healthcare_demo
COMMENT 'Healthcare clinical document matching and prior authorization system. Contains member reference data, authorization records, clinical document metadata, and probabilistic matching analytics.';

USE CATALOG healthcare_demo;

-- ============================================================================
-- SCHEMAS
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS ref
COMMENT 'Reference / golden-master data. Contains canonical member records used as the single source of truth for probabilistic matching.';

CREATE SCHEMA IF NOT EXISTS raw
COMMENT 'Raw ingestion layer. Stores authorization requests, clinical document metadata, and supporting child tables as received from upstream source systems.';

CREATE SCHEMA IF NOT EXISTS curated
COMMENT 'Curated / enriched layer. Contains OCR-parsed and normalized clinical document extracts ready for matching pipelines.';

CREATE SCHEMA IF NOT EXISTS analytics
COMMENT 'Analytics layer. Contains match candidate pairs, Fellegi-Sunter parameters, feature vectors, and match event logs.';

-- ============================================================================
-- TABLE: ref.member (Golden Master Member Record)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.ref.member (
  member_id         STRING    NOT NULL  COMMENT 'UUID surrogate key uniquely identifying a member across all systems. -- PHI/PII: Protected',
  first_name        STRING              COMMENT 'Member legal first name as recorded in the enrollment system. -- PHI/PII: Protected',
  last_name         STRING              COMMENT 'Member legal last name as recorded in the enrollment system. -- PHI/PII: Protected',
  middle_initial    STRING              COMMENT 'Member middle initial, single character. -- PHI/PII: Protected',
  dob               DATE                COMMENT 'Member date of birth (YYYY-MM-DD). Critical matching field. -- PHI/PII: Protected',
  gender            STRING              COMMENT 'Member gender code: M=Male, F=Female, U=Unknown, O=Other.',
  ssn4              STRING              COMMENT 'Last four digits of the member Social Security Number. Used as a high-weight matching signal. -- PHI/PII: Protected',
  address_line1     STRING              COMMENT 'Primary street address line. -- PHI/PII: Protected',
  city              STRING              COMMENT 'City of primary residence.',
  state             STRING              COMMENT 'Two-letter US state abbreviation of primary residence.',
  zip               STRING              COMMENT 'Five-digit or ZIP+4 postal code of primary residence.',
  phone             STRING              COMMENT 'Primary contact phone number in E.164 or 10-digit format. -- PHI/PII: Protected',
  email             STRING              COMMENT 'Primary contact email address. -- PHI/PII: Protected',
  preferred_language STRING             COMMENT 'ISO 639-1 language code for the members preferred communication language.',
  race_ethnicity    STRING              COMMENT 'Self-reported race/ethnicity category per CMS standards.',
  created_at        TIMESTAMP           COMMENT 'Timestamp when this member record was first created in the golden master.',
  updated_at        TIMESTAMP           COMMENT 'Timestamp of the most recent update to this member record. Maintained by ETL.',
  source_system     STRING              COMMENT 'Identifier of the upstream source system that originated or last refreshed this record (e.g. EPIC, FACETS).'
)
COMMENT 'Golden master member reference table. Single source of truth for member demographics used in clinical document probabilistic matching. Each row represents one unique health plan member.'
TBLPROPERTIES (
  'contains_pii'  = 'true',
  'contains_phi'  = 'true',
  'domain'        = 'member_management',
  'quality.tier'  = 'gold',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- TABLE: raw.authorization (Prior Auth Request)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.raw.authorization (
  auth_id                   STRING    NOT NULL  COMMENT 'UUID surrogate key uniquely identifying this authorization request.',
  auth_number               STRING              COMMENT 'Human-readable business key for the authorization (e.g. AUTH-2026-000123). Used in all external communications.',
  member_id                 STRING              COMMENT 'FK to ref.member. The member for whom this authorization is requested. -- PHI/PII: Protected',
  service_from_date         DATE                COMMENT 'Requested service start date. Defines the beginning of the authorized service window.',
  service_to_date           DATE                COMMENT 'Requested service end date. Defines the end of the authorized service window.',
  procedure_code            STRING              COMMENT 'Primary CPT/HCPCS procedure code requested for authorization.',
  procedure_modifier        STRING              COMMENT 'Procedure modifier code (e.g. 26, TC, LT, RT) qualifying the primary procedure.',
  diagnosis_code            STRING              COMMENT 'Primary ICD-10-CM diagnosis code supporting medical necessity.',
  rendering_provider_npi    STRING              COMMENT 'NPI of the rendering/servicing provider who will perform the procedure.',
  rendering_provider_name   STRING              COMMENT 'Full name of the rendering provider as submitted on the authorization request.',
  status                    STRING              COMMENT 'Current authorization status. Valid values: Pending, Approved, Denied, Partial, Cancelled, Expired, Pended.',
  approved_units            DECIMAL(8,2)        COMMENT 'Number of approved service units (visits, hours, etc). NULL if not yet adjudicated.',
  denial_reason_code        STRING              COMMENT 'Standardized denial reason code when status is Denied or Partial.',
  auth_requested_date       DATE                COMMENT 'Date the authorization request was received from the provider.',
  auth_decision_date        DATE                COMMENT 'Date the clinical reviewer made the authorization decision.',
  clinical_doc_id           STRING              COMMENT 'FK to raw.clinical_document. Reference to the matched clinical document supporting this auth.',
  doc_match_method          STRING              COMMENT 'Method used to match clinical documents to this authorization: CoverSheet, OCR, FaxMeta, or Manual.',
  doc_received_date         TIMESTAMP           COMMENT 'Timestamp when the supporting clinical documentation was received.',
  pend_reason               STRING              COMMENT 'Reason the authorization was pended (e.g. missing_clinical_docs, incomplete_form, additional_info_needed).',
  additional_info_due_date  DATE                COMMENT 'Deadline by which additional information must be received before auto-denial.',
  created_at                TIMESTAMP           COMMENT 'Timestamp when this authorization record was first created.',
  updated_at                TIMESTAMP           COMMENT 'Timestamp of the most recent update to this authorization record. Maintained by ETL.',
  created_by                STRING              COMMENT 'User or service account that created this authorization record.'
)
COMMENT 'Prior authorization request records. Each row represents a single auth request linked to a member and optionally to a clinical document. Tracks the full lifecycle from submission through adjudication.'
TBLPROPERTIES (
  'contains_pii'  = 'true',
  'contains_phi'  = 'true',
  'domain'        = 'prior_authorization',
  'quality.tier'  = 'bronze',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- TABLE: raw.auth_procedure (Child table - procedures per auth)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.raw.auth_procedure (
  auth_id           STRING    NOT NULL  COMMENT 'FK to raw.authorization. Parent authorization request.',
  procedure_code    STRING    NOT NULL  COMMENT 'CPT/HCPCS procedure code associated with this authorization.',
  procedure_seq     INT                 COMMENT 'Sequence number indicating the ordinal position of this procedure within the authorization (1-based).'
)
COMMENT 'Child table storing one or more procedure codes per authorization request. Supports multi-procedure prior auth submissions.'
TBLPROPERTIES (
  'contains_pii'  = 'false',
  'contains_phi'  = 'false',
  'domain'        = 'prior_authorization',
  'quality.tier'  = 'bronze'
);

-- ============================================================================
-- TABLE: raw.auth_status_history (Audit trail of status changes)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.raw.auth_status_history (
  history_id    BIGINT GENERATED ALWAYS AS IDENTITY  COMMENT 'Auto-generated surrogate key for each status change event.',
  auth_id       STRING    NOT NULL                    COMMENT 'FK to raw.authorization. The authorization whose status changed.',
  old_status    STRING                                COMMENT 'Previous authorization status value before the change.',
  new_status    STRING                                COMMENT 'New authorization status value after the change.',
  changed_by    STRING                                COMMENT 'User or service account that triggered the status change.',
  changed_at    TIMESTAMP                             COMMENT 'Timestamp when the status change occurred.',
  change_reason STRING                                COMMENT 'Free-text or coded reason explaining why the status was changed.'
)
COMMENT 'Immutable audit log of authorization status transitions. Every status change on raw.authorization is captured here for compliance and analytics.'
TBLPROPERTIES (
  'contains_pii'  = 'false',
  'contains_phi'  = 'false',
  'domain'        = 'prior_authorization',
  'quality.tier'  = 'bronze'
);

-- ============================================================================
-- TABLE: raw.clinical_document (TIFF / document metadata)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.raw.clinical_document (
  doc_id                STRING    NOT NULL  COMMENT 'UUID surrogate key uniquely identifying this clinical document.',
  auth_id               STRING              COMMENT 'FK to raw.authorization. The authorization this document supports (NULL if unmatched).',
  member_id             STRING              COMMENT 'FK to ref.member. The member this document belongs to (NULL if unmatched). -- PHI/PII: Protected',
  document_type         STRING              COMMENT 'Classification of the document: prior_auth_form, clinical_note, lab_result, imaging_report, discharge_summary, or other.',
  file_name             STRING              COMMENT 'Original file name as received from the source channel.',
  file_path             STRING              COMMENT 'Unity Catalog Volume path where the document binary is stored (e.g. /Volumes/healthcare_demo/raw/raw_docs/...).',
  file_format           STRING              COMMENT 'File format of the stored document: TIFF, PDF, or PNG.',
  file_size_bytes       BIGINT              COMMENT 'Size of the document file in bytes.',
  page_count            INT                 COMMENT 'Total number of pages in the document.',
  source_channel        STRING              COMMENT 'Channel through which the document was received: fax, electronic, upload, or mail.',
  sender_fax_number     STRING              COMMENT 'Fax number of the sending party, if received via fax. -- PHI/PII: Protected',
  sender_provider_npi   STRING              COMMENT 'NPI of the provider who sent the document.',
  received_timestamp    TIMESTAMP           COMMENT 'Timestamp when the document was received by the intake system.',
  ingestion_timestamp   TIMESTAMP           COMMENT 'Timestamp when the document was ingested into the data platform.',
  mrm_batch_id          STRING              COMMENT 'Medical Records Management batch identifier for grouping documents received together.',
  cover_sheet_detected  BOOLEAN             COMMENT 'TRUE if a fax cover sheet was detected as the first page of the document.',
  ocr_status            STRING              COMMENT 'OCR processing status: pending, completed, failed, or partial.',
  ocr_confidence_score  FLOAT               COMMENT 'Overall OCR engine confidence score (0.0 to 1.0) for the extracted text.',
  match_status          STRING              COMMENT 'Document-to-member/auth matching status: matched, unmatched, pending_review, or manual_matched.',
  doc_match_method      STRING              COMMENT 'Method used for matching: CoverSheet, OCR, FaxMeta, or Manual.',
  match_confidence_score FLOAT              COMMENT 'Confidence score (0.0 to 1.0) of the document match to a member/authorization.',
  matched_by            STRING              COMMENT 'User or automated system that performed or confirmed the match.',
  matched_at            TIMESTAMP           COMMENT 'Timestamp when the document was matched to a member/authorization.',
  is_readable           BOOLEAN             COMMENT 'TRUE if the document passed quality checks and is readable by OCR.',
  quality_score         FLOAT               COMMENT 'Image quality score (0.0 to 1.0) based on DPI, contrast, skew, and noise analysis.',
  notes                 STRING              COMMENT 'Free-text notes added by clinical staff or automated processes.',
  created_at            TIMESTAMP           COMMENT 'Timestamp when this document metadata record was first created.',
  updated_at            TIMESTAMP           COMMENT 'Timestamp of the most recent update to this document record. Maintained by ETL.'
)
COMMENT 'Metadata table for clinical documents (TIFF, PDF, PNG) received via fax, electronic submission, upload, or mail. Tracks document lifecycle from receipt through OCR processing and matching to members/authorizations.'
TBLPROPERTIES (
  'contains_pii'  = 'true',
  'contains_phi'  = 'true',
  'domain'        = 'clinical_documents',
  'quality.tier'  = 'bronze',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- TABLE: curated.clinical_doc_parsed (OCR-extracted fields)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.curated.clinical_doc_parsed (
  doc_id                    STRING    NOT NULL  COMMENT 'FK to raw.clinical_document. The source document that was parsed.',
  file_path                 STRING              COMMENT 'Unity Catalog Volume path to the source document file.',
  ingest_ts                 TIMESTAMP           COMMENT 'Timestamp when the document was ingested into the parsing pipeline.',
  raw_text                  STRING              COMMENT 'Full raw text extracted by the OCR engine from the document. -- PHI/PII: Protected',
  extracted_first_name      STRING              COMMENT 'Patient first name extracted from the document via OCR/NLP. -- PHI/PII: Protected',
  extracted_last_name       STRING              COMMENT 'Patient last name extracted from the document via OCR/NLP. -- PHI/PII: Protected',
  extracted_dob             DATE                COMMENT 'Patient date of birth extracted from the document. -- PHI/PII: Protected',
  extracted_ssn4            STRING              COMMENT 'Last four digits of patient SSN extracted from the document. -- PHI/PII: Protected',
  extracted_auth_id         STRING              COMMENT 'Authorization ID or number extracted from the document (e.g. from a cover sheet or form field).',
  extracted_member_id_form  STRING              COMMENT 'Member ID as printed on the submitted form, extracted via OCR. -- PHI/PII: Protected',
  extracted_provider_name   STRING              COMMENT 'Rendering or referring provider name extracted from the document.',
  extracted_provider_npi    STRING              COMMENT 'Provider NPI extracted from the document.',
  extracted_procedure_code  STRING              COMMENT 'CPT/HCPCS procedure code extracted from the document.',
  extracted_diagnosis_code  STRING              COMMENT 'ICD-10-CM diagnosis code extracted from the document.',
  parse_error_status        STRING              COMMENT 'Error status from the parsing pipeline: NULL if successful, otherwise an error code/message.',
  parse_confidence          FLOAT               COMMENT 'Overall confidence score (0.0 to 1.0) of the parsing/extraction across all fields.',
  unreadable_flag           BOOLEAN             COMMENT 'TRUE if the document was determined to be unreadable or below quality thresholds.',
  extraction_model          STRING              COMMENT 'Name and version of the OCR/NLP model used for extraction (e.g. tesseract-5.3, azure-di-v4).',
  extraction_timestamp      TIMESTAMP           COMMENT 'Timestamp when the extraction process completed.',
  created_at                TIMESTAMP           COMMENT 'Timestamp when this parsed record was first created.'
)
COMMENT 'Curated table containing structured fields extracted from clinical documents via OCR and NLP. Each row represents parsed output from a single clinical document, used as input to the probabilistic matching pipeline.'
TBLPROPERTIES (
  'contains_pii'  = 'true',
  'contains_phi'  = 'true',
  'domain'        = 'clinical_documents',
  'quality.tier'  = 'silver',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- TABLE: analytics.doc_member_match_candidates
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.analytics.doc_member_match_candidates (
  match_id                STRING    NOT NULL  COMMENT 'UUID surrogate key uniquely identifying this candidate match pair.',
  doc_id                  STRING    NOT NULL  COMMENT 'FK to raw.clinical_document. The clinical document in this candidate pair.',
  member_id               STRING    NOT NULL  COMMENT 'FK to ref.member. The candidate member in this match pair. -- PHI/PII: Protected',
  blocking_key            STRING              COMMENT 'The blocking key value used to generate this candidate pair (e.g. soundex_last+dob_year).',
  jw_first_name_score     FLOAT               COMMENT 'Jaro-Winkler similarity score (0.0 to 1.0) between extracted and member first names.',
  jw_last_name_score      FLOAT               COMMENT 'Jaro-Winkler similarity score (0.0 to 1.0) between extracted and member last names.',
  dob_match_score         FLOAT               COMMENT 'Date of birth match score: 1.0 for exact, partial credit for transpositions or partial matches.',
  ssn4_match_score        FLOAT               COMMENT 'SSN last-4 match score: 1.0 for exact match, 0.0 otherwise.',
  gender_match_score      FLOAT               COMMENT 'Gender match score: 1.0 for exact match, 0.0 otherwise.',
  phone_match_score       FLOAT               COMMENT 'Phone number match score: 1.0 for exact, partial credit for partial digit overlap.',
  city_match_score        FLOAT               COMMENT 'City match score using Jaro-Winkler or exact match (0.0 to 1.0).',
  state_match_score       FLOAT               COMMENT 'State match score: 1.0 for exact match, 0.0 otherwise.',
  zip_match_score         FLOAT               COMMENT 'ZIP code match score: 1.0 for exact 5-digit match, partial for 3-digit prefix match.',
  total_weight            FLOAT               COMMENT 'Fellegi-Sunter composite match weight (sum of individual field log-likelihood ratios).',
  match_classification    STRING              COMMENT 'Classification based on total_weight thresholds: match, possible_match, or non_match.',
  fellegi_sunter_log_odds FLOAT               COMMENT 'Log-odds ratio from the Fellegi-Sunter model for this candidate pair.',
  created_at              TIMESTAMP           COMMENT 'Timestamp when this candidate match record was generated.'
)
COMMENT 'Document-to-member probabilistic match candidates generated by the blocking and scoring pipeline. Each row is a candidate pair with per-field similarity scores and an overall Fellegi-Sunter composite weight.'
TBLPROPERTIES (
  'contains_pii'  = 'true',
  'contains_phi'  = 'false',
  'domain'        = 'matching',
  'quality.tier'  = 'gold',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- TABLE: analytics.doc_auth_match_candidates
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.analytics.doc_auth_match_candidates (
  match_id              STRING    NOT NULL  COMMENT 'UUID surrogate key uniquely identifying this document-to-authorization candidate match.',
  doc_id                STRING    NOT NULL  COMMENT 'FK to raw.clinical_document. The clinical document in this candidate pair.',
  auth_id               STRING    NOT NULL  COMMENT 'FK to raw.authorization. The candidate authorization in this match pair.',
  member_match_id       STRING              COMMENT 'FK to analytics.doc_member_match_candidates. The member match that linked this doc to the auth member.',
  auth_number_match     BOOLEAN             COMMENT 'TRUE if the extracted auth number from the document matches the authorization auth_number.',
  service_date_overlap  BOOLEAN             COMMENT 'TRUE if the document dates fall within the authorizations service_from_date to service_to_date window.',
  procedure_code_match  BOOLEAN             COMMENT 'TRUE if the extracted procedure code matches the authorization procedure code.',
  provider_npi_match    BOOLEAN             COMMENT 'TRUE if the extracted provider NPI matches the authorization rendering_provider_npi.',
  total_weight          FLOAT               COMMENT 'Composite match weight combining all authorization-level matching signals.',
  match_classification  STRING              COMMENT 'Classification based on total_weight thresholds: match, possible_match, or non_match.',
  created_at            TIMESTAMP           COMMENT 'Timestamp when this candidate match record was generated.'
)
COMMENT 'Document-to-authorization match candidates. After a document is matched to a member, this table stores candidate authorization matches based on auth number, service dates, procedure codes, and provider NPI.'
TBLPROPERTIES (
  'contains_pii'  = 'false',
  'contains_phi'  = 'false',
  'domain'        = 'matching',
  'quality.tier'  = 'gold',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- TABLE: analytics.doc_member_pairs_features (Feature vector for ML)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.analytics.doc_member_pairs_features (
  pair_id             STRING    NOT NULL  COMMENT 'UUID surrogate key for this feature vector record.',
  doc_id              STRING    NOT NULL  COMMENT 'FK to raw.clinical_document. The document side of the pair.',
  member_id           STRING    NOT NULL  COMMENT 'FK to ref.member. The member side of the pair. -- PHI/PII: Protected',
  blocking_key        STRING              COMMENT 'Blocking key that generated this candidate pair.',
  doc_first_name      STRING              COMMENT 'First name as extracted from the clinical document. -- PHI/PII: Protected',
  doc_last_name       STRING              COMMENT 'Last name as extracted from the clinical document. -- PHI/PII: Protected',
  doc_dob             STRING              COMMENT 'Date of birth as extracted from the clinical document (string form). -- PHI/PII: Protected',
  doc_ssn4            STRING              COMMENT 'SSN last-4 as extracted from the clinical document. -- PHI/PII: Protected',
  member_first_name   STRING              COMMENT 'First name from the golden master member record. -- PHI/PII: Protected',
  member_last_name    STRING              COMMENT 'Last name from the golden master member record. -- PHI/PII: Protected',
  member_dob          STRING              COMMENT 'Date of birth from the golden master member record (string form). -- PHI/PII: Protected',
  member_ssn4         STRING              COMMENT 'SSN last-4 from the golden master member record. -- PHI/PII: Protected',
  jw_first_name       FLOAT               COMMENT 'Jaro-Winkler similarity score for first name (0.0 to 1.0).',
  jw_last_name        FLOAT               COMMENT 'Jaro-Winkler similarity score for last name (0.0 to 1.0).',
  dob_exact_match     INT                 COMMENT 'Binary indicator: 1 if date of birth matches exactly, 0 otherwise.',
  dob_partial_match   INT                 COMMENT 'Binary indicator: 1 if date of birth partially matches (e.g. year+month but not day), 0 otherwise.',
  ssn4_exact_match    INT                 COMMENT 'Binary indicator: 1 if SSN last-4 matches exactly, 0 otherwise.',
  gender_match        INT                 COMMENT 'Binary indicator: 1 if gender matches, 0 otherwise.',
  phone_match         INT                 COMMENT 'Binary indicator: 1 if phone number matches, 0 otherwise.',
  city_match          INT                 COMMENT 'Binary indicator: 1 if city matches (case-insensitive), 0 otherwise.',
  state_match         INT                 COMMENT 'Binary indicator: 1 if state matches, 0 otherwise.',
  zip_match           INT                 COMMENT 'Binary indicator: 1 if ZIP code matches (5-digit), 0 otherwise.',
  created_at          TIMESTAMP           COMMENT 'Timestamp when this feature vector was computed.'
)
COMMENT 'Pre-computed feature vectors for document-to-member candidate pairs. Contains both raw field values and computed similarity features used as input to the Fellegi-Sunter model and supervised ML classifiers.'
TBLPROPERTIES (
  'contains_pii'  = 'true',
  'contains_phi'  = 'true',
  'domain'        = 'matching',
  'quality.tier'  = 'gold',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- TABLE: analytics.match_events (Event log for downstream consumers)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.analytics.match_events (
  event_id              STRING    NOT NULL  COMMENT 'UUID uniquely identifying this match event.',
  event_type            STRING              COMMENT 'Type of match event: doc_member_match, doc_auth_match, manual_override, match_rejected, match_confirmed.',
  doc_id                STRING              COMMENT 'FK to raw.clinical_document. The document involved in this event.',
  member_id             STRING              COMMENT 'FK to ref.member. The member involved in this event. -- PHI/PII: Protected',
  auth_id               STRING              COMMENT 'FK to raw.authorization. The authorization involved in this event (if applicable).',
  match_score           FLOAT               COMMENT 'Composite match score at the time of the event.',
  match_classification  STRING              COMMENT 'Match classification at the time of the event: match, possible_match, or non_match.',
  match_method          STRING              COMMENT 'Method used: CoverSheet, OCR, FaxMeta, Manual, or ML_Model.',
  event_payload         STRING              COMMENT 'JSON payload with additional event details, field-level scores, and metadata.',
  event_timestamp       TIMESTAMP           COMMENT 'Timestamp when the match event occurred.',
  published_to_kafka    BOOLEAN             COMMENT 'TRUE if this event was successfully published to the Kafka match-events topic.',
  kafka_publish_ts      TIMESTAMP           COMMENT 'Timestamp when the event was published to Kafka (NULL if not yet published).'
)
COMMENT 'Immutable event log capturing all match lifecycle events. Used for downstream integration via Kafka, audit trails, and match analytics dashboards.'
TBLPROPERTIES (
  'contains_pii'  = 'true',
  'contains_phi'  = 'false',
  'domain'        = 'matching',
  'quality.tier'  = 'gold',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- TABLE: analytics.fellegi_sunter_parameters (Model parameters)
-- ============================================================================
CREATE TABLE IF NOT EXISTS healthcare_demo.analytics.fellegi_sunter_parameters (
  field_name              STRING    NOT NULL  COMMENT 'Name of the matching field (e.g. first_name, last_name, dob, ssn4, gender, phone, city, state, zip).',
  m_probability           FLOAT               COMMENT 'Estimated probability that this field agrees given a true match (sensitivity). Range 0.0 to 1.0.',
  u_probability           FLOAT               COMMENT 'Estimated probability that this field agrees given a true non-match (false positive rate). Range 0.0 to 1.0.',
  agreement_weight        FLOAT               COMMENT 'Log2(m_probability / u_probability). Positive weight added when field values agree.',
  disagreement_weight     FLOAT               COMMENT 'Log2((1 - m_probability) / (1 - u_probability)). Negative weight added when field values disagree.',
  field_weight_multiplier FLOAT               COMMENT 'Domain-expert multiplier applied to this fields weight to adjust relative importance (default 1.0).',
  last_estimated_at       TIMESTAMP           COMMENT 'Timestamp when these parameters were last estimated via EM algorithm or manual calibration.'
)
COMMENT 'Fellegi-Sunter probabilistic record linkage model parameters. Stores m-probabilities, u-probabilities, and derived weights for each matching field. Updated periodically by the parameter estimation pipeline.'
TBLPROPERTIES (
  'contains_pii'  = 'false',
  'contains_phi'  = 'false',
  'domain'        = 'matching',
  'quality.tier'  = 'gold'
);

-- ============================================================================
-- END OF DDL
-- ============================================================================
