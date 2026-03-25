-- ============================================================================
-- Healthcare Clinical Document Matching System
-- Unity Catalog Volumes
-- Target: fevm-serverless-stable-swv01.cloud.databricks.com
-- Catalog: healthcare_demo
-- ============================================================================

USE CATALOG healthcare_demo;

-- ============================================================================
-- VOLUME: raw.raw_docs
-- Stores raw clinical document files (TIFF, PDF, PNG) received from
-- fax servers, electronic submissions, manual uploads, and mail scanning.
--
-- Directory structure convention:
--   /Volumes/healthcare_demo/raw/raw_docs/
--     ├── incoming/           -- newly received, not yet processed
--     ├── processing/         -- currently being OCR'd / parsed
--     ├── completed/          -- successfully processed and matched
--     ├── failed/             -- failed OCR or quality checks
--     └── archive/            -- aged-off documents per retention policy
--
-- Access pattern:
--   - Intake pipeline writes to incoming/
--   - OCR pipeline moves from incoming/ -> processing/ -> completed/|failed/
--   - Retention pipeline moves from completed/ -> archive/
-- ============================================================================
CREATE VOLUME IF NOT EXISTS healthcare_demo.raw.raw_docs
COMMENT 'Unity Catalog managed volume for raw clinical document storage. Contains TIFF, PDF, and PNG files received via fax, electronic submission, upload, and mail scanning. Referenced by raw.clinical_document.file_path.';

-- ============================================================================
-- END OF VOLUMES DDL
-- ============================================================================
