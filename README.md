# Clinical Document Matching Demo — Prior Authorization & Interoperability

End-to-end Databricks lakehouse demo for healthcare clinical document processing: AI-powered document parsing, probabilistic record linkage (Fellegi-Sunter), and prior authorization matching.

## System Overview

The Prior Authorization (PA) module manages the lifecycle of service authorization requests from submission through clinical determination. Authorization requests arrive via three channels — electronic (X12 278 / FHIR), telephonic (CIT), and fax — and are persisted as records linked to the Member golden master.

Fax-submitted requests generate TIFF/PDF bundles that must be matched to pending authorization records through a cascading hierarchy:

1. **Cover Sheet OCR** — Structured identifiers (auth number, member ID, DOB) extracted and mapped directly
2. **Full-Document OCR** — Member demographic tokens with fuzzy-match resolution (name + DOB + SSN4)
3. **Fax Metadata Correlation** — Sender fax number → provider NPI → open authorizations within ±72 hours

Unmatched documents are routed to a manual review work queue.

## Architecture

```
MRM System                    Databricks Lakehouse
┌──────────┐                 ┌─────────────────────────────────────────┐
│ TIFF/PDF │───drop──────────│ UC Volume: /Volumes/.../raw_docs/       │
│ Bundles  │                 │                                         │
└──────────┘                 │  ┌─────────────────────────────────┐    │
                             │  │ 01: Ingest seed data            │    │
                             │  │ 02: ai_parse_document (OCR)     │    │
                             │  │ 03: ai_query (field extraction) │    │
                             │  │ 04: Blocking + similarity       │    │
                             │  │ 05: Fellegi-Sunter scoring      │    │
                             │  │ 06: DQ checks + PII masking     │    │
                             │  │ 07: Publish events (Delta/Kafka)│    │
                             │  └─────────────────────────────────┘    │
                             │                                         │
                             │  Delta Tables:                          │
                             │   ref.member (golden master)            │
                             │   raw.authorization                     │
                             │   raw.clinical_document                 │
                             │   curated.clinical_doc_parsed           │
                             │   analytics.doc_member_match_candidates │
                             │   analytics.doc_auth_match_candidates   │
                             │   analytics.match_events                │
                             └─────────────────────────────────────────┘
```

## Databricks Features Used

| Feature | Usage |
|---------|-------|
| **Unity Catalog** | Catalog, schemas, managed Delta tables, volumes, grants |
| **ai_parse_document** | OCR and layout analysis on PDF/TIFF clinical documents |
| **ai_query** | LLM-based structured field extraction (claude-sonnet-4) |
| **Column Masking** | Dynamic PII/PHI masking based on user group membership |
| **Delta Lake** | All tables, MERGE for incremental updates, time travel |
| **Workflows** | Orchestration of the 7-step pipeline |

## Repository Structure

```
interop_demo/
├── README.md
├── infra/
│   ├── unity_catalog.sql        # Catalog, schemas, all table DDLs with column comments
│   ├── volumes.sql              # UC volume for raw documents
│   └── grants.sql               # Roles, grants, column masking functions
├── data/
│   ├── synthetic/
│   │   ├── members_seed.csv           # 1,000 members (golden master)
│   │   ├── authorizations_seed.csv    # 3,000 authorizations
│   │   └── intake_forms_structured.csv # 6,000 documents with error injection
│   ├── docs_templates/
│   │   ├── prior_auth_form_*.txt      # PA form templates (3 variants)
│   │   └── clinical_note_*.txt        # Clinical note templates (3 variants)
│   └── generation/
│       ├── generate_synthetic_data.py # Generates seed CSVs
│       └── generate_pdfs.py           # Renders PDFs from templates + CSV data
├── notebooks/
│   ├── 01_ingest_seed_data.py               # Load CSVs → Delta tables
│   ├── 02_ingest_docs_and_parse.py          # Binary file read + ai_parse_document
│   ├── 03_extract_structured_fields.sql     # ai_query for JSON extraction
│   ├── 04_matching_features_and_weights.py  # Blocking + similarity computation
│   ├── 05_fellegi_sunter_scoring.sql        # Probabilistic weights + classification
│   ├── 06_quality_checks_and_pii_masks.sql  # DQ checks + masking views
│   └── 07_publish_results_and_events.py     # Delta + Kafka event publishing
├── src/
│   ├── matching/
│   │   ├── similarity.py        # Jaro-Winkler, DOB, SSN4 similarity UDFs
│   │   ├── fellegi_sunter.py    # Fellegi-Sunter model (m/u probs, weights, classification)
│   │   └── blocking.py          # Blocking strategies (SSN4, DOB, Soundex, DOB-window)
│   └── dq/
│       ├── checks.py            # Data quality check suite
│       └── pii_masking.py       # Column masking functions and views
├── tests/
│   ├── test_similarity.py       # 25+ tests for similarity functions
│   └── test_fellegi_sunter.py   # 20+ tests for F-S model
└── api/
    ├── match_results_openapi.yaml    # OpenAPI 3.0 spec
    └── example_responses/
        └── match_event.json          # Sample event payloads
```

## Data Model

### Entity Relationships

```
Member (golden master)          Authorization                Clinical Document
┌──────────────────┐           ┌─────────────────┐          ┌───────────────────┐
│ member_id (PK)   │──1:N──────│ member_id (FK)   │          │ doc_id (PK)       │
│ first_name       │           │ auth_id (PK)     │──1:N─────│ auth_id (FK)      │
│ last_name        │           │ auth_number      │          │ member_id (FK)    │
│ dob              │           │ service_from/to  │          │ document_type     │
│ ssn4 (PHI)       │           │ procedure_code   │          │ file_path         │
│ gender           │           │ diagnosis_code   │          │ source_channel    │
│ address/phone    │           │ status           │          │ ocr_status        │
└──────────────────┘           │ doc_match_method │          │ match_status      │
                               └─────────────────┘          │ doc_match_method  │
                                                            └───────────────────┘
                                                                     │
                                                                     │ parsed by ai_parse_document
                                                                     ▼
                                                            ┌───────────────────┐
                                                            │ clinical_doc_     │
                                                            │ parsed            │
                                                            │ extracted_*       │
                                                            │ parse_confidence  │
                                                            │ unreadable_flag   │
                                                            └───────────────────┘
                                                                     │
                                                                     │ Fellegi-Sunter matching
                                                                     ▼
                                            ┌──────────────────────────────────────────┐
                                            │ doc_member_match_candidates               │
                                            │ jw_first_name_score, jw_last_name_score  │
                                            │ dob_match_score, ssn4_match_score        │
                                            │ total_weight, match_classification       │
                                            └──────────────────────────────────────────┘
```

### Synthetic Data Distributions

| Dataset | Rows | Key Characteristics |
|---------|------|---------------------|
| Members | 1,000 | Realistic US demographics, ~30% null middle_initial, ~5% null SSN4 |
| Authorizations | 3,000 | 45% Approved, 25% Pending, 15% Denied, mixed CPT/ICD-10 codes |
| Documents | 6,000 | **Error injection**: 10% transposed DOB, 15% name variants, 5% swapped names, 8% missing SSN4, 5-10% unreadable |

### Fellegi-Sunter Weight Configuration

| Field | m-probability | u-probability | Multiplier | Agreement Weight |
|-------|--------------|---------------|------------|-----------------|
| SSN4 | 0.98 | 0.001 | **5.0×** | ~49.5 |
| DOB | 0.95 | 0.0001 | **4.0×** | ~53.1 |
| Last Name | 0.92 | 0.008 | 1.0× | ~6.8 |
| First Name | 0.90 | 0.010 | 1.0× | ~6.5 |
| Phone | 0.85 | 0.0001 | 1.5× | ~19.4 |
| City | 0.88 | 0.020 | 0.5× | ~2.7 |
| Zip | 0.90 | 0.010 | 0.8× | ~5.2 |
| State | 0.95 | 0.050 | 0.3× | ~1.3 |
| Gender | 0.98 | 0.500 | 0.3× | ~0.3 |

**Thresholds:** match ≥ 12.0 | possible_match ≥ 6.0 | non_match < 6.0

## Running the Demo

### Prerequisites

- Databricks workspace: `fevm-serverless-stable-swv01.cloud.databricks.com`
- Cluster with Databricks Runtime 14.0+ (for `ai_parse_document` and `ai_query`)
- Unity Catalog enabled

### Step 1: Create Infrastructure

Run the SQL files in order on a SQL Warehouse or notebook:

```sql
-- 1. Create catalog, schemas, and all tables
%run infra/unity_catalog.sql

-- 2. Create UC volume for raw documents
%run infra/volumes.sql

-- 3. Create roles, grants, and masking functions
%run infra/grants.sql
```

### Step 2: Generate and Upload Synthetic Data

```bash
# Generate CSVs (already provided in data/synthetic/)
python data/generation/generate_synthetic_data.py

# Generate PDFs from templates (requires fpdf2)
pip install fpdf2
python data/generation/generate_pdfs.py --output data/generated_docs/ --limit 100

# Upload CSVs to UC volume
databricks fs cp data/synthetic/ /Volumes/healthcare_demo/raw/raw_docs/seed_data/ --recursive
```

### Step 3: Run the Pipeline

Execute notebooks in order as a Databricks Workflow:

| Task | Notebook | Description |
|------|----------|-------------|
| A | `01_ingest_seed_data.py` | Load CSVs into Delta tables |
| B | `02_ingest_docs_and_parse.py` | Parse documents with `ai_parse_document` |
| C | `03_extract_structured_fields.sql` | Extract fields with `ai_query` (LLM) |
| D | `04_matching_features_and_weights.py` | Blocking + similarity features |
| E | `05_fellegi_sunter_scoring.sql` | Probabilistic scoring + classification |
| F | `06_quality_checks_and_pii_masks.sql` | DQ validation + PII masking |
| G | `07_publish_results_and_events.py` | Publish match events |

### Step 4: Trigger Options

- **Scheduled**: Every N minutes via Databricks Workflow trigger
- **Event-based**: Cloud storage event on file arrival in UC volume
- **Manual**: REST API call from MRM system
- **Kafka**: Configure `KAFKA_CONFIG` in notebook 07 for real-time event streaming

## PHI/PII Protection

All tables containing PHI/PII are tagged with `contains_pii = true` / `contains_phi = true` in table properties. Column-level masking functions dynamically mask SSN4, DOB, and phone based on user group membership:

| Role | SSN4 | DOB | Phone | Raw Text |
|------|------|-----|-------|----------|
| `healthcare_demo_clinical` | Full | Full | Full | Full |
| `healthcare_demo_admin` | Full | Full | Full | Full |
| `healthcare_demo_analyst` | `****` | `YYYY-XX-XX` | `******1234` | Hidden |
| `healthcare_demo_intake` | `****` | `YYYY-XX-XX` | `******1234` | Hidden |

## Local Testing

```bash
# Run similarity and Fellegi-Sunter tests
cd interop_demo
pip install pytest
pytest tests/ -v
```
