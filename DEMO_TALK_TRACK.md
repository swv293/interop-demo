# Clinical Document Matching on Databricks — Prior Authorization & Interoperability Demo

## Talk Track & Presenter Guide

**Target Audience:** Healthcare payer CTO / VP Engineering, Data Architects, Clinical Ops Directors
**Demo Duration:** 45-60 minutes
**Workspace:** `fevm-serverless-stable-swv01.cloud.databricks.com`
**Presenter:** Swami Venkatesh

---

## Table of Contents

1. [Part 1: The Business Problem (5 min)](#part-1-the-business-problem-5-min)
2. [Part 2: Architecture Walkthrough (5 min)](#part-2-architecture-walkthrough-5-min)
3. [Part 3: Interactive Notebook Walkthrough (20 min)](#part-3-interactive-notebook-walkthrough-20-min)
4. [Part 4: Production Pipeline Deep Dive (15 min)](#part-4-production-pipeline-deep-dive-15-min)
5. [Part 5: Dashboard & Genie Space (5 min)](#part-5-dashboard--genie-space-5-min)
6. [Part 6: Key Differentiators & Wrap-up (5 min)](#part-6-key-differentiators--wrap-up-5-min)
7. [Appendix: Demo URLs](#appendix-demo-urls)

---

## Part 1: The Business Problem (5 min)

### Opening Statement

> "Let me set the stage with a problem every health plan deals with daily. Prior authorization requests come in through three channels: electronic via X12 278 or FHIR, telephonic through your CIT system, and — still the most common for many payers — fax. Each fax submission generates a TIFF or PDF bundle that needs to be linked to a member record and an open authorization. That matching step is the bottleneck we are going to solve today."

### Key Points to Cover

**The document matching challenge:**
- Fax-submitted PA requests generate TIFF/PDF bundles — sometimes 10-30 pages of mixed clinical notes, cover sheets, and lab results
- These bundles must be matched to: (1) a member in the enrollment system, and (2) an open authorization record
- Without automation, intake staff manually search by name, DOB, or auth number — slow, error-prone, and inconsistent

**The cascading match strategy (draw on whiteboard or show slide):**
1. **Cover Sheet OCR** — Extract structured identifiers (auth number, member ID, DOB) from the cover sheet and do a direct lookup
2. **Full-Document OCR** — When the cover sheet fails, parse the entire document for demographic tokens and do fuzzy matching (name + DOB + SSN4)
3. **Fax Metadata Correlation** — Sender fax number mapped to provider NPI, then to open authorizations within a 72-hour window
4. **Manual Review** — Unmatched documents route to a human work queue

**Why this matters:**
- **TAT compliance**: 72 hours for urgent PAs, 14 days for standard — every hour of manual matching burns into that SLA
- **Claims adjudication downstream**: A mismatched document means a denied claim or a delayed determination
- **Scale**: A mid-size plan processes 5,000-10,000 fax PA requests per week — manual matching cannot keep up

**Transition:**

> "So the question is: can we build an intelligent, automated document matching pipeline on a modern data platform — one that handles OCR, fuzzy matching, probabilistic scoring, and governance in a single architecture? That is exactly what we built on Databricks. Let me walk you through the architecture."

---

## Part 2: Architecture Walkthrough (5 min)

### Show the Architecture Diagram

Open the README.md or display this diagram on screen:

```
MRM System                    Databricks Lakehouse
+------------+               +-------------------------------------------+
| TIFF/PDF   |---drop------->| UC Volume: /Volumes/.../raw_docs/         |
| Bundles    |               |                                           |
+------------+               |  01: Ingest seed data                     |
                             |  02: ai_parse_document (OCR)              |
                             |  03: ai_query (field extraction)          |
                             |  04: Blocking + similarity                |
                             |  05: Fellegi-Sunter scoring               |
                             |  06: DQ checks + PII masking              |
                             |  07: Publish events (Delta/Kafka)         |
                             |                                           |
                             |  Delta Tables:                            |
                             |   ref.member (golden master)              |
                             |   raw.authorization                       |
                             |   raw.clinical_document                   |
                             |   curated.clinical_doc_parsed             |
                             |   analytics.doc_member_match_candidates   |
                             |   analytics.doc_auth_match_candidates     |
                             |   analytics.match_events                  |
                             +-------------------------------------------+
```

### Talking Points

> "Here is the end-to-end flow. Documents land in a Unity Catalog Volume — think of it as a managed cloud storage location governed by UC permissions. From there, seven processing steps take us from raw binary files to scored match events."

**Lakehouse medallion pattern:**
- **Raw** — Binary files in UC Volume, plus document metadata and authorization records as ingested
- **Curated** — Parsed and LLM-extracted documents with structured fields
- **Analytics** — Candidate pairs, Fellegi-Sunter scores, match classifications, events

**Unity Catalog governance:**
- Single catalog: `serverless_stable_swv01_catalog`
- Four schemas: `ref` (golden master), `raw` (ingested), `curated` (parsed), `analytics` (scored)
- Column masking on SSN4, DOB, phone — dynamic based on user group
- Row-level security via materialized views

**Key Databricks features in play:**
- **Serverless compute** — no clusters to manage
- **AI Functions** — `ai_parse_document` for OCR, `ai_query` for LLM extraction
- **Delta Lake** — ACID transactions, MERGE, time travel
- **Materialized Views** — pre-computed masked data for analyst access

**Transition:**

> "Now let me show you this running live. We will walk through each notebook interactively so you can see the data at every step."

---

## Part 3: Interactive Notebook Walkthrough (20 min)

### Navigation

Open the notebook folder in the workspace:
`/Workspace/Users/swami.venkatesh@databricks.com/interop_demo/notebooks/`

---

### Notebook 1: `01_ingest_seed_data.py` — Bulk CSV Loading into Delta Tables

**What it does:** Loads 1,000 members, 3,000 authorizations, and 6,000 clinical document metadata records from CSV files in a UC Volume into managed Delta tables.

**Key Databricks feature:** Unity Catalog Volumes for file storage, `spark.read.csv` with schema inference, Delta table writes.

**What to show on screen:**
- **Cell 1-2 (Configuration):** Point out the catalog/schema/volume path configuration. Highlight `USE_VOLUME = True` and the UC Volume path: `/Volumes/serverless_stable_swv01_catalog/raw/raw_docs/seed_data`
- **Cell 3:** `spark.sql(f"USE CATALOG {CATALOG}")` — show the single-line catalog context switch
- **After execution:** Run quick counts to show the loaded data:
  - Member count: `SELECT COUNT(*) FROM ref.member` (expect ~1,000)
  - Auth status distribution: `SELECT status, COUNT(*) FROM raw.authorization GROUP BY status` (expect ~45% Approved, 25% Pending, 15% Denied)
  - Document type breakdown: `SELECT document_type, COUNT(*) FROM raw.clinical_document GROUP BY document_type`

**Talking point:**

> "This simulates the initial data load from your enrollment and authorization systems. In production, this would be an incremental feed from your MRM or claims system. Notice we are using Unity Catalog Volumes — these are governed storage locations where you can drop files and have them tracked by the catalog. No DBFS, no external mounts — everything lives under UC governance."

**Transition:**

> "Now that we have our reference data, let us bring in the actual clinical documents — the TIFF and PDF bundles that need to be matched."

---

### Notebook 2: `02_ingest_docs_and_parse.py` — AI-Powered Document Parsing

**What it does:** Reads binary PDF/TIFF files from a UC Volume, runs `ai_parse_document()` to extract text and layout structure, and writes parsed results to the curated layer.

**Key Databricks feature:** `ai_parse_document()` — a built-in Databricks AI Function that performs OCR and document layout analysis natively, with no external vendor required.

**What to show on screen:**
- **Cell 3 (Configuration):** Point out `VOLUME_PATH`, `MIN_TEXT_LENGTH_THRESHOLD = 50`, and `PARSE_BATCH_SIZE = 100`
- **Cell 5 (Binary file read):** Highlight `spark.read.format("binaryFile")` — show that each row contains path, modification time, length, and raw binary content
- **Cell with `ai_parse_document`:** This is the key cell. Show the function call and explain the VARIANT output structure:
  ```python
  ai_parse_document(content, map('version', '2.0'))
  ```
- **After execution:** Show a sample parsed row — point out `raw_text` (the extracted text), `page_count`, and `unreadable_flag`

**Talking point:**

> "This is where the magic starts. `ai_parse_document` is a native Databricks AI Function — it handles PDF and TIFF natively with full layout analysis. No external OCR vendor, no API keys, no data leaving the platform. The output is a VARIANT column containing the full document structure — pages, elements, text content. We flatten that into a `raw_text` column for the next step. Notice the `unreadable_flag` — about 5-10% of faxed documents are genuinely unreadable, and we flag those immediately rather than sending garbage downstream."

**Transition:**

> "We now have raw text from every readable document. The next step is to extract structured fields — names, dates, identifiers — from that unstructured text."

---

### Notebook 3: `03_extract_structured_fields.sql` — LLM-Based Field Extraction

**What it does:** Uses `ai_query()` with Claude Sonnet to extract structured clinical identifiers (first_name, last_name, DOB, SSN4, auth_id, provider) from the raw text of each parsed document.

**Key Databricks feature:** `ai_query()` with `databricks-claude-sonnet-4` — SQL-callable LLM inference for structured JSON extraction.

**What to show on screen:**
- **Cell 3 (the `ai_query` call):** This is the highlight cell. Show the full SQL:
  ```sql
  ai_query(
    'databricks-claude-sonnet-4',
    CONCAT(
      'Extract the following fields from this clinical document text. ',
      ...
    )
  )
  ```
  Point out: (1) the model name is a string — swap models without code changes, (2) the prompt engineering that requests JSON output, (3) this is pure SQL — no Python, no SDK
- **Cell with the MERGE statement:** Show how extracted fields are merged back into `clinical_doc_parsed` incrementally — only documents that have not been extracted yet are processed
- **After execution:** Query a few rows to show the extracted fields side by side with the raw text:
  ```sql
  SELECT doc_id, extracted_first_name, extracted_last_name, extracted_dob, extracted_ssn4
  FROM curated.clinical_doc_parsed
  WHERE extracted_first_name IS NOT NULL
  LIMIT 5
  ```

**Talking point:**

> "We are using an LLM to extract structured identifiers from unstructured clinical text — name, DOB, SSN4, auth number — with a single SQL function call. No NLP pipeline to build, no entity extraction model to train, no regex library to maintain. The prompt asks for JSON output, and `ai_query` handles the parsing. The MERGE pattern means we only process new documents on each run — this is fully incremental."

**Transition:**

> "Now we have structured fields for each document. The question is: which member does this document belong to? That is a record linkage problem, and we solve it with blocking and probabilistic scoring."

---

### Notebook 4: `04_matching_features_and_weights.py` — Blocking and Similarity Computation

**What it does:** Generates candidate document-member pairs using blocking strategies (SSN4, DOB exact, DOB +/- 1 day), then computes Jaro-Winkler similarity scores on name fields and exact match scores on SSN4 and DOB.

**Key Databricks feature:** PySpark UDFs for Jaro-Winkler string similarity, blocking strategies to reduce comparison space.

**What to show on screen:**
- **Cell 3 (Configuration):** Show the table references — `DOC_TABLE`, `MEMBER_TABLE`, `FEATURES_TABLE`
- **Cell with blocking strategies:** Show the three blocking approaches:
  1. **SSN4 blocking** — exact join on last 4 digits of SSN
  2. **DOB exact blocking** — exact join on date of birth
  3. **DOB-window blocking** — join where DOB is within +/- 1 day (catches transposition errors like 03/15 vs 03/16)
- **Cell with Jaro-Winkler UDF:** Show the UDF registration and how it is applied:
  ```python
  jw_sim("d.first_name", "m.first_name")
  ```
- **After execution:** Show sample candidate pairs with scores:
  ```sql
  SELECT doc_id, member_id, jw_first_name_score, jw_last_name_score, dob_match_score, ssn4_match_score
  FROM analytics.doc_member_pairs_features
  LIMIT 10
  ```

**Talking point:**

> "Blocking is critical for performance. Without it, matching 6,000 documents against 1,000 members means 6 million comparisons. With SSN4 and DOB blocking, we reduce that to a few thousand — the pairs that actually have a chance of matching. We use three strategies: SSN4 exact match, DOB exact match, and DOB plus-or-minus one day. That last one is important — about 10% of our synthetic documents have transposed DOB digits, which simulates real-world data entry errors. Jaro-Winkler handles the fuzzy name matching — it is designed for short strings like person names and gives higher scores to strings that share a common prefix."

**Transition:**

> "We now have similarity scores for every candidate pair. The next step is to combine those scores into a single match decision using a principled probabilistic framework."

---

### Notebook 5: `05_fellegi_sunter_scoring.sql` — Probabilistic Record Linkage

**What it does:** Applies the Fellegi-Sunter probabilistic record linkage model — computes per-field log-likelihood ratio weights from m/u probabilities, sums them into a total match score, and classifies each pair as `match`, `possible_match`, or `non_match`.

**Key Databricks feature:** Fellegi-Sunter model implemented in pure SQL with a configurable parameter table, Delta MERGE for parameter initialization.

**What to show on screen:**
- **Cell 3 (parameter initialization):** Show the MERGE that loads default m/u probabilities:
  ```sql
  MERGE INTO analytics.fellegi_sunter_parameters AS target
  USING (
    SELECT * FROM VALUES
      ('first_name',  0.90, 0.010, 0.0, 0.0, 1.0, CURRENT_TIMESTAMP()),
      ('last_name',   0.92, 0.008, 0.0, 0.0, 1.0, CURRENT_TIMESTAMP()),
      ('dob',         0.95, 0.0001, 0.0, 0.0, 4.0, CURRENT_TIMESTAMP()),
      ('ssn4',        0.98, 0.001, 0.0, 0.0, 5.0, CURRENT_TIMESTAMP()),
      ...
  ```
  Highlight the **multiplier column** — SSN4 has 5.0x, DOB has 4.0x, while city and state are 0.3-0.5x
- **Cell with weight computation:** Show `LOG2(m_prob / u_prob) * multiplier` and explain agreement vs. disagreement weights
- **Cell with classification:** Show the threshold logic:
  ```sql
  CASE
    WHEN total_weight >= 12.0 THEN 'match'
    WHEN total_weight >= 6.0  THEN 'possible_match'
    ELSE 'non_match'
  END
  ```
- **After execution:** Show the classification distribution:
  ```sql
  SELECT match_classification, COUNT(*), ROUND(AVG(total_weight), 2) AS avg_weight
  FROM analytics.doc_member_match_candidates
  GROUP BY match_classification
  ```

**Talking point:**

> "This is where the math does the heavy lifting. Fellegi-Sunter is the gold standard for probabilistic record linkage — it has been used in census matching, epidemiology, and healthcare for decades. The key insight is that each field carries a different amount of evidence. SSN4 and DOB carry 3 to 5 times the weight of name fields because they are the strongest identifiers in clinical data. If SSN4 matches, that is strong evidence of a true match. If first name matches but SSN4 does not, that is much weaker. The log-likelihood ratio gives us a principled way to combine all that evidence into a single score, and the thresholds give us a three-way classification: definite match, possible match for human review, and non-match."

**Transition:**

> "We have our matches. Before we publish them downstream, we need to validate data quality and ensure PHI/PII is properly masked."

---

### Notebook 6: `06_quality_checks_and_pii_masks.sql` — Data Quality and Governance

**What it does:** Runs data quality threshold checks across the pipeline (unreadable rate, missing DOB rate, missing SSN4 rate, match rate) and creates Unity Catalog column masking functions for PHI/PII protection.

**Key Databricks feature:** Unity Catalog column masking functions, materialized views with role-based PII masking.

**What to show on screen:**
- **Cell 3 (DQ Check 1):** Show the unreadable document check:
  ```sql
  SELECT
    'pct_unreadable_docs' AS check_name,
    ...
    CASE
      WHEN pct < 15.0 THEN 'PASS'
      ELSE 'FAIL'
    END AS status,
    '< 15%' AS threshold
  FROM curated.clinical_doc_parsed
  ```
  Point out the PASS/FAIL logic and the 15% threshold
- **Cell with masking functions:** Show `mask_ssn4` and `mask_dob`:
  ```sql
  CREATE OR REPLACE FUNCTION mask_ssn4(val STRING)
    RETURNS STRING
    RETURN CASE
      WHEN IS_MEMBER('healthcare_demo_clinical') THEN val
      ELSE '****'
    END;
  ```
- **Cell with materialized view:** Show the masked member view:
  ```sql
  CREATE OR REPLACE MATERIALIZED VIEW curated.member_masked AS
  SELECT member_id, first_name, last_name,
         mask_dob(dob) AS dob,
         mask_ssn4(ssn4) AS ssn4,
         ...
  ```
- **After execution:** Query the masked view to show the masking in action — SSN4 appears as `****`, DOB appears as `YYYY-XX-XX`

**Talking point:**

> "Two things happening here. First, data quality checks with explicit thresholds — if more than 15% of documents are unreadable, or more than 20% are missing DOB, we flag it. These checks gate the pipeline in production. Second, PHI/PII masking applied at the catalog level. The `mask_ssn4` function checks your group membership at query time — clinical reviewers see the real values, analysts see asterisks. This is materialized, meaning the masked data is physically stored and refreshed automatically. There is no runtime overhead and no way for an analyst to bypass the mask."

**Transition:**

> "The last step is publishing match results as events for downstream consumers."

---

### Notebook 7: `07_publish_results_and_events.py` — Event Publishing

**What it does:** Reads high-confidence matches (above a configurable weight threshold), formats them as structured JSON event payloads, and writes them to a Delta event table and optionally to a Kafka topic.

**Key Databricks feature:** Delta table as an event store with structured JSON payloads, Kafka producer integration for real-time streaming.

**What to show on screen:**
- **Cell 2 (Configuration):** Point out `KAFKA_CONFIG = None` (disabled for demo) and `MIN_PUBLISH_WEIGHT = 6.0` — only `possible_match` and above are published
- **Cell with event construction:** Show the JSON payload structure — doc_id, member_id, auth_id, match_class, total_weight, component weights, timestamp
- **Cell with Kafka config block:** Show the commented-out Kafka configuration — bootstrap servers, topic name, SASL authentication
- **After execution:** Query the event table:
  ```sql
  SELECT * FROM analytics.match_events ORDER BY event_ts DESC LIMIT 5
  ```
  Show the event summary:
  ```sql
  SELECT match_class, COUNT(*) FROM analytics.match_events GROUP BY match_class
  ```

**Talking point:**

> "Match results are published as structured events — either to a Delta table for batch consumers or to Kafka for real-time downstream systems like the CGX clinical workstation. The Delta event table gives you time travel, audit trails, and the ability to replay events. The Kafka path gives you sub-second latency for systems that need it. In production, the CGX workstation subscribes to the Kafka topic and surfaces match results to intake coordinators in real time."

**Transition:**

> "That was the interactive, step-by-step walkthrough. Now let me show you what this looks like as a production pipeline — fully streaming, fully automated, fully governed."

---

## Part 4: Production Pipeline Deep Dive (15 min)

### 4a: SDP Pipeline Architecture

**Open:** `sdp_pipeline.py` in the workspace or show the pipeline DAG in the Databricks UI.
**Pipeline URL:** `/#joblist/pipelines/e6d16ddc-e4a2-41e7-a098-7f99fa3af26d`

**Walk through the pipeline DAG on screen, pointing to each node:**

```
Auto Loader (UC Volume)
      |
clinical_doc_parsed (ai_parse_document)
      |
clinical_doc_structured (ai_query extraction)
    |            |
member_pairs   auth_pairs        <-- PARALLEL branches
    |            |
member_matches auth_matches      <-- PARALLEL branches
    |            |
match_events (fan-in via append_flow)
```

**Key code blocks to highlight:**

1. **Auto Loader with batch sizing (Table 1: `clinical_doc_parsed`):**
   ```python
   spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format", "binaryFile")
     .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)  # "100"
     .option("cloudFiles.maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)  # "10g"
     .load(VOLUME_PATH)
   ```
   > "Auto Loader tracks which files have been processed in its checkpoint. On each pipeline run, only NEW files flow through. The batch sizing knobs — `maxFilesPerTrigger` and `maxBytesPerTrigger` — let you control throughput. Under SLA pressure, crank these up. During off-peak, keep them low to save cost."

2. **Inline DQ expectations (multiple tables):**
   ```python
   @dp.expect("doc_id_present", "doc_id IS NOT NULL")
   @dp.expect("raw_text_or_unreadable", "raw_text IS NOT NULL OR unreadable_flag = true")
   @dp.expect_or_drop("no_all_nulls", "first_name IS NOT NULL OR last_name IS NOT NULL OR dob IS NOT NULL OR ssn4 IS NOT NULL")
   @dp.expect_or_fail("weight_class_consistent", "NOT (match_class = 'match' AND total_weight < 4.0)")
   ```
   > "Three levels of enforcement. `@dp.expect` logs a warning but keeps the row. `@dp.expect_or_drop` silently drops rows that fail — we use this for documents with zero extracted fields. `@dp.expect_or_fail` halts the entire pipeline — we use this for logical consistency violations like a match classification that contradicts the score."

3. **Parallel branches — member and auth matching:**
   ```python
   # Table 3a: doc_member_pairs_features (Parallel Branch 1)
   def doc_member_pairs_features():
       docs = dp.read_stream("clinical_doc_structured")
       members = spark.table(f"{REF_SCHEMA}.member")
       ...

   # Table 3b: doc_auth_pairs_features (Parallel Branch 2)
   def doc_auth_pairs_features():
       docs = dp.read_stream("clinical_doc_structured")
       auths = spark.table(f"{RAW_SCHEMA}.authorization")
       ...
   ```
   > "Both branches read from the same upstream table — `clinical_doc_structured` — but the SDP engine schedules them in parallel. Member matching and auth matching do not depend on each other, so they run simultaneously. This cuts total pipeline latency roughly in half."

4. **Fan-in pattern — `create_streaming_table` + `@dp.append_flow`:**
   ```python
   dp.create_streaming_table(name="match_events", ...)

   @dp.append_flow(target="match_events", name="member_match_events_flow")
   def member_match_events_flow():
       ...

   @dp.append_flow(target="match_events", name="auth_match_events_flow")
   def auth_match_events_flow():
       ...
   ```
   > "This is the fan-in. Two independent streams — member matches and auth matches — merge into a single `match_events` table. The `append_flow` decorator tells the SDP engine to append rows from each stream as they arrive. The result is a unified, append-only audit log with both entity types."

5. **Pure-Python Jaro-Winkler UDF (no external dependencies):**
   ```python
   def _jaro_winkler(a: str, b: str, prefix_weight: float = 0.1) -> float:
       ...
   spark.udf.register("jw_sim", jw_udf, DoubleType())
   ```
   > "We implemented Jaro-Winkler in pure Python — no pip packages required. This is a hard constraint of serverless SDP: you cannot install external libraries. The UDF is registered at module level and available in SQL expressions throughout the pipeline."

**Main talking point:**

> "This is a streaming-first architecture. Auto Loader tracks which files have been processed. On each run, only NEW documents flow through. The member and auth matching branches run in parallel — the SDP engine handles the scheduling automatically. And every table has inline DQ expectations — the pipeline is self-validating."

---

### 4b: Job DAG and Optimization

**Open:** Job `#48775581306437` in the Databricks UI.
**Show the task dependency graph:**

```
sdp_pipeline -----> validate_pipeline -----> refresh_dq_metrics -----> refresh_dashboard
     |
     +------------> publish_kafka (parallel, non-blocking)
```

**Walk through the `prod_bundle.yml` configuration:**

1. **Task 1 — `sdp_pipeline`:**
   ```yaml
   - task_key: sdp_pipeline
     pipeline_task:
       pipeline_id: ${resources.pipelines.clinical_doc_matching_sdp.id}
       full_refresh: false
   ```
   > "The pipeline runs in incremental mode by default. Full refresh is available via the `full_refresh` parameter — you would use that after a schema change or a backfill."

2. **Task 2 — `validate_pipeline` (depends on pipeline):**
   ```yaml
   - task_key: validate_pipeline
     depends_on:
       - task_key: sdp_pipeline
     environment_key: serverless_env
     spark_python_task:
       python_file: ../post_pipeline/validate_pipeline.py
   ```
   > "This is the integrity gate. It runs cross-table assertions — row counts, referential integrity, classification consistency. If any assertion fails, it raises an exception and halts the job. Downstream tasks never see corrupted data."

   Show the key validation logic from `validate_pipeline.py`:
   ```python
   parsed_count = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.clinical_doc_parsed").first().c
   assert_check("clinical_doc_parsed has rows", parsed_count > 0, f"count={parsed_count}")
   structured_count = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.clinical_doc_structured").first().c
   assert_check("structured <= parsed", structured_count <= parsed_count, ...)
   ```

3. **Task 3 — `refresh_dq_metrics` (depends on validation):**
   ```yaml
   - task_key: refresh_dq_metrics
     depends_on:
       - task_key: validate_pipeline
   ```
   > "Once validation passes, we compute DQ metrics — unreadable percentage, missing field rates, match distribution — and write them to `dq_run_results`. This table feeds the dashboard and provides a historical record of pipeline health."

   Show the DQ results table DDL from `refresh_dq_metrics.py`:
   ```python
   CREATE TABLE IF NOT EXISTS {CATALOG}.pipeline_prd.dq_run_results (
       batch_id TIMESTAMP, run_ts TIMESTAMP, total_docs BIGINT,
       pct_unreadable DOUBLE, pct_missing_dob DOUBLE, pct_missing_ssn4 DOUBLE,
       parse_errors BIGINT, pct_match DOUBLE, pct_possible_match DOUBLE, pct_non_match DOUBLE
   )
   ```

4. **Task 4 — `refresh_dashboard` (depends on DQ):**
   > "The dashboard refresh is the final step in the sequential chain. It only runs after DQ metrics are computed, so the dashboard always reflects validated data."

5. **Task 5 — `publish_kafka` (parallel with everything after pipeline):**
   ```yaml
   - task_key: publish_kafka
     depends_on:
       - task_key: sdp_pipeline  # only depends on pipeline, NOT validation
   ```
   > "Notice that Kafka publishing depends only on the pipeline — not on validation or DQ. This is intentional. Downstream real-time consumers like the CGX workstation should not be blocked by internal quality checks. They get match events as soon as the pipeline produces them. If DQ checks later fail, the system can issue correction events."

**Key optimization talking points:**

- **Parallelism:** `publish_kafka` runs in parallel with `validate_pipeline` — zero wait time for real-time consumers
- **Fail-fast:** `validate_pipeline` halts the job before DQ metrics if integrity checks fail — no wasted compute
- **Incremental vs full refresh:** Default is incremental; full refresh is a parameter flip — not a code change
- **Serverless compute:** All post-pipeline tasks use `environment_key: serverless_env` — zero cluster management, auto-scaling, pay-per-query
- **Batch sizing for SLA control:** `maxFilesPerTrigger: 100` and `maxBytesPerTrigger: 10g` — tunable knobs for throughput vs. cost

**Main talking point:**

> "The job is designed for operational reliability. The validation task catches data integrity issues before they corrupt the dashboard. Kafka publishing runs in parallel because downstream consumers should not be blocked by internal DQ checks. And everything runs on serverless — zero infrastructure management."

---

### 4c: Schema Isolation Strategy

**Draw or describe the schema separation:**

| Schema | Purpose | Written By |
|--------|---------|-----------|
| `ref` | Member golden master | Seed data / enrollment feed |
| `raw` | Authorization records, document metadata | Seed data / intake feed |
| `curated` | Parsed documents (interactive) | Interactive notebooks |
| `analytics` | Match candidates, events (interactive) | Interactive notebooks |
| `pipeline_prd` | All pipeline output tables | Lakeflow SDP pipeline |
| `dashboard_prd` | Dashboard views | Dashboard refresh task |

**Talking point:**

> "Interactive notebooks write to `curated` and `analytics`. The production pipeline writes to `pipeline_prd`. The dashboard reads from `pipeline_prd`. There are no ownership conflicts — you can run the interactive demo and the production pipeline simultaneously without any table locking or permission issues. This separation also means you can test changes in the interactive schemas before promoting to production."

---

## Part 5: Dashboard & Genie Space (5 min)

### PRD Lakeview Dashboard

**Open:** PRD Dashboard URL (see Appendix)

**Walk through the four pages:**

**Page 1 — Pipeline Overview:**
- KPI counters: total documents processed, match rate, average processing time
- Document type pie chart: prior_auth_form vs clinical_note
- Channel distribution bar chart: fax, electronic, telephonic
- > "This is the operational pulse of the pipeline. At a glance, you can see how many documents have been processed, what the match rate is, and how the intake channels are trending."

**Page 2 — Data Quality:**
- DQ metrics trend line over time: pct_unreadable, pct_missing_dob, pct_missing_ssn4
- Risk tier breakdown: PASS vs FAIL per check
- Threshold reference lines
- > "DQ metrics are tracked per pipeline run. You can see trends — if the unreadable rate is creeping up, that might indicate a fax machine quality issue at a specific provider. Each metric has a red/green threshold so you can spot problems at a glance."

**Page 3 — Match Results:**
- Fellegi-Sunter summary: count and percentage by classification
- Classification pie chart: match / possible_match / non_match
- Score histogram: distribution of total_weight values
- Auth match detail table: doc_id, auth_number, match_class, total_weight
- > "This page is where clinical ops spends most of their time. The pie chart tells you what fraction of documents matched automatically vs. what needs human review. The histogram shows the score distribution — you want a bimodal distribution with clear separation between matches and non-matches. If the distributions overlap heavily, your blocking or weight parameters need tuning."

**Page 4 — Batch Comparison:**
- Documents per batch over time
- Extraction quality per batch: percent with all fields extracted
- Match distribution per batch: stacked bar chart by classification
- DQ check history: pass/fail per batch
- > "This page is for pipeline engineers. You can compare batch quality over time, spot regressions, and correlate DQ failures with upstream data changes."

### Genie Space

**Open:** Genie Space URL (see Appendix)

**Demo a natural language question:**

Type: **"What is the match rate for prior auth forms received by fax?"**

> "Genie translates natural language questions into SQL queries against the governed tables. The analyst never writes SQL, never sees raw PII — Genie queries the masked views by default. Ask it anything: match rates by channel, unreadable trends by document type, top providers by volume."

**Try a second question:** **"Show me the top 5 documents with the highest match scores this week."**

**Talking point:**

> "Business users get self-service analytics through the dashboard and natural language Q&A through Genie — no SQL required, no PII exposed. The Genie space is pointed at the same governed tables and materialized views, so the masking and access controls apply automatically."

---

## Part 6: Key Differentiators & Wrap-up (5 min)

### Summary Slide / Verbal Summary

> "Let me summarize the seven key Databricks differentiators we demonstrated today."

| # | Differentiator | What We Showed |
|---|---------------|----------------|
| 1 | **AI Functions** (`ai_parse_document`, `ai_query`) | Native OCR and LLM extraction — no external OCR vendor, no NLP pipeline, no model training. Pure SQL function calls. |
| 2 | **Unity Catalog** | End-to-end governance: catalog, schemas, volumes, column masking with `IS_MEMBER()`, PHI/PII tags, grants. One governance model from raw files to dashboard. |
| 3 | **Lakeflow SDP** | Streaming-first pipeline with declarative tables, inline DQ expectations (`@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_or_fail`), parallel branches, and fan-in pattern. |
| 4 | **Serverless** | Zero infrastructure management — serverless pipeline compute, serverless SQL warehouse, serverless task compute. No clusters to configure or monitor. |
| 5 | **Materialized Views** | Pre-computed masked data for secure analyst access. Physically stored, auto-refreshed, zero runtime overhead. |
| 6 | **Delta Lake** | ACID transactions, MERGE for incremental updates, time travel for audit, change data feed (CDF) for incremental processing. Every table is a Delta table. |
| 7 | **Genie** | Natural language analytics for non-technical users. Business users ask questions in plain English, get answers from governed tables — no SQL, no PII exposure. |

### Closing Statement

> "What we have built here is a complete clinical document matching platform — from raw fax images to scored match events — running entirely on Databricks. There is no external OCR vendor. There is no separate NLP pipeline. There is no unprotected PII. The pipeline is streaming, the governance is baked in, and the analytics are self-service. Every component — AI parsing, LLM extraction, probabilistic matching, DQ validation, PII masking, event publishing — runs on a single platform with a single governance model. That is the value proposition."

### Q&A Prompts

If the audience is quiet, prompt with:
- "What does your current fax intake workflow look like? How much of the matching is manual today?"
- "What is your current turnaround time for PA matching, and where is the bottleneck?"
- "Are you using any probabilistic matching today, or is it all rules-based?"
- "How are you handling PHI/PII masking for your analytics team currently?"

---

## Appendix: Demo URLs

| Resource | URL |
|----------|-----|
| **Workspace** | https://fevm-serverless-stable-swv01.cloud.databricks.com |
| **Interactive Notebooks** | /Workspace/Users/swami.venkatesh@databricks.com/interop_demo/notebooks/ |
| **Lakeflow SDP Pipeline** | /#joblist/pipelines/e6d16ddc-e4a2-41e7-a098-7f99fa3af26d |
| **Production Job** | /#job/48775581306437 |
| **PRD Dashboard** | /sql/dashboards/01f12a34de2e1ccd9a0de8994d06e4cb |
| **Interactive Dashboard** | /sql/dashboards/01f1285cd4c11f658d1cd900a6cb6df5 |
| **Genie Space** | /explore/genie/01f1285d1e0b1b08b288b1ad4747d76f |
| **GitHub (demo)** | https://github.com/swv293/interop-demo |
| **GitHub (pipeline)** | https://github.com/swv293/databricks-clinical-matching-prod |

---

## Pre-Demo Checklist

Before starting the demo, verify:

- [ ] Workspace is accessible and logged in
- [ ] Notebooks are attached to a cluster with DBR 14.0+ (or serverless)
- [ ] UC Volume `/Volumes/serverless_stable_swv01_catalog/raw/raw_docs/` contains seed CSVs and sample PDFs
- [ ] Seed data tables are populated (`ref.member` = ~1,000 rows, `raw.authorization` = ~3,000 rows)
- [ ] At least one pipeline run has completed (check `pipeline_prd.match_events` for data)
- [ ] Dashboard loads without errors
- [ ] Genie Space is accessible and responds to queries
- [ ] Kafka configuration is commented out (unless you want to demo live streaming)

## Timing Guide

| Section | Duration | Cumulative |
|---------|----------|-----------|
| Part 1: Business Problem | 5 min | 5 min |
| Part 2: Architecture | 5 min | 10 min |
| Part 3: Notebooks (7 notebooks) | 20 min | 30 min |
| Part 4: Production Pipeline | 15 min | 45 min |
| Part 5: Dashboard & Genie | 5 min | 50 min |
| Part 6: Wrap-up & Differentiators | 5 min | 55 min |
| Q&A Buffer | 5 min | 60 min |

**Pacing tip:** If running long, compress Notebooks 1 and 7 to 1 minute each (show output only, skip code walkthrough). The highest-impact sections are Notebooks 2-3 (AI Functions) and Part 4 (production pipeline).
