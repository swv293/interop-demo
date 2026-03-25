# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingest Documents & Parse with AI
# MAGIC
# MAGIC This notebook reads binary PDF/TIFF files from a Unity Catalog volume,
# MAGIC uses `ai_parse_document()` to extract text and structure, and writes
# MAGIC parsed results to `serverless_stable_swv01_catalog.curated.clinical_doc_parsed`.
# MAGIC
# MAGIC **Pipeline Position:** Step 2 of 7 (after seed data ingestion)
# MAGIC
# MAGIC **Key Databricks Features Used:**
# MAGIC - `READ_FILES` / `spark.read.format("binaryFile")` for binary ingestion
# MAGIC - `ai_parse_document()` for AI-powered OCR and document parsing
# MAGIC - Unity Catalog managed Delta tables
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `/Volumes/serverless_stable_swv01_catalog/raw/raw_docs/*.{pdf,tiff,tif}` — raw clinical documents
# MAGIC - `serverless_stable_swv01_catalog.raw.clinical_document` — document metadata from intake log
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `serverless_stable_swv01_catalog.curated.clinical_doc_parsed` — parsed document text and metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "serverless_stable_swv01_catalog"
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/raw_docs"
OUTPUT_TABLE = f"{CATALOG}.curated.clinical_doc_parsed"

# Minimum text length to consider a document "readable"
MIN_TEXT_LENGTH_THRESHOLD = 50

# Batch size for ai_parse_document calls (to manage API rate limits)
PARSE_BATCH_SIZE = 100

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Binary Files from UC Volume
# MAGIC
# MAGIC We use `binaryFile` format to read all PDF and TIFF files from the raw docs volume.
# MAGIC Each row contains the file path, modification time, length, and binary content.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, BooleanType, TimestampType

# Read all binary files from the volume
raw_files_df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.{pdf,tiff,tif,PDF,TIFF,TIF}")
    .option("recursiveFileLookup", "true")
    .load(VOLUME_PATH)
)

print(f"Found {raw_files_df.count()} documents in {VOLUME_PATH}")
display(raw_files_df.select("path", "modificationTime", "length").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse Documents with ai_parse_document
# MAGIC
# MAGIC `ai_parse_document` is a Databricks AI Function that performs:
# MAGIC - OCR on scanned images (TIFF, PDF)
# MAGIC - Layout analysis to identify text regions, tables, headers
# MAGIC - Structured element extraction with confidence scores
# MAGIC
# MAGIC Returns a struct with: `document.elements[]`, `error_status`, `metadata`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: SQL-based parsing using ai_parse_document (preferred)
# MAGIC
# MAGIC This approach uses Spark SQL with `READ_FILES` and `ai_parse_document` directly.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view of parsed documents using ai_parse_document
# MAGIC CREATE OR REPLACE TEMPORARY VIEW parsed_docs_raw AS
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_parse_document(content, named_struct(
# MAGIC     'mode', 'OCR',
# MAGIC     'outputFormat', 'MARKDOWN'
# MAGIC   )) AS parsed
# MAGIC FROM read_files(
# MAGIC   '/Volumes/serverless_stable_swv01_catalog/raw/raw_docs/',
# MAGIC   format => 'binaryFile',
# MAGIC   pathGlobFilter => '*.{pdf,tiff,tif}'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Python-based parsing (fallback / more control)

# COMMAND ----------

# Python approach with explicit control over batching and error handling
from pyspark.sql import functions as F

# Extract doc_id from file path (filename without extension)
files_with_id = raw_files_df.withColumn(
    "doc_id",
    F.regexp_extract(F.col("path"), r"([^/]+)\.[^.]+$", 1)
)

# Parse using SQL expression on each row
# ai_parse_document accepts binary content and returns a parsed struct
parsed_df = files_with_id.selectExpr(
    "doc_id",
    "path AS file_path",
    "modificationTime AS file_mod_time",
    "length AS file_size_bytes",
    """ai_parse_document(content, named_struct(
        'mode', 'OCR',
        'outputFormat', 'MARKDOWN'
    )) AS parsed_result"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract Text and Metadata from Parsed Results
# MAGIC
# MAGIC The `ai_parse_document` output contains:
# MAGIC - `parsed_result.document.elements[]` — array of extracted text elements
# MAGIC - `parsed_result.error_status` — null if successful, error details otherwise
# MAGIC - `parsed_result.metadata` — page count, language, confidence scores

# COMMAND ----------

# Flatten the parsed result into usable columns
extracted_df = parsed_df.select(
    "doc_id",
    "file_path",
    "file_size_bytes",

    # Concatenate all text elements into a single raw_text field
    F.concat_ws(
        "\n",
        F.transform(
            F.col("parsed_result.document.elements"),
            lambda elem: elem.getField("content")
        )
    ).alias("raw_text"),

    # Error status — null means successful parse
    F.col("parsed_result.error_status").alias("parse_error_status"),

    # Metadata fields
    F.col("parsed_result.metadata.pageCount").cast("int").alias("page_count"),
    F.col("parsed_result.metadata.confidence").cast("float").alias("parse_confidence"),

    # Timestamps
    F.current_timestamp().alias("ingest_ts"),
)

# Determine readability based on error status and text length
enriched_df = extracted_df.withColumn(
    "unreadable_flag",
    F.when(
        F.col("parse_error_status").isNotNull(), True
    ).when(
        F.length(F.col("raw_text")) < MIN_TEXT_LENGTH_THRESHOLD, True
    ).otherwise(False)
).withColumn(
    "extraction_model", F.lit("ai_parse_document_v1")
).withColumn(
    "extraction_timestamp", F.current_timestamp()
).withColumn(
    "created_at", F.current_timestamp()
)

# Add placeholder columns for structured extraction (populated in notebook 03)
final_df = enriched_df.select(
    "doc_id",
    "file_path",
    "ingest_ts",
    "raw_text",
    F.lit(None).cast("string").alias("extracted_first_name"),
    F.lit(None).cast("string").alias("extracted_last_name"),
    F.lit(None).cast("date").alias("extracted_dob"),
    F.lit(None).cast("string").alias("extracted_ssn4"),
    F.lit(None).cast("string").alias("extracted_auth_id"),
    F.lit(None).cast("string").alias("extracted_member_id_form"),
    F.lit(None).cast("string").alias("extracted_provider_name"),
    F.lit(None).cast("string").alias("extracted_provider_npi"),
    F.lit(None).cast("string").alias("extracted_procedure_code"),
    F.lit(None).cast("string").alias("extracted_diagnosis_code"),
    "parse_error_status",
    "parse_confidence",
    "unreadable_flag",
    "extraction_model",
    "extraction_timestamp",
    "created_at",
)

print(f"Parsed documents: {final_df.count()} rows")
print(f"Unreadable: {final_df.filter(F.col('unreadable_flag') == True).count()}")
display(final_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to clinical_doc_parsed Delta Table

# COMMAND ----------

(
    final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"✓ Written to {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quality Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_docs,
# MAGIC   SUM(CASE WHEN unreadable_flag = true THEN 1 ELSE 0 END) AS unreadable_docs,
# MAGIC   ROUND(AVG(parse_confidence), 3) AS avg_confidence,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN unreadable_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
# MAGIC   ) AS pct_unreadable,
# MAGIC   SUM(CASE WHEN parse_error_status IS NOT NULL THEN 1 ELSE 0 END) AS parse_errors
# MAGIC FROM serverless_stable_swv01_catalog.curated.clinical_doc_parsed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Notebook 03** to extract structured fields (name, DOB, SSN4, etc.)
# MAGIC from `raw_text` using `ai_query` with an LLM.
