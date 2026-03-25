# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Seed Data into Unity Catalog
# MAGIC
# MAGIC This notebook loads the synthetic CSV seed files (members, authorizations, intake forms)
# MAGIC into Delta tables in the `serverless_stable_swv01_catalog` catalog. It is the first step in the
# MAGIC Prior Authorization & Clinical Document Processing pipeline.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run `infra/unity_catalog.sql` to create catalog, schemas, and table DDLs
# MAGIC - Upload CSV files from `data/synthetic/` to a UC volume or DBFS
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `serverless_stable_swv01_catalog.ref.member` — 1,000 member golden master records
# MAGIC - `serverless_stable_swv01_catalog.raw.authorization` — 3,000 authorization records
# MAGIC - `serverless_stable_swv01_catalog.raw.clinical_document` — 6,000 intake form metadata records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "serverless_stable_swv01_catalog"
SCHEMA_REF = "ref"
SCHEMA_RAW = "raw"

# Path to uploaded CSVs — adjust if you placed them elsewhere
# Option A: UC Volume path (preferred)
CSV_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_RAW}/raw_docs/seed_data"
# Option B: Workspace files path
CSV_WORKSPACE_PATH = "/Workspace/Users/{user}/interop_demo/data/synthetic"

# Toggle which path to use
USE_VOLUME = True
csv_base = CSV_VOLUME_PATH if USE_VOLUME else CSV_WORKSPACE_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Catalog Context

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Members Seed Data

# COMMAND ----------

members_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .load(f"{csv_base}/members_seed.csv")
)

# Cast columns to correct types
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

members_typed = (
    members_df
    .withColumn("dob", F.to_date("dob", "yyyy-MM-dd"))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)

print(f"Members loaded: {members_typed.count()} rows")
members_typed.printSchema()
display(members_typed.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Members to Delta

# COMMAND ----------

(
    members_typed
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_REF}.member")
)

print(f"✓ Written to {CATALOG}.{SCHEMA_REF}.member")
spark.sql(f"SELECT COUNT(*) AS member_count FROM {CATALOG}.{SCHEMA_REF}.member").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Authorizations Seed Data

# COMMAND ----------

auths_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .load(f"{csv_base}/authorizations_seed.csv")
)

auths_typed = (
    auths_df
    .withColumn("service_from_date", F.to_date("service_from_date", "yyyy-MM-dd"))
    .withColumn("service_to_date", F.to_date("service_to_date", "yyyy-MM-dd"))
    .withColumn("auth_requested_date", F.to_date("auth_requested_date", "yyyy-MM-dd"))
    .withColumn("auth_decision_date", F.to_date("auth_decision_date", "yyyy-MM-dd"))
    .withColumn("additional_info_due_date", F.to_date("additional_info_due_date", "yyyy-MM-dd"))
    .withColumn("doc_received_date", F.to_timestamp("doc_received_date"))
    .withColumn("approved_units", F.col("approved_units").cast("decimal(8,2)"))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)

print(f"Authorizations loaded: {auths_typed.count()} rows")
display(auths_typed.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Authorizations to Delta

# COMMAND ----------

(
    auths_typed
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_RAW}.authorization")
)

print(f"✓ Written to {CATALOG}.{SCHEMA_RAW}.authorization")

# Status distribution check
spark.sql(f"""
    SELECT status, COUNT(*) AS cnt,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
    FROM {CATALOG}.{SCHEMA_RAW}.authorization
    GROUP BY status
    ORDER BY cnt DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Intake Forms (Document Metadata)

# COMMAND ----------

intake_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .load(f"{csv_base}/intake_forms_structured.csv")
)

# Map intake form fields to clinical_document schema
docs_typed = (
    intake_df
    .withColumn("doc_id", F.col("doc_id"))
    .withColumn("member_id", F.col("true_member_id"))
    .withColumn("auth_id", F.col("true_auth_id"))
    .withColumn("document_type", F.col("form_type"))
    .withColumn("file_name", F.concat(F.col("doc_id"), F.lit(".pdf")))
    .withColumn("file_path", F.concat(
        F.lit(f"/Volumes/{CATALOG}/{SCHEMA_RAW}/raw_docs/"),
        F.col("doc_id"), F.lit(".pdf")
    ))
    .withColumn("file_format", F.lit("PDF"))
    .withColumn("source_channel", F.col("source_channel"))
    .withColumn("sender_fax_number", F.col("sender_fax_number"))
    .withColumn("is_readable", F.col("is_readable").cast("boolean"))
    .withColumn("quality_score", F.col("quality_score").cast("float"))
    .withColumn("ocr_status", F.lit("pending"))
    .withColumn("match_status", F.lit("pending_review"))
    .withColumn("received_timestamp", F.current_timestamp())
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .select(
        "doc_id", "auth_id", "member_id", "document_type",
        "file_name", "file_path", "file_format",
        "source_channel", "sender_fax_number",
        "is_readable", "quality_score", "ocr_status", "match_status",
        "received_timestamp", "ingestion_timestamp",
        "created_at", "updated_at"
    )
)

print(f"Documents loaded: {docs_typed.count()} rows")
display(docs_typed.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Clinical Documents to Delta

# COMMAND ----------

(
    docs_typed
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_RAW}.clinical_document")
)

print(f"✓ Written to {CATALOG}.{SCHEMA_RAW}.clinical_document")

# Document type distribution
spark.sql(f"""
    SELECT document_type, COUNT(*) AS cnt,
           ROUND(AVG(quality_score), 3) AS avg_quality,
           SUM(CASE WHEN is_readable = false THEN 1 ELSE 0 END) AS unreadable_count
    FROM {CATALOG}.{SCHEMA_RAW}.clinical_document
    GROUP BY document_type
    ORDER BY cnt DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation Summary

# COMMAND ----------

print("=" * 60)
print("SEED DATA INGESTION SUMMARY")
print("=" * 60)

for table, schema in [
    ("member", SCHEMA_REF),
    ("authorization", SCHEMA_RAW),
    ("clinical_document", SCHEMA_RAW),
]:
    count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table}").collect()[0][0]
    print(f"  {CATALOG}.{schema}.{table}: {count:,} rows")

print("=" * 60)
print("Seed data ingestion complete.")
