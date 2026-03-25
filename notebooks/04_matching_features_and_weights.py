# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Matching Features & Candidate Pair Generation
# MAGIC
# MAGIC This notebook builds candidate pairs between parsed clinical documents and
# MAGIC the member golden master using blocking strategies, then computes similarity
# MAGIC features (Jaro-Winkler on names, exact/partial on DOB and SSN4).
# MAGIC
# MAGIC **Pipeline Position:** Step 4 of 7 (after structured field extraction)
# MAGIC
# MAGIC **Key Techniques:**
# MAGIC - Blocking to reduce O(n²) comparison space
# MAGIC - Jaro-Winkler string similarity for fuzzy name matching
# MAGIC - Exact and partial match scoring for DOB, SSN4
# MAGIC - PySpark UDFs for similarity computation
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `healthcare_demo.curated.clinical_doc_parsed` — documents with extracted fields
# MAGIC - `healthcare_demo.ref.member` — member golden master
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `healthcare_demo.analytics.doc_member_pairs_features` — feature vectors for all candidate pairs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "healthcare_demo"

DOC_TABLE = f"{CATALOG}.curated.clinical_doc_parsed"
MEMBER_TABLE = f"{CATALOG}.ref.member"
FEATURES_TABLE = f"{CATALOG}.analytics.doc_member_pairs_features"

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Source Tables

# COMMAND ----------

from pyspark.sql import functions as F

# Load parsed documents (only readable ones with at least some extracted data)
docs_df = (
    spark.table(DOC_TABLE)
    .filter(F.col("unreadable_flag") == False)
    .filter(
        F.col("extracted_first_name").isNotNull() |
        F.col("extracted_last_name").isNotNull() |
        F.col("extracted_dob").isNotNull() |
        F.col("extracted_ssn4").isNotNull()
    )
    .select(
        F.col("doc_id"),
        F.col("extracted_first_name").alias("doc_first_name"),
        F.col("extracted_last_name").alias("doc_last_name"),
        F.col("extracted_dob").cast("string").alias("doc_dob"),
        F.col("extracted_ssn4").alias("doc_ssn4"),
        F.col("extracted_provider_npi").alias("doc_provider_npi"),
    )
)

# Load member golden master
members_df = (
    spark.table(MEMBER_TABLE)
    .select(
        F.col("member_id"),
        F.col("first_name").alias("member_first_name"),
        F.col("last_name").alias("member_last_name"),
        F.col("dob").cast("string").alias("member_dob"),
        F.col("ssn4").alias("member_ssn4"),
        F.col("gender").alias("member_gender"),
        F.col("phone").alias("member_phone"),
        F.col("city").alias("member_city"),
        F.col("state").alias("member_state"),
        F.col("zip").alias("member_zip"),
    )
)

print(f"Documents with extracted data: {docs_df.count()}")
print(f"Members in golden master: {members_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Blocking Keys
# MAGIC
# MAGIC Blocking reduces the candidate pair space from O(docs × members) to a manageable
# MAGIC set by only comparing records that share a blocking key.
# MAGIC
# MAGIC **Strategies:**
# MAGIC - **SSN4 block**: Exact match on SSN4 (when available) — highest precision
# MAGIC - **DOB block**: Exact match on DOB string — catches most true matches
# MAGIC - **Name-DOB-year block**: Soundex of last name + DOB year — catches name variants
# MAGIC - **DOB-window block**: DOB ±1 day — catches transposed month/day errors

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/{user}/interop_demo/src")

from matching.blocking import generate_blocking_keys, create_candidate_pairs
from matching.similarity import (
    jaro_winkler_udf, dob_match_udf, ssn4_match_udf,
    gender_match_udf, phone_match_udf
)

# COMMAND ----------

# Strategy 1: SSN4 blocking (exact match on SSN4 where available)
ssn4_pairs = (
    docs_df.filter(F.col("doc_ssn4").isNotNull())
    .join(
        members_df.filter(F.col("member_ssn4").isNotNull()),
        docs_df["doc_ssn4"] == members_df["member_ssn4"],
        "inner"
    )
    .withColumn("blocking_key", F.concat(F.lit("ssn4:"), F.col("doc_ssn4")))
)

print(f"SSN4 blocking pairs: {ssn4_pairs.count()}")

# COMMAND ----------

# Strategy 2: DOB blocking (exact match on DOB)
dob_pairs = (
    docs_df.filter(F.col("doc_dob").isNotNull())
    .join(
        members_df.filter(F.col("member_dob").isNotNull()),
        docs_df["doc_dob"] == members_df["member_dob"],
        "inner"
    )
    .withColumn("blocking_key", F.concat(F.lit("dob:"), F.col("doc_dob")))
)

print(f"DOB blocking pairs: {dob_pairs.count()}")

# COMMAND ----------

# Strategy 3: DOB-window blocking (±1 day for transposition errors)
# Convert DOB strings to dates and do a range join
dob_window_pairs = (
    docs_df.filter(F.col("doc_dob").isNotNull())
    .withColumn("doc_dob_date", F.to_date("doc_dob"))
    .join(
        members_df.filter(F.col("member_dob").isNotNull())
        .withColumn("member_dob_date", F.to_date("member_dob")),
        (
            F.abs(F.datediff(
                F.col("doc_dob_date"),
                F.col("member_dob_date")
            )) <= 1
        ) & (
            # Exclude exact DOB matches (already covered above)
            F.col("doc_dob") != F.col("member_dob")
        ),
        "inner"
    )
    .withColumn("blocking_key", F.lit("dob_window"))
    .drop("doc_dob_date", "member_dob_date")
)

print(f"DOB-window blocking pairs: {dob_window_pairs.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Union All Candidate Pairs and Deduplicate

# COMMAND ----------

# Select consistent columns from each blocking strategy
pair_columns = [
    "doc_id", "member_id", "blocking_key",
    "doc_first_name", "doc_last_name", "doc_dob", "doc_ssn4",
    "member_first_name", "member_last_name", "member_dob", "member_ssn4",
    "member_gender", "member_phone", "member_city", "member_state", "member_zip"
]

all_pairs = (
    ssn4_pairs.select(*pair_columns)
    .unionByName(dob_pairs.select(*pair_columns))
    .unionByName(dob_window_pairs.select(*pair_columns))
    .dropDuplicates(["doc_id", "member_id"])
)

total_pairs = all_pairs.count()
print(f"Total deduplicated candidate pairs: {total_pairs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compute Similarity Features
# MAGIC
# MAGIC For each candidate pair, compute:
# MAGIC - **Jaro-Winkler** on first_name and last_name (0.0-1.0)
# MAGIC - **DOB match score** (1.0 exact, 0.8 transposed, 0.5 close, 0.0 mismatch)
# MAGIC - **SSN4 match score** (1.0 exact, 0.5 partial, 0.0 mismatch)
# MAGIC - **Exact match indicators** for gender, phone, city, state, zip

# COMMAND ----------

# Apply similarity UDFs to compute feature vectors
features_df = (
    all_pairs
    # Name similarity via Jaro-Winkler
    .withColumn("jw_first_name",
        jaro_winkler_udf(F.col("doc_first_name"), F.col("member_first_name")))
    .withColumn("jw_last_name",
        jaro_winkler_udf(F.col("doc_last_name"), F.col("member_last_name")))

    # DOB comparison
    .withColumn("dob_exact_match",
        F.when(F.col("doc_dob") == F.col("member_dob"), 1).otherwise(0))
    .withColumn("dob_partial_match",
        F.when(
            (F.col("doc_dob").isNotNull()) & (F.col("member_dob").isNotNull()) &
            (F.substring("doc_dob", 1, 4) == F.substring("member_dob", 1, 4)),
            1
        ).otherwise(0))

    # SSN4 comparison
    .withColumn("ssn4_exact_match",
        F.when(
            (F.col("doc_ssn4").isNotNull()) & (F.col("member_ssn4").isNotNull()) &
            (F.col("doc_ssn4") == F.col("member_ssn4")),
            1
        ).otherwise(0))

    # Demographic exact matches
    .withColumn("gender_match", F.lit(0))  # doc doesn't have gender; placeholder
    .withColumn("phone_match", F.lit(0))   # doc doesn't have phone; placeholder
    .withColumn("city_match", F.lit(0))    # doc doesn't have city; placeholder
    .withColumn("state_match", F.lit(0))   # doc doesn't have state; placeholder
    .withColumn("zip_match", F.lit(0))     # doc doesn't have zip; placeholder

    # Generate a unique pair_id
    .withColumn("pair_id", F.md5(F.concat("doc_id", F.lit("|"), "member_id")))
    .withColumn("created_at", F.current_timestamp())
)

display(features_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Features to Delta Table

# COMMAND ----------

(
    features_df
    .select(
        "pair_id", "doc_id", "member_id", "blocking_key",
        "doc_first_name", "doc_last_name", "doc_dob", "doc_ssn4",
        "member_first_name", "member_last_name", "member_dob", "member_ssn4",
        "jw_first_name", "jw_last_name",
        "dob_exact_match", "dob_partial_match", "ssn4_exact_match",
        "gender_match", "phone_match", "city_match", "state_match", "zip_match",
        "created_at"
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FEATURES_TABLE)
)

print(f"✓ Written {features_df.count()} feature rows to {FEATURES_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Feature Distribution Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Name similarity distribution
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN jw_first_name >= 0.95 THEN '0.95-1.00 (very high)'
# MAGIC     WHEN jw_first_name >= 0.85 THEN '0.85-0.95 (high)'
# MAGIC     WHEN jw_first_name >= 0.70 THEN '0.70-0.85 (medium)'
# MAGIC     ELSE '< 0.70 (low)'
# MAGIC   END AS first_name_similarity_band,
# MAGIC   COUNT(*) AS pair_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
# MAGIC FROM healthcare_demo.analytics.doc_member_pairs_features
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Blocking key distribution
# MAGIC SELECT
# MAGIC   blocking_key,
# MAGIC   COUNT(*) AS pair_count
# MAGIC FROM healthcare_demo.analytics.doc_member_pairs_features
# MAGIC GROUP BY blocking_key
# MAGIC ORDER BY pair_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Notebook 05** to apply Fellegi-Sunter probabilistic weights
# MAGIC and classify matches.
