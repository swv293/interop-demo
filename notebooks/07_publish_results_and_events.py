# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Publish Match Results & Events
# MAGIC
# MAGIC This notebook reads high-confidence matches from the match candidate tables,
# MAGIC formats them as JSON events, and publishes to Kafka and/or a Delta event table.
# MAGIC
# MAGIC **Pipeline Position:** Step 7 of 7 (final step)
# MAGIC
# MAGIC **Key Databricks Features Used:**
# MAGIC - Delta table as event store
# MAGIC - Kafka producer integration (when configured)
# MAGIC - Structured JSON event payloads
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `serverless_stable_swv01_catalog.analytics.doc_member_match_candidates`
# MAGIC - `serverless_stable_swv01_catalog.analytics.doc_auth_match_candidates`
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `serverless_stable_swv01_catalog.analytics.match_events` — Delta event store
# MAGIC - Kafka topic `clinical-matching-results` (optional)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "serverless_stable_swv01_catalog"

# Kafka configuration (set to None to skip Kafka publishing)
KAFKA_CONFIG = None
# Uncomment and configure for Kafka:
# KAFKA_CONFIG = {
#     "kafka.bootstrap.servers": "your-kafka-broker:9092",
#     "topic": "clinical-matching-results",
#     "kafka.security.protocol": "SASL_SSL",
#     "kafka.sasl.mechanism": "PLAIN",
#     "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";',
# }

# Only publish matches above this confidence threshold
MIN_PUBLISH_WEIGHT = 6.0  # possible_match and above

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build Match Events from Member Matches

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import json

# Load high-confidence member matches
member_matches = (
    spark.table(f"{CATALOG}.analytics.doc_member_match_candidates")
    .filter(F.col("total_weight") >= MIN_PUBLISH_WEIGHT)
)

# Build structured event payloads
member_events = (
    member_matches
    .withColumn("event_id", F.expr("uuid()"))
    .withColumn("event_type", F.lit("MEMBER_MATCH"))
    .withColumn("event_payload", F.to_json(F.struct(
        F.col("match_id"),
        F.col("doc_id"),
        F.col("member_id"),
        F.col("blocking_key"),
        F.col("jw_first_name_score"),
        F.col("jw_last_name_score"),
        F.col("dob_match_score"),
        F.col("ssn4_match_score"),
        F.col("total_weight"),
        F.col("fellegi_sunter_log_odds"),
        F.col("match_classification"),
    )))
    .withColumn("match_score", F.col("total_weight"))
    .withColumn("match_method", F.col("blocking_key"))
    .withColumn("auth_id", F.lit(None).cast("string"))
    .withColumn("event_timestamp", F.current_timestamp())
    .withColumn("published_to_kafka", F.lit(False))
    .withColumn("kafka_publish_ts", F.lit(None).cast("timestamp"))
    .select(
        "event_id", "event_type", "doc_id", "member_id", "auth_id",
        "match_score", "match_classification", "match_method",
        "event_payload", "event_timestamp",
        "published_to_kafka", "kafka_publish_ts"
    )
)

print(f"Member match events to publish: {member_matches.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build Match Events from Authorization Matches

# COMMAND ----------

# Load auth matches
auth_matches = (
    spark.table(f"{CATALOG}.analytics.doc_auth_match_candidates")
    .filter(F.col("match_classification").isin("match", "possible_match"))
)

# Build auth match events
auth_events = (
    auth_matches
    .withColumn("event_id", F.expr("uuid()"))
    .withColumn("event_type", F.lit("AUTH_MATCH"))
    .withColumn("member_id", F.lit(None).cast("string"))
    .withColumn("event_payload", F.to_json(F.struct(
        F.col("match_id"),
        F.col("doc_id"),
        F.col("auth_id"),
        F.col("member_match_id"),
        F.col("auth_number_match"),
        F.col("service_date_overlap"),
        F.col("procedure_code_match"),
        F.col("provider_npi_match"),
        F.col("total_weight"),
        F.col("match_classification"),
    )))
    .withColumn("match_score", F.col("total_weight"))
    .withColumn("match_method",
        F.when(F.col("auth_number_match"), F.lit("auth_number_direct"))
        .otherwise(F.lit("probabilistic"))
    )
    .withColumn("event_timestamp", F.current_timestamp())
    .withColumn("published_to_kafka", F.lit(False))
    .withColumn("kafka_publish_ts", F.lit(None).cast("timestamp"))
    .select(
        "event_id", "event_type", "doc_id", "member_id", "auth_id",
        "match_score", "match_classification", "match_method",
        "event_payload", "event_timestamp",
        "published_to_kafka", "kafka_publish_ts"
    )
)

print(f"Auth match events to publish: {auth_matches.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Union All Events and Write to Delta

# COMMAND ----------

all_events = member_events.unionByName(auth_events)

(
    all_events
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOG}.analytics.match_events")
)

total_events = all_events.count()
print(f"✓ Published {total_events} events to {CATALOG}.analytics.match_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Publish to Kafka (Optional)
# MAGIC
# MAGIC When `KAFKA_CONFIG` is set, events are serialized as JSON and written to
# MAGIC the configured Kafka topic for consumption by downstream systems (MRM, CGX).

# COMMAND ----------

if KAFKA_CONFIG:
    topic = KAFKA_CONFIG.pop("topic", "clinical-matching-results")

    # Write events to Kafka
    (
        all_events
        .withColumn("key", F.col("doc_id").cast("string"))
        .withColumn("value", F.col("event_payload"))
        .select("key", "value")
        .write
        .format("kafka")
        .options(**KAFKA_CONFIG)
        .option("topic", topic)
        .save()
    )

    # Update the Delta table to mark events as published
    spark.sql(f"""
        UPDATE {CATALOG}.analytics.match_events
        SET published_to_kafka = true,
            kafka_publish_ts = CURRENT_TIMESTAMP()
        WHERE published_to_kafka = false
    """)

    print(f"✓ Published {total_events} events to Kafka topic: {topic}")
else:
    print("ℹ Kafka not configured — events written to Delta only.")
    print("  To enable Kafka, set KAFKA_CONFIG in the configuration cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Event Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   event_type,
# MAGIC   match_classification,
# MAGIC   COUNT(*) AS event_count,
# MAGIC   ROUND(AVG(match_score), 2) AS avg_score,
# MAGIC   SUM(CASE WHEN published_to_kafka THEN 1 ELSE 0 END) AS kafka_published
# MAGIC FROM serverless_stable_swv01_catalog.analytics.match_events
# MAGIC GROUP BY event_type, match_classification
# MAGIC ORDER BY event_type, event_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Match method distribution
# MAGIC SELECT
# MAGIC   match_method,
# MAGIC   COUNT(*) AS cnt,
# MAGIC   ROUND(AVG(match_score), 2) AS avg_score
# MAGIC FROM serverless_stable_swv01_catalog.analytics.match_events
# MAGIC GROUP BY match_method
# MAGIC ORDER BY cnt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sample Event Payloads (for API consumers)

# COMMAND ----------

# Show sample event payloads for documentation/API testing
sample_events = (
    spark.table(f"{CATALOG}.analytics.match_events")
    .filter(F.col("match_classification") == "match")
    .limit(3)
    .select("event_id", "event_type", "match_classification", "event_payload")
)

for row in sample_events.collect():
    print(f"\n--- {row.event_type} ({row.match_classification}) ---")
    print(json.dumps(json.loads(row.event_payload), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC
# MAGIC The clinical document matching pipeline has finished:
# MAGIC
# MAGIC 1. **Seed Data** → Member & authorization records loaded
# MAGIC 2. **Document Parsing** → PDFs/TIFFs parsed with `ai_parse_document`
# MAGIC 3. **Field Extraction** → Structured fields extracted with `ai_query`
# MAGIC 4. **Candidate Pairs** → Blocking + similarity features computed
# MAGIC 5. **F-S Scoring** → Fellegi-Sunter probabilistic weights applied
# MAGIC 6. **Quality & PII** → DQ checks passed, masking views created
# MAGIC 7. **Event Publishing** → Match events written to Delta (+ Kafka if configured)
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Review `possible_match` results in the work queue
# MAGIC - Configure Kafka for real-time downstream integration
# MAGIC - Schedule as a Databricks Workflow for automated processing
