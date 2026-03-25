"""
Data quality checks for the clinical document matching pipeline.

Each check returns a structured result dict that can be written to a Delta
table for historical trending or surfaced in a Databricks dashboard.
"""

from datetime import datetime


def run_quality_checks(spark, catalog: str = "healthcare_demo") -> dict:
    """
    Run comprehensive data-quality checks on the clinical document matching
    pipeline tables.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session with access to the specified catalog.
    catalog : str
        Unity Catalog name.  Default ``'healthcare_demo'``.

    Returns
    -------
    dict
        Keys are check names; values are dicts with:

        * ``status`` -- ``'PASS'`` or ``'FAIL'``
        * ``value`` -- the measured metric
        * ``threshold`` -- the pass/fail threshold
        * ``description`` -- human-readable explanation

    Checks Performed
    ----------------
    1. **pct_unreadable_docs** -- Percent of documents that could not be
       parsed (threshold: < 15%).
    2. **pct_missing_dob** -- Percent of parsed documents missing a DOB
       (threshold: < 20%).
    3. **pct_missing_ssn4** -- Percent of parsed documents missing SSN4
       (threshold: < 25%).
    4. **match_category_distribution** -- Distribution of match/
       possible_match/non_match categories (informational, always PASS).
    5. **avg_parse_confidence** -- Average parse confidence score
       (threshold: > 0.7).
    6. **pct_auths_with_match** -- Percent of authorisations with at
       least one matched document (informational).
    7. **orphaned_documents** -- Count of documents with zero match
       candidates (informational).
    8. **duplicate_match_detection** -- Count of documents matched to
       multiple members with high confidence (threshold: < 5%).
    """
    results = {}
    run_ts = datetime.utcnow().isoformat()

    # ----------------------------------------------------------------
    # Helper: safely read a table or return None
    # ----------------------------------------------------------------
    def _read(schema, table):
        try:
            return spark.table(f"{catalog}.{schema}.{table}")
        except Exception:
            return None

    # Table references
    raw_docs = _read("raw", "clinical_documents")
    parsed_docs = _read("curated", "clinical_doc_parsed")
    match_candidates = _read("analytics", "match_candidates")
    auths = _read("curated", "authorizations")

    # ----------------------------------------------------------------
    # 1. Percent unreadable documents
    # ----------------------------------------------------------------
    if raw_docs is not None and parsed_docs is not None:
        total_raw = raw_docs.count()
        total_parsed = parsed_docs.count()
        if total_raw > 0:
            pct_unreadable = ((total_raw - total_parsed) / total_raw) * 100.0
        else:
            pct_unreadable = 0.0
        results["pct_unreadable_docs"] = {
            "status": "PASS" if pct_unreadable < 15.0 else "FAIL",
            "value": round(pct_unreadable, 2),
            "threshold": "< 15%",
            "description": (
                f"{pct_unreadable:.1f}% of documents could not be parsed "
                f"({total_raw - total_parsed} of {total_raw})."
            ),
            "run_ts": run_ts,
        }
    else:
        results["pct_unreadable_docs"] = {
            "status": "SKIP",
            "value": None,
            "threshold": "< 15%",
            "description": "Required tables not found. Skipped.",
            "run_ts": run_ts,
        }

    # ----------------------------------------------------------------
    # 2. Percent documents with missing DOB
    # ----------------------------------------------------------------
    if parsed_docs is not None:
        total_parsed = parsed_docs.count()
        missing_dob = parsed_docs.filter("dob IS NULL OR dob = ''").count()
        pct_missing = (missing_dob / total_parsed * 100.0) if total_parsed > 0 else 0.0
        results["pct_missing_dob"] = {
            "status": "PASS" if pct_missing < 20.0 else "FAIL",
            "value": round(pct_missing, 2),
            "threshold": "< 20%",
            "description": (
                f"{pct_missing:.1f}% of parsed documents are missing DOB "
                f"({missing_dob} of {total_parsed})."
            ),
            "run_ts": run_ts,
        }
    else:
        results["pct_missing_dob"] = {
            "status": "SKIP", "value": None, "threshold": "< 20%",
            "description": "Parsed documents table not found.", "run_ts": run_ts,
        }

    # ----------------------------------------------------------------
    # 3. Percent documents with missing SSN4
    # ----------------------------------------------------------------
    if parsed_docs is not None:
        total_parsed = parsed_docs.count()
        missing_ssn4 = parsed_docs.filter(
            "ssn_last4 IS NULL OR ssn_last4 = '' OR ssn4 IS NULL OR ssn4 = ''"
        ).count()
        pct_missing = (missing_ssn4 / total_parsed * 100.0) if total_parsed > 0 else 0.0
        results["pct_missing_ssn4"] = {
            "status": "PASS" if pct_missing < 25.0 else "FAIL",
            "value": round(pct_missing, 2),
            "threshold": "< 25%",
            "description": (
                f"{pct_missing:.1f}% of parsed documents are missing SSN4 "
                f"({missing_ssn4} of {total_parsed})."
            ),
            "run_ts": run_ts,
        }
    else:
        results["pct_missing_ssn4"] = {
            "status": "SKIP", "value": None, "threshold": "< 25%",
            "description": "Parsed documents table not found.", "run_ts": run_ts,
        }

    # ----------------------------------------------------------------
    # 4. Match category distribution
    # ----------------------------------------------------------------
    if match_candidates is not None:
        from pyspark.sql import functions as F

        dist_rows = (
            match_candidates
            .groupBy("match_classification")
            .agg(F.count("*").alias("cnt"))
            .collect()
        )
        distribution = {row["match_classification"]: row["cnt"] for row in dist_rows}
        total_candidates = sum(distribution.values())
        pct_dist = {
            k: round(v / total_candidates * 100.0, 2) if total_candidates > 0 else 0.0
            for k, v in distribution.items()
        }
        results["match_category_distribution"] = {
            "status": "PASS",
            "value": pct_dist,
            "threshold": "informational",
            "description": (
                f"Match distribution across {total_candidates} candidates: "
                + ", ".join(f"{k}={v}%" for k, v in sorted(pct_dist.items()))
            ),
            "run_ts": run_ts,
        }
    else:
        results["match_category_distribution"] = {
            "status": "SKIP", "value": None, "threshold": "informational",
            "description": "Match candidates table not found.", "run_ts": run_ts,
        }

    # ----------------------------------------------------------------
    # 5. Average parse confidence score
    # ----------------------------------------------------------------
    if parsed_docs is not None:
        from pyspark.sql import functions as F

        avg_conf_row = parsed_docs.agg(F.avg("confidence_score").alias("avg_conf")).collect()
        avg_conf = avg_conf_row[0]["avg_conf"] if avg_conf_row else None
        if avg_conf is not None:
            results["avg_parse_confidence"] = {
                "status": "PASS" if avg_conf > 0.7 else "FAIL",
                "value": round(float(avg_conf), 4),
                "threshold": "> 0.7",
                "description": f"Average parse confidence: {avg_conf:.4f}.",
                "run_ts": run_ts,
            }
        else:
            results["avg_parse_confidence"] = {
                "status": "SKIP", "value": None, "threshold": "> 0.7",
                "description": "No confidence scores available.", "run_ts": run_ts,
            }
    else:
        results["avg_parse_confidence"] = {
            "status": "SKIP", "value": None, "threshold": "> 0.7",
            "description": "Parsed documents table not found.", "run_ts": run_ts,
        }

    # ----------------------------------------------------------------
    # 6. Percent of auths with matched documents
    # ----------------------------------------------------------------
    if auths is not None and match_candidates is not None:
        from pyspark.sql import functions as F

        total_auths = auths.count()
        matched_auths = (
            match_candidates
            .filter("match_classification = 'match'")
            .select("auth_id")
            .distinct()
            .count()
        )
        pct = (matched_auths / total_auths * 100.0) if total_auths > 0 else 0.0
        results["pct_auths_with_match"] = {
            "status": "PASS",
            "value": round(pct, 2),
            "threshold": "informational",
            "description": (
                f"{pct:.1f}% of authorisations have a matched document "
                f"({matched_auths} of {total_auths})."
            ),
            "run_ts": run_ts,
        }
    else:
        results["pct_auths_with_match"] = {
            "status": "SKIP", "value": None, "threshold": "informational",
            "description": "Auth or match tables not found.", "run_ts": run_ts,
        }

    # ----------------------------------------------------------------
    # 7. Orphaned documents (no match candidate)
    # ----------------------------------------------------------------
    if parsed_docs is not None and match_candidates is not None:
        from pyspark.sql import functions as F

        orphaned = (
            parsed_docs.alias("p")
            .join(
                match_candidates.select("doc_id").distinct().alias("m"),
                on=F.col("p.doc_id") == F.col("m.doc_id"),
                how="left_anti",
            )
            .count()
        )
        results["orphaned_documents"] = {
            "status": "PASS",
            "value": orphaned,
            "threshold": "informational",
            "description": f"{orphaned} parsed documents have no match candidates.",
            "run_ts": run_ts,
        }
    else:
        results["orphaned_documents"] = {
            "status": "SKIP", "value": None, "threshold": "informational",
            "description": "Required tables not found.", "run_ts": run_ts,
        }

    # ----------------------------------------------------------------
    # 8. Duplicate match detection
    # ----------------------------------------------------------------
    if match_candidates is not None:
        from pyspark.sql import functions as F

        total_docs = match_candidates.select("doc_id").distinct().count()
        dup_matches = (
            match_candidates
            .filter("match_classification = 'match'")
            .groupBy("doc_id")
            .agg(F.countDistinct("member_id").alias("member_count"))
            .filter("member_count > 1")
            .count()
        )
        pct_dup = (dup_matches / total_docs * 100.0) if total_docs > 0 else 0.0
        results["duplicate_match_detection"] = {
            "status": "PASS" if pct_dup < 5.0 else "FAIL",
            "value": round(pct_dup, 2),
            "threshold": "< 5%",
            "description": (
                f"{pct_dup:.1f}% of documents matched to multiple members "
                f"({dup_matches} of {total_docs})."
            ),
            "run_ts": run_ts,
        }
    else:
        results["duplicate_match_detection"] = {
            "status": "SKIP", "value": None, "threshold": "< 5%",
            "description": "Match candidates table not found.", "run_ts": run_ts,
        }

    return results


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def generate_dq_report(check_results: dict) -> str:
    """
    Format data-quality check results as a human-readable report string.

    Parameters
    ----------
    check_results : dict
        Output of :func:`run_quality_checks`.

    Returns
    -------
    str
        Multi-line report suitable for printing to the console or storing
        as a notebook cell output.
    """
    lines = [
        "=" * 70,
        "  CLINICAL DOCUMENT MATCHING -- DATA QUALITY REPORT",
        "=" * 70,
        "",
    ]

    pass_count = 0
    fail_count = 0
    skip_count = 0

    for check_name, result in sorted(check_results.items()):
        status = result.get("status", "UNKNOWN")
        value = result.get("value")
        threshold = result.get("threshold", "")
        description = result.get("description", "")

        if status == "PASS":
            icon = "[PASS]"
            pass_count += 1
        elif status == "FAIL":
            icon = "[FAIL]"
            fail_count += 1
        else:
            icon = "[SKIP]"
            skip_count += 1

        lines.append(f"  {icon}  {check_name}")
        lines.append(f"         Value: {value}  |  Threshold: {threshold}")
        lines.append(f"         {description}")
        lines.append("")

    lines.append("-" * 70)
    lines.append(
        f"  Summary: {pass_count} PASS, {fail_count} FAIL, {skip_count} SKIP"
    )
    overall = "ALL CHECKS PASSED" if fail_count == 0 else "ACTION REQUIRED"
    lines.append(f"  Overall: {overall}")
    lines.append("=" * 70)

    return "\n".join(lines)
