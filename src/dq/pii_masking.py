"""
PHI / PII masking utilities for the clinical document matching pipeline.

Creates Unity Catalog masking views and column-level masking functions
so that downstream consumers (analysts, dashboards) never see raw
protected health information.
"""


def create_masking_views(spark, catalog: str = "healthcare_demo") -> dict:
    """
    Create Unity Catalog masking views for PHI/PII protection.

    Three views are created in the specified catalog:

    1. ``curated.member_masked``
       SSN4 fully masked (``****``), DOB shows only the year, phone
       number shows only the last 4 digits.

    2. ``curated.clinical_doc_parsed_masked``
       Extracted SSN4 and DOB are masked in the same manner as the
       member view.

    3. ``analytics.match_candidates_safe``
       Contains only scores and classifications -- no PII columns at all.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session with CREATE VIEW privileges on the catalog.
    catalog : str
        Unity Catalog name.  Default ``'healthcare_demo'``.

    Returns
    -------
    dict
        Keys are view names; values are ``'created'`` or an error message.
    """
    results = {}

    # ----------------------------------------------------------------
    # 1. member_masked
    # ----------------------------------------------------------------
    view_name = f"{catalog}.curated.member_masked"
    sql = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        member_id,
        first_name,
        last_name,
        '****' AS ssn_last4,
        CONCAT(CAST(YEAR(TO_DATE(dob)) AS STRING), '-**-**') AS dob,
        gender,
        CASE
            WHEN phone IS NOT NULL
            THEN CONCAT('***-***-', RIGHT(phone, 4))
            ELSE NULL
        END AS phone,
        address_line1,
        city,
        state,
        zip_code,
        created_at,
        updated_at
    FROM {catalog}.curated.members
    """
    try:
        spark.sql(sql)
        results[view_name] = "created"
    except Exception as e:
        results[view_name] = f"error: {str(e)}"

    # ----------------------------------------------------------------
    # 2. clinical_doc_parsed_masked
    # ----------------------------------------------------------------
    view_name = f"{catalog}.curated.clinical_doc_parsed_masked"
    sql = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        doc_id,
        document_type,
        source_system,
        first_name,
        last_name,
        '****' AS ssn_last4,
        CASE
            WHEN dob IS NOT NULL
            THEN CONCAT(CAST(YEAR(TO_DATE(dob)) AS STRING), '-**-**')
            ELSE NULL
        END AS dob,
        gender,
        CASE
            WHEN phone IS NOT NULL
            THEN CONCAT('***-***-', RIGHT(phone, 4))
            ELSE NULL
        END AS phone,
        city,
        state,
        zip_code,
        confidence_score,
        parsed_at
    FROM {catalog}.curated.clinical_doc_parsed
    """
    try:
        spark.sql(sql)
        results[view_name] = "created"
    except Exception as e:
        results[view_name] = f"error: {str(e)}"

    # ----------------------------------------------------------------
    # 3. match_candidates_safe
    # ----------------------------------------------------------------
    view_name = f"{catalog}.analytics.match_candidates_safe"
    sql = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        doc_id,
        member_id,
        blocking_strategy,
        first_name_score,
        last_name_score,
        dob_score,
        ssn4_score,
        gender_score,
        phone_score,
        city_score,
        state_score,
        zip_score,
        total_weight,
        match_classification,
        run_ts
    FROM {catalog}.analytics.match_candidates
    """
    try:
        spark.sql(sql)
        results[view_name] = "created"
    except Exception as e:
        results[view_name] = f"error: {str(e)}"

    return results


def apply_column_masks(spark, catalog: str = "healthcare_demo") -> dict:
    """
    Create column-level masking functions and apply them via Unity Catalog.

    Uses ``CREATE FUNCTION`` to define dynamic masking functions that
    return the real value for privileged users (members of the
    ``phi_authorized`` group) and a masked value for everyone else.

    Three masking functions are created:

    * ``mask_ssn4(val STRING)`` -- returns ``****`` for non-privileged users.
    * ``mask_dob(val STRING)`` -- returns ``YYYY-**-**`` for non-privileged users.
    * ``mask_phone(val STRING)`` -- returns ``***-***-XXXX`` for non-privileged users.

    After creating the functions, ``ALTER TABLE ... ALTER COLUMN ... SET MASK``
    is used to bind each function to the appropriate column.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session with administrative privileges.
    catalog : str
        Unity Catalog name.  Default ``'healthcare_demo'``.

    Returns
    -------
    dict
        Keys are operation descriptions; values are ``'applied'`` or an
        error message.
    """
    results = {}

    # ----------------------------------------------------------------
    # Masking function: SSN4
    # ----------------------------------------------------------------
    func_name = f"{catalog}.curated.mask_ssn4"
    sql = f"""
    CREATE OR REPLACE FUNCTION {func_name}(val STRING)
    RETURNS STRING
    RETURN
        CASE
            WHEN is_account_group_member('phi_authorized') THEN val
            ELSE '****'
        END
    """
    try:
        spark.sql(sql)
        results[f"function:{func_name}"] = "created"
    except Exception as e:
        results[f"function:{func_name}"] = f"error: {str(e)}"

    # ----------------------------------------------------------------
    # Masking function: DOB
    # ----------------------------------------------------------------
    func_name = f"{catalog}.curated.mask_dob"
    sql = f"""
    CREATE OR REPLACE FUNCTION {func_name}(val STRING)
    RETURNS STRING
    RETURN
        CASE
            WHEN is_account_group_member('phi_authorized') THEN val
            ELSE CONCAT(CAST(YEAR(TO_DATE(val)) AS STRING), '-**-**')
        END
    """
    try:
        spark.sql(sql)
        results[f"function:{func_name}"] = "created"
    except Exception as e:
        results[f"function:{func_name}"] = f"error: {str(e)}"

    # ----------------------------------------------------------------
    # Masking function: Phone
    # ----------------------------------------------------------------
    func_name = f"{catalog}.curated.mask_phone"
    sql = f"""
    CREATE OR REPLACE FUNCTION {func_name}(val STRING)
    RETURNS STRING
    RETURN
        CASE
            WHEN is_account_group_member('phi_authorized') THEN val
            ELSE CONCAT('***-***-', RIGHT(val, 4))
        END
    """
    try:
        spark.sql(sql)
        results[f"function:{func_name}"] = "created"
    except Exception as e:
        results[f"function:{func_name}"] = f"error: {str(e)}"

    # ----------------------------------------------------------------
    # Apply masks to members table
    # ----------------------------------------------------------------
    mask_bindings = [
        ("curated.members", "ssn_last4", f"{catalog}.curated.mask_ssn4"),
        ("curated.members", "dob", f"{catalog}.curated.mask_dob"),
        ("curated.members", "phone", f"{catalog}.curated.mask_phone"),
        ("curated.clinical_doc_parsed", "ssn_last4", f"{catalog}.curated.mask_ssn4"),
        ("curated.clinical_doc_parsed", "dob", f"{catalog}.curated.mask_dob"),
        ("curated.clinical_doc_parsed", "phone", f"{catalog}.curated.mask_phone"),
    ]

    for table, column, mask_func in mask_bindings:
        full_table = f"{catalog}.{table}"
        desc = f"mask:{full_table}.{column}"
        sql = f"""
        ALTER TABLE {full_table}
        ALTER COLUMN {column}
        SET MASK {mask_func}
        """
        try:
            spark.sql(sql)
            results[desc] = "applied"
        except Exception as e:
            results[desc] = f"error: {str(e)}"

    return results
