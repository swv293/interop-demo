"""
Blocking strategies for clinical document matching.

Blocking drastically reduces the candidate pair space by only comparing
records that share one or more coarse-grained keys.  Without blocking,
comparing N documents against M members requires O(N*M) comparisons;
with good blocking keys this drops to O(N*k) where k << M.

All functions operate on PySpark DataFrames and use ``pyspark.sql.functions``
for distributed execution.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# Soundex implementation (pure Python, UDF-friendly)
# ---------------------------------------------------------------------------

def soundex(name: str) -> str:
    """
    Compute the American Soundex code for a name string.

    The algorithm produces a four-character code where the first character
    is the uppercase first letter and the remaining three are digits
    derived from the consonant pattern.

    Parameters
    ----------
    name : str
        Input name.  Leading/trailing whitespace is stripped and the
        string is uppercased before processing.

    Returns
    -------
    str
        Four-character Soundex code, or ``'0000'`` if the input is
        None/empty or contains no alphabetic characters.
    """
    if not name:
        return "0000"

    name = name.strip().upper()
    if not name:
        return "0000"

    # Filter to alpha only
    alpha = [c for c in name if c.isalpha()]
    if not alpha:
        return "0000"

    # Soundex mapping
    mapping = {
        "B": "1", "F": "1", "P": "1", "V": "1",
        "C": "2", "G": "2", "J": "2", "K": "2",
        "Q": "2", "S": "2", "X": "2", "Z": "2",
        "D": "3", "T": "3",
        "L": "4",
        "M": "5", "N": "5",
        "R": "6",
    }

    # First letter is kept as-is
    code = [alpha[0]]
    prev_digit = mapping.get(alpha[0], "0")

    for ch in alpha[1:]:
        digit = mapping.get(ch, "0")
        if digit != "0" and digit != prev_digit:
            code.append(digit)
            if len(code) == 4:
                break
        prev_digit = digit

    # Pad with zeros if needed
    while len(code) < 4:
        code.append("0")

    return "".join(code[:4])


# Register as UDF
soundex_udf = F.udf(soundex, StringType())


# ---------------------------------------------------------------------------
# Blocking key generation
# ---------------------------------------------------------------------------

def generate_blocking_keys(df: DataFrame, key_type: str = "standard") -> DataFrame:
    """
    Add a ``blocking_key`` column to a DataFrame for candidate pair generation.

    Four blocking strategies are supported, each trading off recall against
    the number of candidate pairs produced.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame.  Expected columns depend on the strategy:

        * ``standard`` -- requires ``zip_code`` (or ``zip``) and ``dob``
        * ``ssn4`` -- requires ``ssn_last4`` (or ``ssn4``)
        * ``soundex`` -- requires ``last_name`` and ``dob``
        * ``dob_window`` -- requires ``dob``

    key_type : str
        One of ``'standard'``, ``'ssn4'``, ``'soundex'``, ``'dob_window'``.

    Returns
    -------
    pyspark.sql.DataFrame
        Original DataFrame with an additional ``blocking_key`` column.
        Rows where the blocking key cannot be computed are dropped.
    """
    cols = [c.lower() for c in df.columns]

    if key_type == "standard":
        # Block on (zip_code, year from DOB)
        zip_col = "zip_code" if "zip_code" in cols else "zip"
        df_out = (
            df
            .withColumn(
                "blocking_key",
                F.concat_ws(
                    "|",
                    F.lit("STD"),
                    F.coalesce(F.col(zip_col).cast("string"), F.lit("")),
                    F.year(F.to_date(F.col("dob"))).cast("string"),
                ),
            )
            .filter(
                F.col(zip_col).isNotNull() & F.col("dob").isNotNull()
            )
        )
        return df_out

    elif key_type == "ssn4":
        # Block on SSN last 4 where available
        ssn_col = "ssn_last4" if "ssn_last4" in cols else "ssn4"
        df_out = (
            df
            .withColumn(
                "blocking_key",
                F.concat(F.lit("SSN4|"), F.col(ssn_col).cast("string")),
            )
            .filter(
                F.col(ssn_col).isNotNull()
                & (F.length(F.col(ssn_col).cast("string")) == 4)
            )
        )
        return df_out

    elif key_type == "soundex":
        # Block on soundex(last_name) + year from DOB
        df_out = (
            df
            .withColumn("_sdx", soundex_udf(F.col("last_name")))
            .withColumn(
                "blocking_key",
                F.concat_ws(
                    "|",
                    F.lit("SDX"),
                    F.col("_sdx"),
                    F.year(F.to_date(F.col("dob"))).cast("string"),
                ),
            )
            .drop("_sdx")
            .filter(
                F.col("last_name").isNotNull() & F.col("dob").isNotNull()
            )
        )
        return df_out

    elif key_type == "dob_window":
        # Create keys for DOB +/- 1 day to catch transposition errors.
        # Each record produces up to 3 blocking keys (day-1, day, day+1).
        df_out = (
            df
            .withColumn("_dob_date", F.to_date(F.col("dob")))
            .filter(F.col("_dob_date").isNotNull())
        )

        # Generate three rows per record
        frames = []
        for offset in [-1, 0, 1]:
            frame = (
                df_out
                .withColumn(
                    "blocking_key",
                    F.concat(
                        F.lit("DOB|"),
                        F.date_format(
                            F.date_add(F.col("_dob_date"), offset),
                            "yyyy-MM-dd",
                        ),
                    ),
                )
            )
            frames.append(frame)

        result = frames[0]
        for f in frames[1:]:
            result = result.unionByName(f, allowMissingColumns=True)

        return result.drop("_dob_date")

    else:
        raise ValueError(
            f"Unknown key_type '{key_type}'. "
            f"Must be one of: standard, ssn4, soundex, dob_window"
        )


# ---------------------------------------------------------------------------
# Candidate pair creation
# ---------------------------------------------------------------------------

def create_candidate_pairs(
    doc_df: DataFrame,
    member_df: DataFrame,
    blocking_keys: list = None,
) -> DataFrame:
    """
    Create candidate pairs between parsed documents and member records.

    Applies each requested blocking strategy independently, joins document
    and member DataFrames on the resulting blocking keys, unions all pairs,
    and deduplicates.

    Parameters
    ----------
    doc_df : pyspark.sql.DataFrame
        Parsed clinical documents.  Must contain a ``doc_id`` column.
    member_df : pyspark.sql.DataFrame
        Member/patient master records.  Must contain a ``member_id`` column.
    blocking_keys : list of str
        Blocking strategies to apply.  Defaults to
        ``['standard', 'ssn4']``.

    Returns
    -------
    pyspark.sql.DataFrame
        Columns: ``doc_id``, ``member_id``, ``blocking_strategy``.
        Each row is a unique candidate pair (deduplicated across
        strategies).  The ``blocking_strategy`` column records which
        strategy produced the pair.
    """
    if blocking_keys is None:
        blocking_keys = ["standard", "ssn4"]

    all_pairs = None

    for strategy in blocking_keys:
        # Generate blocking keys for both sides
        doc_blocked = generate_blocking_keys(doc_df, key_type=strategy)
        member_blocked = generate_blocking_keys(member_df, key_type=strategy)

        # Join on blocking_key to produce candidate pairs
        pairs = (
            doc_blocked.alias("d")
            .join(
                member_blocked.alias("m"),
                on=F.col("d.blocking_key") == F.col("m.blocking_key"),
                how="inner",
            )
            .select(
                F.col("d.doc_id").alias("doc_id"),
                F.col("m.member_id").alias("member_id"),
                F.lit(strategy).alias("blocking_strategy"),
            )
        )

        if all_pairs is None:
            all_pairs = pairs
        else:
            all_pairs = all_pairs.unionByName(pairs)

    if all_pairs is None:
        # Return empty DataFrame if no strategies were provided
        from pyspark.sql.types import StructType, StructField, StringType as ST
        schema = StructType([
            StructField("doc_id", ST(), True),
            StructField("member_id", ST(), True),
            StructField("blocking_strategy", ST(), True),
        ])
        return doc_df.sparkSession.createDataFrame([], schema)

    # Deduplicate: keep each (doc_id, member_id) only once.
    # Collect the set of strategies that produced the pair.
    deduped = (
        all_pairs
        .groupBy("doc_id", "member_id")
        .agg(
            F.concat_ws(",", F.collect_set("blocking_strategy")).alias("blocking_strategy")
        )
    )

    return deduped
