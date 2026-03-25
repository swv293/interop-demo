"""
PySpark-compatible similarity functions for clinical document matching.

All functions are pure Python (no external dependencies) so they can be
registered as PySpark UDFs and executed on Spark workers without additional
package installation.
"""

from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf


# ---------------------------------------------------------------------------
# Jaro-Winkler Similarity
# ---------------------------------------------------------------------------

def jaro_winkler_similarity(s1: str, s2: str) -> float:
    """
    Compute the Jaro-Winkler similarity between two strings.

    Pure Python implementation with no external dependencies so it can run
    inside a PySpark UDF on any cluster node.

    Parameters
    ----------
    s1 : str
        First string.
    s2 : str
        Second string.

    Returns
    -------
    float
        Similarity score in [0.0, 1.0].  Returns 0.0 when either input is
        None or empty.

    Notes
    -----
    * Prefix scaling factor ``p = 0.1`` (Winkler default).
    * Maximum common-prefix length considered is 4 characters.
    """
    if not s1 or not s2:
        return 0.0

    s1 = s1.strip().upper()
    s2 = s2.strip().upper()

    if not s1 or not s2:
        return 0.0

    if s1 == s2:
        return 1.0

    len_s1 = len(s1)
    len_s2 = len(s2)

    # Maximum distance for matching characters
    match_distance = max(len_s1, len_s2) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s1_matches = [False] * len_s1
    s2_matches = [False] * len_s2

    matches = 0
    transpositions = 0

    # Count matching characters
    for i in range(len_s1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len_s2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    # Count transpositions
    k = 0
    for i in range(len_s1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (
        matches / len_s1
        + matches / len_s2
        + (matches - transpositions / 2.0) / matches
    ) / 3.0

    # Winkler modification: boost for common prefix (max 4 chars)
    prefix_len = 0
    for i in range(min(4, len_s1, len_s2)):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    p = 0.1  # Winkler prefix weight
    return float(jaro + prefix_len * p * (1.0 - jaro))


# ---------------------------------------------------------------------------
# Date-of-Birth Match Score
# ---------------------------------------------------------------------------

def dob_match_score(dob1: str, dob2: str) -> float:
    """
    Score the similarity between two date-of-birth strings.

    Designed for healthcare member matching where DOB is a strong identifier
    but data-entry transposition errors are common (month/day swap).

    Parameters
    ----------
    dob1 : str
        Date of birth in ``YYYY-MM-DD`` format.
    dob2 : str
        Date of birth in ``YYYY-MM-DD`` format.

    Returns
    -------
    float
        * ``1.0`` -- exact match
        * ``0.8`` -- month and day are transposed (e.g. 1985-03-12 vs 1985-12-03)
        * ``0.5`` -- same year and month, day within +/-3
        * ``0.3`` -- same year only
        * ``0.0`` -- no meaningful match or invalid inputs
    """
    if not dob1 or not dob2:
        return 0.0

    dob1 = dob1.strip()
    dob2 = dob2.strip()

    if not dob1 or not dob2:
        return 0.0

    # Exact match
    if dob1 == dob2:
        return 1.0

    try:
        parts1 = dob1.split("-")
        parts2 = dob2.split("-")
        if len(parts1) != 3 or len(parts2) != 3:
            return 0.0

        y1, m1, d1 = int(parts1[0]), int(parts1[1]), int(parts1[2])
        y2, m2, d2 = int(parts2[0]), int(parts2[1]), int(parts2[2])
    except (ValueError, IndexError):
        return 0.0

    # Transposed month/day (e.g. 1985-03-12 vs 1985-12-03)
    if y1 == y2 and m1 == d2 and d1 == m2:
        return 0.8

    # Same year and month, day within +/-3
    if y1 == y2 and m1 == m2 and abs(d1 - d2) <= 3:
        return 0.5

    # Same year only
    if y1 == y2:
        return 0.3

    return 0.0


# ---------------------------------------------------------------------------
# SSN Last-4 Match Score
# ---------------------------------------------------------------------------

def ssn4_match_score(ssn1: str, ssn2: str) -> float:
    """
    Score the similarity between two last-four-digit SSN strings.

    Parameters
    ----------
    ssn1 : str
        Last 4 digits of SSN.
    ssn2 : str
        Last 4 digits of SSN.

    Returns
    -------
    float
        * ``1.0`` -- exact match
        * ``0.5`` -- 3 of 4 digits match (in corresponding positions)
        * ``0.0`` -- otherwise, or invalid/missing input
    """
    if not ssn1 or not ssn2:
        return 0.0

    ssn1 = ssn1.strip()
    ssn2 = ssn2.strip()

    if len(ssn1) != 4 or len(ssn2) != 4:
        return 0.0

    if ssn1 == ssn2:
        return 1.0

    # Count positional digit matches
    matching_digits = sum(1 for a, b in zip(ssn1, ssn2) if a == b)

    if matching_digits >= 3:
        return 0.5

    return 0.0


# ---------------------------------------------------------------------------
# Gender Match Score
# ---------------------------------------------------------------------------

def gender_match_score(g1: str, g2: str) -> float:
    """
    Score the similarity between two gender values.

    Parameters
    ----------
    g1 : str
        Gender value (e.g. 'M', 'F', 'Male', 'Female', 'Unknown').
    g2 : str
        Gender value.

    Returns
    -------
    float
        * ``1.0`` -- exact match (case-insensitive, normalised)
        * ``0.5`` -- either value is 'Unknown' or missing
        * ``0.0`` -- mismatch
    """
    if not g1 or not g2:
        return 0.5

    g1 = g1.strip().upper()
    g2 = g2.strip().upper()

    if not g1 or not g2:
        return 0.5

    # Normalise common representations
    norm = {
        "M": "M", "MALE": "M",
        "F": "F", "FEMALE": "F",
        "U": "UNKNOWN", "UNKNOWN": "UNKNOWN",
        "O": "OTHER", "OTHER": "OTHER",
    }
    g1_norm = norm.get(g1, g1)
    g2_norm = norm.get(g2, g2)

    if g1_norm == "UNKNOWN" or g2_norm == "UNKNOWN":
        return 0.5

    if g1_norm == g2_norm:
        return 1.0

    return 0.0


# ---------------------------------------------------------------------------
# Phone Match Score
# ---------------------------------------------------------------------------

def phone_match_score(p1: str, p2: str) -> float:
    """
    Score the similarity between two phone number strings.

    Parameters
    ----------
    p1 : str
        Phone number (any format -- digits are extracted).
    p2 : str
        Phone number.

    Returns
    -------
    float
        * ``1.0`` -- exact digit match
        * ``0.7`` -- last 7 digits match (same local number, different area code)
        * ``0.0`` -- otherwise or invalid input
    """
    if not p1 or not p2:
        return 0.0

    # Strip to digits only
    digits1 = "".join(c for c in p1 if c.isdigit())
    digits2 = "".join(c for c in p2 if c.isdigit())

    if not digits1 or not digits2:
        return 0.0

    if digits1 == digits2:
        return 1.0

    # Compare last 7 digits (local number without area code)
    if len(digits1) >= 7 and len(digits2) >= 7:
        if digits1[-7:] == digits2[-7:]:
            return 0.7

    return 0.0


# ---------------------------------------------------------------------------
# PySpark UDF Registrations
# ---------------------------------------------------------------------------

jaro_winkler_udf = udf(jaro_winkler_similarity, FloatType())
dob_match_udf = udf(dob_match_score, FloatType())
ssn4_match_udf = udf(ssn4_match_score, FloatType())
gender_match_udf = udf(gender_match_score, FloatType())
phone_match_udf = udf(phone_match_score, FloatType())
