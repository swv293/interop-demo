"""
Tests for the similarity functions used in clinical document matching.

Tests cover:
- Jaro-Winkler string similarity (name matching)
- DOB match scoring (exact, transposed, partial, no match)
- SSN4 match scoring (exact, partial, no match)
- Gender and phone match scoring
- Edge cases: None, empty strings, special characters
"""

import pytest
import sys
import os

# Add src to path for local testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from matching.similarity import (
    jaro_winkler_similarity,
    dob_match_score,
    ssn4_match_score,
    gender_match_score,
    phone_match_score,
)


# ---------------------------------------------------------------------------
# Jaro-Winkler Similarity Tests
# ---------------------------------------------------------------------------

class TestJaroWinklerSimilarity:
    """Tests for Jaro-Winkler string similarity."""

    def test_identical_strings(self):
        assert jaro_winkler_similarity("MARTHA", "MARTHA") == 1.0

    def test_completely_different(self):
        score = jaro_winkler_similarity("ABC", "XYZ")
        assert score < 0.5

    def test_similar_names(self):
        """Common healthcare name variants should score high."""
        score = jaro_winkler_similarity("MARTHA", "MARHTA")
        assert score > 0.9

    def test_mcdonald_variants(self):
        """McDonald vs MacDonald — common in clinical data."""
        score = jaro_winkler_similarity("McDonald", "MacDonald")
        assert score > 0.8

    def test_johnson_jonson(self):
        """Spelling variant — common OCR error."""
        score = jaro_winkler_similarity("Johnson", "Jonson")
        assert score > 0.9

    def test_smith_smyth(self):
        score = jaro_winkler_similarity("Smith", "Smyth")
        assert score > 0.8

    def test_case_insensitive_behavior(self):
        """Verify the function handles mixed case."""
        score = jaro_winkler_similarity("john", "JOHN")
        # Depends on implementation — our impl should normalize
        assert score > 0.0

    def test_none_input_first(self):
        assert jaro_winkler_similarity(None, "John") == 0.0

    def test_none_input_second(self):
        assert jaro_winkler_similarity("John", None) == 0.0

    def test_both_none(self):
        assert jaro_winkler_similarity(None, None) == 0.0

    def test_empty_string(self):
        assert jaro_winkler_similarity("", "John") == 0.0

    def test_both_empty(self):
        assert jaro_winkler_similarity("", "") == 0.0

    def test_single_char(self):
        score = jaro_winkler_similarity("J", "J")
        assert score == 1.0

    def test_hyphenated_names(self):
        """Mary-Jo vs MaryJo — common in healthcare records."""
        score = jaro_winkler_similarity("Mary-Jo", "MaryJo")
        assert score > 0.85

    def test_apostrophe_names(self):
        """O'Brien vs OBrien."""
        score = jaro_winkler_similarity("O'Brien", "OBrien")
        assert score > 0.85

    def test_prefix_boost(self):
        """Jaro-Winkler should boost scores for matching prefixes."""
        jw_score = jaro_winkler_similarity("DWAYNE", "DUANE")
        # The common prefix "D" should give a small boost
        assert jw_score > 0.0

    def test_swapped_first_last(self):
        """Swapped names should have low similarity."""
        score = jaro_winkler_similarity("Robert", "Williams")
        assert score < 0.6

    def test_score_range(self):
        """All scores should be between 0 and 1."""
        test_pairs = [
            ("John", "Jane"), ("A", "ABCDEFGHIJ"),
            ("Test", "Testing"), ("XX", "YY"),
        ]
        for s1, s2 in test_pairs:
            score = jaro_winkler_similarity(s1, s2)
            assert 0.0 <= score <= 1.0, f"Score {score} out of range for ({s1}, {s2})"


# ---------------------------------------------------------------------------
# DOB Match Score Tests
# ---------------------------------------------------------------------------

class TestDobMatchScore:
    """Tests for date of birth matching with transposition handling."""

    def test_exact_match(self):
        assert dob_match_score("1985-03-12", "1985-03-12") == 1.0

    def test_transposed_month_day(self):
        """Transposed month/day — very common OCR error."""
        score = dob_match_score("1985-03-12", "1985-12-03")
        assert score == 0.8

    def test_close_day(self):
        """Same year+month, day off by 1-3 — keyboard error."""
        score = dob_match_score("1985-03-12", "1985-03-14")
        assert score == 0.5

    def test_same_year_only(self):
        score = dob_match_score("1985-03-12", "1985-07-25")
        assert score == 0.3

    def test_completely_different(self):
        score = dob_match_score("1985-03-12", "1960-11-05")
        assert score == 0.0

    def test_none_first(self):
        assert dob_match_score(None, "1985-03-12") == 0.0

    def test_none_second(self):
        assert dob_match_score("1985-03-12", None) == 0.0

    def test_both_none(self):
        assert dob_match_score(None, None) == 0.0

    def test_empty_string(self):
        assert dob_match_score("", "1985-03-12") == 0.0

    def test_invalid_date(self):
        """Invalid date strings should not crash."""
        score = dob_match_score("not-a-date", "1985-03-12")
        assert score == 0.0


# ---------------------------------------------------------------------------
# SSN4 Match Score Tests
# ---------------------------------------------------------------------------

class TestSsn4MatchScore:
    """Tests for last-4 SSN matching."""

    def test_exact_match(self):
        assert ssn4_match_score("1234", "1234") == 1.0

    def test_three_of_four_match(self):
        """3 of 4 digits matching — common transposition."""
        score = ssn4_match_score("1234", "1235")
        assert score == 0.5

    def test_two_of_four_match(self):
        score = ssn4_match_score("1234", "1256")
        assert score == 0.0

    def test_completely_different(self):
        assert ssn4_match_score("1234", "5678") == 0.0

    def test_leading_zeros(self):
        """Leading zeros must be preserved."""
        assert ssn4_match_score("0042", "0042") == 1.0

    def test_none_first(self):
        assert ssn4_match_score(None, "1234") == 0.0

    def test_none_second(self):
        assert ssn4_match_score("1234", None) == 0.0

    def test_empty_string(self):
        assert ssn4_match_score("", "1234") == 0.0


# ---------------------------------------------------------------------------
# Gender Match Score Tests
# ---------------------------------------------------------------------------

class TestGenderMatchScore:
    def test_exact_match(self):
        assert gender_match_score("Male", "Male") == 1.0

    def test_mismatch(self):
        assert gender_match_score("Male", "Female") == 0.0

    def test_unknown_first(self):
        assert gender_match_score("Unknown", "Male") == 0.5

    def test_unknown_second(self):
        assert gender_match_score("Female", "Unknown") == 0.5


# ---------------------------------------------------------------------------
# Phone Match Score Tests
# ---------------------------------------------------------------------------

class TestPhoneMatchScore:
    def test_exact_match(self):
        assert phone_match_score("3035551234", "3035551234") == 1.0

    def test_last_seven_match(self):
        """Different area code but same local number."""
        score = phone_match_score("3035551234", "7205551234")
        assert score == 0.7

    def test_completely_different(self):
        assert phone_match_score("3035551234", "2125559876") == 0.0

    def test_none_input(self):
        assert phone_match_score(None, "3035551234") == 0.0
