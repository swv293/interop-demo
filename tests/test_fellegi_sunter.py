"""
Tests for the Fellegi-Sunter probabilistic record linkage model.

Tests cover:
- Agreement and disagreement weight computation
- Field weight computation with similarity scores
- Total weight aggregation
- Match classification thresholds
- Parameter validation
- Edge cases
"""

import pytest
import math
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from matching.fellegi_sunter import FellegiSunter


# ---------------------------------------------------------------------------
# Initialization Tests
# ---------------------------------------------------------------------------

class TestFellegiSunterInit:
    """Tests for model initialization and defaults."""

    def test_default_initialization(self):
        fs = FellegiSunter()
        assert fs.match_threshold == 12.0
        assert fs.possible_match_threshold == 6.0
        assert "first_name" in fs.params
        assert "ssn4" in fs.params

    def test_custom_thresholds(self):
        fs = FellegiSunter(match_threshold=15.0, possible_match_threshold=8.0)
        assert fs.match_threshold == 15.0
        assert fs.possible_match_threshold == 8.0

    def test_custom_params(self):
        custom = {"test_field": {"m": 0.95, "u": 0.01, "weight_multiplier": 2.0}}
        fs = FellegiSunter(params=custom)
        assert "test_field" in fs.params
        assert fs.params["test_field"]["m"] == 0.95


# ---------------------------------------------------------------------------
# Weight Computation Tests
# ---------------------------------------------------------------------------

class TestWeightComputation:
    """Tests for agreement and disagreement weight calculations."""

    def test_agreement_weight_positive(self):
        """Agreement weights should always be positive (m > u)."""
        fs = FellegiSunter()
        for field in fs.params:
            weight = fs.compute_agreement_weight(field)
            assert weight > 0, f"Agreement weight for {field} should be positive, got {weight}"

    def test_disagreement_weight_negative(self):
        """Disagreement weights should always be negative (1-m < 1-u)."""
        fs = FellegiSunter()
        for field in fs.params:
            weight = fs.compute_disagreement_weight(field)
            assert weight < 0, f"Disagreement weight for {field} should be negative, got {weight}"

    def test_ssn4_highest_agreement_weight(self):
        """SSN4 should have one of the highest agreement weights (5× multiplier)."""
        fs = FellegiSunter()
        ssn4_weight = fs.compute_agreement_weight("ssn4")
        name_weight = fs.compute_agreement_weight("first_name")
        assert ssn4_weight > name_weight * 3, (
            f"SSN4 weight ({ssn4_weight:.2f}) should be >3× name weight ({name_weight:.2f})"
        )

    def test_dob_heavy_weight(self):
        """DOB should have a heavy agreement weight (4× multiplier)."""
        fs = FellegiSunter()
        dob_weight = fs.compute_agreement_weight("dob")
        name_weight = fs.compute_agreement_weight("first_name")
        assert dob_weight > name_weight * 3

    def test_gender_low_weight(self):
        """Gender should have a relatively low weight (0.3× multiplier)."""
        fs = FellegiSunter()
        gender_weight = fs.compute_agreement_weight("gender")
        name_weight = fs.compute_agreement_weight("first_name")
        assert gender_weight < name_weight

    def test_agreement_weight_formula(self):
        """Verify the weight formula: log2(m/u) * multiplier."""
        fs = FellegiSunter()
        field = "first_name"
        p = fs.params[field]
        expected = math.log2(p["m"] / p["u"]) * p["weight_multiplier"]
        actual = fs.compute_agreement_weight(field)
        assert abs(actual - expected) < 1e-10

    def test_unknown_field_raises_error(self):
        fs = FellegiSunter()
        with pytest.raises(KeyError):
            fs.compute_agreement_weight("nonexistent_field")


# ---------------------------------------------------------------------------
# Field Weight Tests
# ---------------------------------------------------------------------------

class TestFieldWeight:
    """Tests for per-field weight computation with similarity scores."""

    def test_high_similarity_uses_agreement(self):
        """Similarity >= threshold should use agreement weight."""
        fs = FellegiSunter()
        weight = fs.compute_field_weight("first_name", 0.95)
        assert weight > 0

    def test_low_similarity_uses_disagreement(self):
        """Similarity < threshold should use disagreement weight."""
        fs = FellegiSunter()
        weight = fs.compute_field_weight("first_name", 0.5)
        assert weight < 0

    def test_perfect_similarity(self):
        """Similarity = 1.0 should give full agreement weight."""
        fs = FellegiSunter()
        weight = fs.compute_field_weight("ssn4", 1.0)
        expected = fs.compute_agreement_weight("ssn4") * 1.0
        assert abs(weight - expected) < 1e-10

    def test_zero_similarity(self):
        """Similarity = 0.0 should give full disagreement weight."""
        fs = FellegiSunter()
        weight = fs.compute_field_weight("ssn4", 0.0)
        assert weight < 0

    def test_threshold_boundary(self):
        """At exactly the threshold, should use agreement weight."""
        fs = FellegiSunter()
        weight_at = fs.compute_field_weight("first_name", 0.85)
        weight_below = fs.compute_field_weight("first_name", 0.84)
        assert weight_at > 0
        assert weight_below < 0


# ---------------------------------------------------------------------------
# Total Weight and Classification Tests
# ---------------------------------------------------------------------------

class TestTotalWeightAndClassification:
    """Tests for total weight computation and match classification."""

    def test_perfect_match_scores_high(self):
        """A record matching on all fields should score well above match threshold."""
        fs = FellegiSunter()
        scores = {
            "first_name": 1.0, "last_name": 1.0,
            "dob": 1.0, "ssn4": 1.0,
            "gender": 1.0, "phone": 1.0,
            "city": 1.0, "state": 1.0, "zip": 1.0,
        }
        total = fs.compute_total_weight(scores)
        assert total >= fs.match_threshold
        assert fs.classify(total) == "match"

    def test_complete_mismatch_scores_low(self):
        """A record mismatching on all fields should score below non-match threshold."""
        fs = FellegiSunter()
        scores = {
            "first_name": 0.0, "last_name": 0.0,
            "dob": 0.0, "ssn4": 0.0,
            "gender": 0.0, "phone": 0.0,
            "city": 0.0, "state": 0.0, "zip": 0.0,
        }
        total = fs.compute_total_weight(scores)
        assert total < fs.possible_match_threshold
        assert fs.classify(total) == "non_match"

    def test_partial_match_possible_match(self):
        """Match on SSN4 + DOB but mismatch on names — should be possible_match."""
        fs = FellegiSunter()
        scores = {
            "first_name": 0.3, "last_name": 0.3,  # Low name similarity
            "dob": 1.0, "ssn4": 1.0,               # Perfect on key fields
            "gender": 0.5, "phone": 0.0,
            "city": 0.0, "state": 0.0, "zip": 0.0,
        }
        total = fs.compute_total_weight(scores)
        classification = fs.classify(total)
        assert classification in ("match", "possible_match"), (
            f"SSN4+DOB match should yield at least possible_match, got {classification}"
        )

    def test_ssn4_dob_alone_strong_signal(self):
        """SSN4 + DOB agreement alone should contribute significant weight."""
        fs = FellegiSunter()
        ssn4_agree = fs.compute_agreement_weight("ssn4")
        dob_agree = fs.compute_agreement_weight("dob")
        combined = ssn4_agree + dob_agree
        assert combined > 40.0, (
            f"SSN4+DOB agreement ({combined:.1f}) should be very strong signal"
        )

    def test_classify_match(self):
        fs = FellegiSunter()
        assert fs.classify(15.0) == "match"

    def test_classify_possible_match(self):
        fs = FellegiSunter()
        assert fs.classify(9.0) == "possible_match"

    def test_classify_non_match(self):
        fs = FellegiSunter()
        assert fs.classify(3.0) == "non_match"

    def test_classify_at_match_threshold(self):
        fs = FellegiSunter()
        assert fs.classify(12.0) == "match"

    def test_classify_at_possible_threshold(self):
        fs = FellegiSunter()
        assert fs.classify(6.0) == "possible_match"

    def test_subset_of_fields(self):
        """Should work with only a subset of fields (missing fields ignored)."""
        fs = FellegiSunter()
        scores = {"dob": 1.0, "ssn4": 1.0}
        total = fs.compute_total_weight(scores)
        assert total > 0
