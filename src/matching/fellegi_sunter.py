"""
Fellegi-Sunter probabilistic record linkage model for clinical document matching.

Implements the classic Fellegi-Sunter framework with log-likelihood ratio weights
tuned for healthcare member matching where SSN4 and DOB carry the highest
discriminatory power.

References
----------
Fellegi, I.P. and Sunter, A.B. (1969). "A Theory for Record Linkage."
Journal of the American Statistical Association, 64(328), pp. 1183-1210.
"""

import math
from copy import deepcopy


class FellegiSunter:
    """
    Fellegi-Sunter probabilistic record linkage scoring.

    Computes log-likelihood ratio weights for each comparison field
    based on m-probabilities (P(agree|match)) and u-probabilities
    (P(agree|non-match)).

    SSN4 and DOB receive the highest absolute weights (3-5x name weights)
    per clinical matching requirements -- this is enforced via the
    ``weight_multiplier`` parameter for each field.

    Parameters
    ----------
    params : dict or None
        Dictionary of field parameters.  Each key is a field name and the
        value is a dict with keys ``'m'``, ``'u'``, and ``'weight_multiplier'``.
        If ``None``, ``DEFAULT_PARAMS`` is used.
    match_threshold : float
        Total composite weight at or above which a pair is classified as
        a definite ``'match'``.  Default ``12.0``.
    possible_match_threshold : float
        Total composite weight at or above which a pair is classified as a
        ``'possible_match'`` (but below ``match_threshold``).  Default ``6.0``.
    """

    # ------------------------------------------------------------------
    # Default parameters tuned for healthcare member matching
    # ------------------------------------------------------------------
    DEFAULT_PARAMS = {
        "first_name":  {"m": 0.90, "u": 0.01,   "weight_multiplier": 1.0},
        "last_name":   {"m": 0.92, "u": 0.008,  "weight_multiplier": 1.0},
        "dob":         {"m": 0.95, "u": 0.0001, "weight_multiplier": 4.0},
        "ssn4":        {"m": 0.98, "u": 0.001,  "weight_multiplier": 5.0},
        "gender":      {"m": 0.98, "u": 0.50,   "weight_multiplier": 0.3},
        "phone":       {"m": 0.85, "u": 0.0001, "weight_multiplier": 1.5},
        "city":        {"m": 0.88, "u": 0.02,   "weight_multiplier": 0.5},
        "state":       {"m": 0.95, "u": 0.05,   "weight_multiplier": 0.3},
        "zip":         {"m": 0.90, "u": 0.01,   "weight_multiplier": 0.8},
    }

    def __init__(self, params=None, match_threshold=12.0, possible_match_threshold=6.0):
        self.params = deepcopy(params) if params is not None else deepcopy(self.DEFAULT_PARAMS)
        self.match_threshold = match_threshold
        self.possible_match_threshold = possible_match_threshold

    # ------------------------------------------------------------------
    # Weight computation
    # ------------------------------------------------------------------

    def compute_agreement_weight(self, field: str) -> float:
        """
        Compute the agreement weight for a field.

        The agreement weight is the log-likelihood ratio when the field
        values agree, scaled by the field's weight multiplier::

            w_agree = log2(m / u) * weight_multiplier

        Parameters
        ----------
        field : str
            Field name (must be a key in ``self.params``).

        Returns
        -------
        float
            Agreement weight.  Always positive for valid m > u.

        Raises
        ------
        KeyError
            If *field* is not in the parameter dictionary.
        """
        p = self.params[field]
        m = p["m"]
        u = p["u"]
        multiplier = p.get("weight_multiplier", 1.0)

        # Guard against log(0)
        if u == 0:
            u = 1e-10
        if m == 0:
            return -float("inf") * multiplier

        return math.log2(m / u) * multiplier

    def compute_disagreement_weight(self, field: str) -> float:
        """
        Compute the disagreement weight for a field.

        The disagreement weight is the log-likelihood ratio when the field
        values disagree, scaled by the field's weight multiplier::

            w_disagree = log2((1 - m) / (1 - u)) * weight_multiplier

        This value is typically negative, penalising the composite score
        when important fields do not match.

        Parameters
        ----------
        field : str
            Field name (must be a key in ``self.params``).

        Returns
        -------
        float
            Disagreement weight.  Typically negative.
        """
        p = self.params[field]
        m = p["m"]
        u = p["u"]
        multiplier = p.get("weight_multiplier", 1.0)

        one_minus_m = 1.0 - m
        one_minus_u = 1.0 - u

        # Guard against log(0)
        if one_minus_u == 0:
            one_minus_u = 1e-10
        if one_minus_m == 0:
            one_minus_m = 1e-10

        return math.log2(one_minus_m / one_minus_u) * multiplier

    def compute_field_weight(self, field: str, similarity_score: float, threshold: float = 0.85) -> float:
        """
        Compute the weight contribution for a single field comparison.

        If the similarity score meets or exceeds the *threshold* the field
        is treated as an agreement; otherwise it is treated as a
        disagreement.  The returned weight is scaled by how close the
        similarity is to 1.0 (agreement) or 0.0 (disagreement) to allow
        partial-match credit.

        Parameters
        ----------
        field : str
            Field name.
        similarity_score : float
            Similarity score for this field, in ``[0.0, 1.0]``.
        threshold : float
            Agreement/disagreement cut-off.  Default ``0.85``.

        Returns
        -------
        float
            Weighted contribution of this field to the composite score.
        """
        if similarity_score is None:
            similarity_score = 0.0

        if similarity_score >= threshold:
            # Agreement: scale by similarity to give partial credit
            return self.compute_agreement_weight(field) * similarity_score
        else:
            # Disagreement: scale by (1 - similarity) so near-misses are
            # penalised less than total mismatches
            return self.compute_disagreement_weight(field) * (1.0 - similarity_score)

    # ------------------------------------------------------------------
    # Composite scoring
    # ------------------------------------------------------------------

    def compute_total_weight(self, field_scores: dict) -> float:
        """
        Compute the composite Fellegi-Sunter weight across all fields.

        Parameters
        ----------
        field_scores : dict
            Mapping of field name to similarity score, e.g.
            ``{'first_name': 0.95, 'dob': 1.0, 'ssn4': 1.0}``.
            Fields not present in ``self.params`` are silently ignored.

        Returns
        -------
        float
            Sum of individual field weights.
        """
        total = 0.0
        for field, score in field_scores.items():
            if field in self.params:
                total += self.compute_field_weight(field, score)
        return total

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def classify(self, total_weight: float) -> str:
        """
        Classify a candidate pair based on its composite weight.

        Parameters
        ----------
        total_weight : float
            Composite weight from :meth:`compute_total_weight`.

        Returns
        -------
        str
            One of ``'match'``, ``'possible_match'``, or ``'non_match'``.
        """
        if total_weight >= self.match_threshold:
            return "match"
        elif total_weight >= self.possible_match_threshold:
            return "possible_match"
        else:
            return "non_match"

    # ------------------------------------------------------------------
    # Parameter estimation from labelled data
    # ------------------------------------------------------------------

    def estimate_parameters(self, labeled_pairs_df, spark):
        """
        Estimate m and u probabilities from a labelled training dataset.

        This is a simple frequency-based estimator: for each field, the
        average similarity score among true matches gives *m*, and the
        average among non-matches gives *u*.  Weight multipliers are left
        unchanged.

        Parameters
        ----------
        labeled_pairs_df : pyspark.sql.DataFrame
            Must contain columns:

            * ``field_name`` (str) -- name of the comparison field
            * ``similarity_score`` (float) -- similarity value in [0, 1]
            * ``is_true_match`` (bool) -- whether the pair is a true match

        spark : pyspark.sql.SparkSession
            Active Spark session (used for internal queries).

        Returns
        -------
        None
            Updates ``self.params`` in place.
        """
        from pyspark.sql import functions as F

        # Compute average similarity grouped by (field, match label)
        stats = (
            labeled_pairs_df
            .groupBy("field_name", "is_true_match")
            .agg(F.avg("similarity_score").alias("avg_sim"))
            .collect()
        )

        # Organise into a lookup: {field_name: {True: avg_sim, False: avg_sim}}
        lookup = {}
        for row in stats:
            field = row["field_name"]
            is_match = row["is_true_match"]
            avg_sim = row["avg_sim"]
            lookup.setdefault(field, {})[is_match] = avg_sim

        # Update params -- only for fields that exist in both the training
        # data and the current parameter set
        for field, match_dict in lookup.items():
            if field not in self.params:
                continue

            if True in match_dict:
                # Clamp to avoid degenerate log values
                m_est = min(max(match_dict[True], 1e-6), 1.0 - 1e-6)
                self.params[field]["m"] = m_est

            if False in match_dict:
                u_est = min(max(match_dict[False], 1e-6), 1.0 - 1e-6)
                self.params[field]["u"] = u_est

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def get_parameters_as_df(self, spark):
        """
        Return the current model parameters as a Spark DataFrame.

        Useful for persisting to a Delta table so the model can be
        reloaded or audited later.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession
            Active Spark session.

        Returns
        -------
        pyspark.sql.DataFrame
            Columns: ``field_name``, ``m_prob``, ``u_prob``,
            ``weight_multiplier``, ``agreement_weight``,
            ``disagreement_weight``.
        """
        rows = []
        for field, p in self.params.items():
            rows.append({
                "field_name": field,
                "m_prob": float(p["m"]),
                "u_prob": float(p["u"]),
                "weight_multiplier": float(p.get("weight_multiplier", 1.0)),
                "agreement_weight": float(self.compute_agreement_weight(field)),
                "disagreement_weight": float(self.compute_disagreement_weight(field)),
            })

        return spark.createDataFrame(rows)

    # ------------------------------------------------------------------
    # Readable summary
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        lines = [
            f"FellegiSunter(match_threshold={self.match_threshold}, "
            f"possible_match_threshold={self.possible_match_threshold})",
            f"  {'Field':<15} {'m':>6} {'u':>8} {'mult':>5} {'w_agree':>9} {'w_disagree':>11}",
            f"  {'-'*55}",
        ]
        for field in sorted(self.params):
            p = self.params[field]
            w_a = self.compute_agreement_weight(field)
            w_d = self.compute_disagreement_weight(field)
            lines.append(
                f"  {field:<15} {p['m']:>6.3f} {p['u']:>8.5f} "
                f"{p.get('weight_multiplier', 1.0):>5.1f} {w_a:>9.3f} {w_d:>11.3f}"
            )
        return "\n".join(lines)
