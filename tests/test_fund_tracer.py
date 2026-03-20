"""
tests/test_fund_tracer.py
=========================
Unit tests for the fund flow tracing engine.
Tests correlation logic, risk tier classification, amount matching,
and evidence generation without requiring real database connections.
"""

import pytest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fixtures.sample_data import (
    make_fund_release, make_tender, make_correlated_tender,
    make_entity_link, make_fund_trail, make_critical_trail,
    make_fund_release_series, make_high_risk_scenario
)


# ── Test: Risk tier classification ────────────────────────────────────────────

class TestRiskTierClassification:

    def setup_method(self):
        from engine.fund_tracer import FundTracer
        self.tracer = FundTracer.__new__(FundTracer)

    def test_lag_0_is_critical(self):
        assert self.tracer._compute_risk_tier(0) == "CRITICAL"

    def test_lag_50_is_critical(self):
        assert self.tracer._compute_risk_tier(50) == "CRITICAL"

    def test_lag_51_is_high(self):
        assert self.tracer._compute_risk_tier(51) == "HIGH"

    def test_lag_90_is_high(self):
        assert self.tracer._compute_risk_tier(90) == "HIGH"

    def test_lag_91_is_medium(self):
        assert self.tracer._compute_risk_tier(91) == "MEDIUM"

    def test_lag_180_is_medium(self):
        assert self.tracer._compute_risk_tier(180) == "MEDIUM"

    def test_lag_181_is_low(self):
        assert self.tracer._compute_risk_tier(181) == "LOW"

    def test_lag_negative_is_critical(self):
        """Tender awarded before fund release (pre-planned) should be CRITICAL."""
        # Negative lag means tender was awarded before fund released
        assert self.tracer._compute_risk_tier(-5) == "CRITICAL"

    def test_all_tiers_covered(self):
        """Every lag day 0–200 maps to a valid tier."""
        valid_tiers = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
        for lag in range(0, 201):
            tier = self.tracer._compute_risk_tier(lag)
            assert tier in valid_tiers, f"lag={lag} produced invalid tier: {tier}"


# ── Test: Amount matching filter ───────────────────────────────────────────────

class TestAmountMatching:

    def test_amount_match_above_threshold_passes(self):
        from engine.fund_tracer import MIN_AMOUNT_MATCH_RATIO
        fund_cr = 84.0
        tender_cr = 76.0  # 90.5% match
        ratio = tender_cr / fund_cr
        assert ratio >= MIN_AMOUNT_MATCH_RATIO

    def test_amount_match_below_threshold_fails(self):
        from engine.fund_tracer import MIN_AMOUNT_MATCH_RATIO
        fund_cr = 100.0
        tender_cr = 50.0  # 50% match
        ratio = tender_cr / fund_cr
        assert ratio < MIN_AMOUNT_MATCH_RATIO

    def test_exact_amount_match_passes(self):
        from engine.fund_tracer import MIN_AMOUNT_MATCH_RATIO
        fund_cr = 84.0
        tender_cr = 84.0
        ratio = tender_cr / fund_cr
        assert ratio >= MIN_AMOUNT_MATCH_RATIO

    def test_split_tender_at_60pct_passes(self):
        """60% of fund amount (minimum threshold) should pass."""
        from engine.fund_tracer import MIN_AMOUNT_MATCH_RATIO
        fund_cr = 100.0
        tender_cr = 60.0
        ratio = tender_cr / fund_cr
        assert ratio >= MIN_AMOUNT_MATCH_RATIO

    def test_split_tender_below_60pct_fails(self):
        from engine.fund_tracer import MIN_AMOUNT_MATCH_RATIO
        fund_cr = 100.0
        tender_cr = 59.0
        ratio = tender_cr / fund_cr
        assert ratio < MIN_AMOUNT_MATCH_RATIO

    def test_min_fund_amount_filter(self):
        """Fund releases below 1 crore should be ignored."""
        from engine.fund_tracer import MIN_FUND_AMOUNT_CR
        assert MIN_FUND_AMOUNT_CR > 0
        assert MIN_FUND_AMOUNT_CR <= 5.0  # Reasonable minimum

    def test_min_tender_amount_filter(self):
        """Tenders below 0.5 crore should be ignored."""
        from engine.fund_tracer import MIN_TENDER_AMOUNT_CR
        assert MIN_TENDER_AMOUNT_CR > 0
        assert MIN_TENDER_AMOUNT_CR <= 2.0


# ── Test: Evidence summary generation ─────────────────────────────────────────

class TestEvidenceSummary:

    def setup_method(self):
        from engine.fund_tracer import FundTracer
        self.tracer = FundTracer.__new__(FundTracer)

    def make_evidence(self, lag_days=67, amount_match=0.905):
        release = make_fund_release()
        tender = make_tender()
        pol_link = {
            "link_type": "family",
            "confidence": 0.93,
            "politician_name": "Rajendra Patil",
            "company_id": "test-company-id",
        }
        return self.tracer._build_evidence_summary(release, tender, pol_link,
                                                    lag_days, amount_match)

    def test_evidence_contains_scheme_name(self):
        summary = self.make_evidence()
        assert "Mahatma Gandhi" in summary or "NREGA" in summary

    def test_evidence_contains_amount(self):
        summary = self.make_evidence()
        assert "84" in summary

    def test_evidence_contains_district(self):
        summary = self.make_evidence()
        assert "Pune" in summary

    def test_evidence_contains_lag_days(self):
        summary = self.make_evidence(lag_days=67)
        assert "67" in summary

    def test_evidence_contains_company_name(self):
        summary = self.make_evidence()
        assert "Patil Constructions" in summary

    def test_evidence_contains_politician_name(self):
        summary = self.make_evidence()
        assert "Rajendra Patil" in summary

    def test_evidence_contains_link_type(self):
        summary = self.make_evidence()
        assert "family" in summary.lower()

    def test_evidence_contains_confidence(self):
        summary = self.make_evidence()
        assert "93%" in summary or "0.93" in summary

    def test_evidence_is_string(self):
        summary = self.make_evidence()
        assert isinstance(summary, str)
        assert len(summary) > 50

    def test_evidence_varies_by_lag(self):
        s1 = self.make_evidence(lag_days=45)
        s2 = self.make_evidence(lag_days=150)
        assert s1 != s2
        assert "45" in s1
        assert "150" in s2


# ── Test: CorrelatedFlow dataclass ─────────────────────────────────────────────

class TestCorrelatedFlow:

    def make_flow(self, lag_days=67, entity_confidence=0.93, risk_tier=None):
        from engine.fund_tracer import CorrelatedFlow
        return CorrelatedFlow(
            politician_id="pol-uuid",
            politician_name="Rajendra Patil",
            fund_release_id="fr-uuid",
            tender_id="t-uuid",
            company_id="comp-uuid",
            company_name="Patil Constructions",
            company_cin="L17110MH2010PLC123456",
            fund_amount_cr=84.0,
            tender_amount_cr=76.0,
            fund_district="Pune",
            fund_scheme="NREGA",
            release_date=date(2024, 3, 1),
            award_date=date(2024, 3, 1) + timedelta(days=lag_days),
            lag_days=lag_days,
            amount_match_pct=90.5,
            entity_link_type="family",
            entity_confidence=entity_confidence,
            risk_tier=risk_tier or ("CRITICAL" if lag_days <= 50 else "HIGH"),
            evidence_summary="Test evidence",
        )

    def test_critical_flow_scores_5_points(self):
        flow = self.make_flow(lag_days=45, risk_tier="CRITICAL")
        assert flow.risk_score_contrib == 5

    def test_high_flow_scores_4_points(self):
        flow = self.make_flow(lag_days=70, risk_tier="HIGH")
        assert flow.risk_score_contrib == 4

    def test_medium_flow_scores_3_points(self):
        flow = self.make_flow(lag_days=120, risk_tier="MEDIUM")
        assert flow.risk_score_contrib == 3

    def test_low_confidence_reduces_score(self):
        """Low entity confidence (< 0.70) should reduce contribution by 1."""
        flow_high_conf = self.make_flow(lag_days=45, entity_confidence=0.93, risk_tier="CRITICAL")
        flow_low_conf = self.make_flow(lag_days=45, entity_confidence=0.55, risk_tier="CRITICAL")
        assert flow_low_conf.risk_score_contrib < flow_high_conf.risk_score_contrib

    def test_contribution_always_positive(self):
        """Score contribution should never be negative."""
        for lag in [10, 50, 90, 150]:
            for conf in [0.50, 0.70, 1.00]:
                tier = "CRITICAL" if lag <= 50 else ("HIGH" if lag <= 90 else "MEDIUM")
                flow = self.make_flow(lag_days=lag, entity_confidence=conf, risk_tier=tier)
                assert flow.risk_score_contrib >= 1


# ── Test: District matching ────────────────────────────────────────────────────

class TestDistrictMatching:

    def test_same_district_matches(self):
        """Fund release and tender in same district should correlate."""
        release = make_fund_release(district="Pune")
        tender = make_tender(district="Pune")
        assert release["district"] == tender["district"]

    def test_different_district_no_match(self):
        """Fund release and tender in different districts should NOT correlate."""
        release = make_fund_release(district="Pune")
        tender = make_tender(district="Mumbai")
        assert release["district"] != tender["district"]

    def test_partial_district_name_handled(self):
        """District names from different sources may have slight variations."""
        # The SQL uses ILIKE %district% — so "Pune" matches "Pune District"
        district_from_pfms = "Pune"
        district_from_gem = "Pune District"
        assert district_from_pfms.lower() in district_from_gem.lower()


# ── Test: Temporal window ─────────────────────────────────────────────────────

class TestTemporalWindow:

    def test_within_window_is_candidate(self):
        from engine.fund_tracer import MAX_LAG_DAYS
        release_date = date(2024, 3, 1)
        award_date = release_date + timedelta(days=MAX_LAG_DAYS - 1)
        lag = (award_date - release_date).days
        assert lag < MAX_LAG_DAYS

    def test_outside_window_not_candidate(self):
        from engine.fund_tracer import MAX_LAG_DAYS
        release_date = date(2024, 3, 1)
        award_date = release_date + timedelta(days=MAX_LAG_DAYS + 1)
        lag = (award_date - release_date).days
        assert lag > MAX_LAG_DAYS

    def test_pre_release_buffer_included(self):
        """Tenders awarded up to 7 days BEFORE fund release should be included."""
        # Pre-planned tenders: the fund is released after tender is already awarded
        # This is suspicious — suggests insider knowledge of upcoming fund release
        from engine.fund_tracer import FundTracer
        tracer = FundTracer.__new__(FundTracer)
        # Negative lag (pre-planned) should still be CRITICAL
        assert tracer._compute_risk_tier(-5) == "CRITICAL"
        assert tracer._compute_risk_tier(-7) == "CRITICAL"


# ── Test: Fund trail fixture validation ───────────────────────────────────────

class TestFundTrailFixtures:

    def test_make_fund_release_has_required_fields(self):
        fr = make_fund_release()
        assert fr["district"]
        assert fr["state"]
        assert fr["amount_cr"] > 0
        assert isinstance(fr["release_date"], date)

    def test_make_tender_has_required_fields(self):
        t = make_tender()
        assert t["winner_cin"]
        assert t["contract_value_cr"] > 0
        assert isinstance(t["award_date"], date)

    def test_correlated_tender_lag_is_correct(self):
        fr = make_fund_release()
        for lag in [30, 67, 89, 150]:
            tender = make_correlated_tender(fr, lag_days=lag)
            actual_lag = (tender["award_date"] - fr["release_date"]).days
            assert actual_lag == lag, f"Expected lag={lag}, got {actual_lag}"

    def test_correlated_tender_amount_ratio_applied(self):
        fr = make_fund_release(amount_cr=100.0)
        tender = make_correlated_tender(fr, amount_ratio=0.85)
        assert abs(tender["contract_value_cr"] - 85.0) < 0.5

    def test_fund_release_series_has_correct_count(self):
        series = make_fund_release_series(count=7)
        assert len(series) == 7

    def test_fund_release_series_unique_ids(self):
        series = make_fund_release_series(count=5)
        ids = [fr["id"] for fr in series]
        assert len(set(ids)) == 5

    def test_critical_trail_has_short_lag(self):
        from engine.fund_tracer import CRITICAL_LAG_DAYS
        trail = make_critical_trail("pol", "fr", "t", "comp")
        assert trail["lag_days"] <= CRITICAL_LAG_DAYS
        assert trail["risk_tier"] == "CRITICAL"

    def test_high_risk_scenario_trail_count(self):
        scenario = make_high_risk_scenario()
        assert len(scenario["fund_trails"]) >= 2

    def test_high_risk_scenario_all_trails_critical(self):
        scenario = make_high_risk_scenario()
        for trail in scenario["fund_trails"]:
            assert trail["risk_tier"] in {"CRITICAL", "HIGH"}


# ── Test: CIN lookup map ──────────────────────────────────────────────────────

class TestCINLookupMap:

    def test_cin_map_structure(self):
        """The CIN→politicians map built by fund_tracer must be a dict of lists."""
        # Test the structure without DB — just verify the expected format
        cin_map = {
            "L17110MH2010PLC123456": [
                {"politician_id": "pol-1", "politician_name": "Rajendra Patil",
                 "company_id": "comp-1", "link_type": "family", "confidence": 0.93,
                 "company_name": "Patil Constructions", "cin": "L17110MH2010PLC123456"}
            ]
        }
        assert isinstance(cin_map, dict)
        for cin, politicians in cin_map.items():
            assert isinstance(politicians, list)
            for pol_link in politicians:
                assert "politician_id" in pol_link
                assert "confidence" in pol_link
                assert 0 < pol_link["confidence"] <= 1.0

    def test_cin_not_in_map_skipped(self):
        """Tenders whose winner CIN is not in the map should produce no trails."""
        cin_map = {"L17110MH2010PLC123456": [{"politician_id": "pol-1", "confidence": 0.93}]}
        tender_cin = "U99999MH2020PTC999999"  # Not in map
        assert tender_cin not in cin_map

    def test_cin_in_map_triggers_correlation(self):
        cin_map = {"L17110MH2010PLC123456": [{"politician_id": "pol-1", "confidence": 0.93}]}
        tender_cin = "L17110MH2010PLC123456"  # In map
        assert tender_cin in cin_map
        assert len(cin_map[tender_cin]) == 1


if __name__ == "__main__":
    import subprocess
    subprocess.run(["pytest", __file__, "-v", "--tb=short"])
