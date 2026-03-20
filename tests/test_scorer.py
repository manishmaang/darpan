"""
tests/test_scorer.py
====================
Unit tests for the risk scoring engine.
Run with: pytest tests/ -v
"""

import pytest
import json
from unittest.mock import MagicMock, patch, call
from datetime import date


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_db_conn():
    """Mock PostgreSQL connection that returns preset data."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.fixture
def sample_politician():
    return {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "name_normalized": "RAJENDRA PATIL",
        "pan": "ABCDE1234F",
        "party": "Test Party",
        "state": "Maharashtra",
        "constituency": "Mumbai North",
        "election_year": 2024,
        "position_held": "MLA",
    }


@pytest.fixture
def sample_assets_2_elections():
    """Politician with assets across 2 elections — significant growth."""
    return [
        {
            "election_year": 2019,
            "total_assets_lakh": 124.0,   # ₹1.24 Cr in 2019
            "declared_annual_income_lakh": 15.0,
        },
        {
            "election_year": 2024,
            "total_assets_lakh": 897.0,   # ₹8.97 Cr in 2024
            "declared_annual_income_lakh": 18.0,
        },
    ]


@pytest.fixture
def sample_assets_low_growth():
    """Politician with reasonable asset growth."""
    return [
        {"election_year": 2019, "total_assets_lakh": 50.0, "declared_annual_income_lakh": 12.0},
        {"election_year": 2024, "total_assets_lakh": 90.0, "declared_annual_income_lakh": 14.0},
    ]


# ── Asset Growth Tests ────────────────────────────────────────────────────────

class TestAssetGrowthScoring:

    def test_high_unexplained_growth_scores_maximum(self, sample_politician, sample_assets_2_elections):
        """
        623% unexplained asset growth (₹1.24Cr → ₹8.97Cr over 5 years,
        with only ₹75L provable income) should score close to maximum (25).
        """
        from engine.scorer import PoliticianScorer, ScoreComponents

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )

        with patch.object(scorer, '_load_assets', return_value=sample_assets_2_elections):
            scorer._score_asset_growth(components, sample_politician)

        # Should score high (≥20 out of 25) for 623% unexplained growth
        assert components.score_asset_growth >= 20
        assert components.score_asset_growth <= 25
        assert components.unexplained_growth_pct > 500
        assert "unexplained" in components.reasons.get("asset_growth", "").lower()

    def test_reasonable_growth_scores_zero(self, sample_politician, sample_assets_low_growth):
        """
        40L→90L over 5 years with ₹65L income = only 38% unexplained.
        Should score 0 — within normal range.
        """
        from engine.scorer import PoliticianScorer, ScoreComponents

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )

        with patch.object(scorer, '_load_assets', return_value=sample_assets_low_growth):
            scorer._score_asset_growth(components, sample_politician)

        assert components.score_asset_growth == 0

    def test_single_election_skips_scoring(self, sample_politician):
        """Only one election affidavit — cannot compute growth, score should be 0."""
        from engine.scorer import PoliticianScorer, ScoreComponents

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )
        single_asset = [{"election_year": 2024, "total_assets_lakh": 500.0,
                         "declared_annual_income_lakh": 15.0}]

        with patch.object(scorer, '_load_assets', return_value=single_asset):
            scorer._score_asset_growth(components, sample_politician)

        assert components.score_asset_growth == 0
        assert "Insufficient" in components.reasons.get("asset_growth", "")

    def test_zero_initial_assets_handled_gracefully(self, sample_politician):
        """Politician with zero initial assets — division by zero must not crash."""
        from engine.scorer import PoliticianScorer, ScoreComponents

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )
        zero_start = [
            {"election_year": 2019, "total_assets_lakh": 0.0, "declared_annual_income_lakh": 10.0},
            {"election_year": 2024, "total_assets_lakh": 500.0, "declared_annual_income_lakh": 12.0},
        ]

        with patch.object(scorer, '_load_assets', return_value=zero_start):
            scorer._score_asset_growth(components, sample_politician)  # Must not raise

        assert components.score_asset_growth == 0

    def test_score_capped_at_25(self, sample_politician):
        """Even extreme growth (10000%) must be capped at 25 points."""
        from engine.scorer import PoliticianScorer, ScoreComponents, MAX_SCORES

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )
        extreme = [
            {"election_year": 2014, "total_assets_lakh": 1.0, "declared_annual_income_lakh": 5.0},
            {"election_year": 2024, "total_assets_lakh": 10000.0, "declared_annual_income_lakh": 10.0},
        ]

        with patch.object(scorer, '_load_assets', return_value=extreme):
            scorer._score_asset_growth(components, sample_politician)

        assert components.score_asset_growth == MAX_SCORES["asset_growth"]


# ── Tender Linkage Tests ──────────────────────────────────────────────────────

class TestTenderLinkageScoring:

    def test_9_tenders_scores_25(self, sample_politician, mock_db_conn):
        """9 verified tenders × 4 points = 36, capped at 25."""
        from engine.scorer import PoliticianScorer, ScoreComponents

        conn, cursor = mock_db_conn
        cursor.fetchone.return_value = {"tender_count": 9, "total_value": 340.0}
        cursor.fetchall.return_value = [
            {"name": "Patil Constructions", "link_type": "family",
             "confidence": 0.93, "tender_count": 9, "tender_value": 340.0}
        ]

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        scorer.conn = conn
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )

        scorer._score_tender_linkage(components, sample_politician)

        assert components.score_tender_linkage == 25  # Capped at max
        assert components.linked_tender_count == 9
        assert components.linked_tender_value_cr == 340.0

    def test_zero_tenders_scores_zero(self, sample_politician, mock_db_conn):
        """No linked tenders = 0 score."""
        from engine.scorer import PoliticianScorer, ScoreComponents

        conn, cursor = mock_db_conn
        cursor.fetchone.return_value = {"tender_count": 0, "total_value": 0}

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        scorer.conn = conn
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )

        scorer._score_tender_linkage(components, sample_politician)

        assert components.score_tender_linkage == 0
        assert "No tenders" in components.reasons.get("tender_linkage", "")

    def test_2_tenders_scores_8(self, sample_politician, mock_db_conn):
        """2 tenders × 4 = 8 points."""
        from engine.scorer import PoliticianScorer, ScoreComponents

        conn, cursor = mock_db_conn
        cursor.fetchone.return_value = {"tender_count": 2, "total_value": 15.0}
        cursor.fetchall.return_value = [
            {"name": "Family Co", "link_type": "family",
             "confidence": 0.90, "tender_count": 2, "tender_value": 15.0}
        ]

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        scorer.conn = conn
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )

        scorer._score_tender_linkage(components, sample_politician)

        assert components.score_tender_linkage == 8


# ── Fund Flow Tests ───────────────────────────────────────────────────────────

class TestFundFlowScoring:

    def test_critical_trails_score_higher(self, sample_politician, mock_db_conn):
        """CRITICAL trails (short lag) should score more than MEDIUM trails."""
        from engine.scorer import PoliticianScorer, ScoreComponents

        conn, cursor = mock_db_conn

        # Scenario 1: 3 CRITICAL trails
        cursor.fetchall.return_value = [
            {"risk_tier": "CRITICAL", "count": 3, "contrib": 15}
        ]
        scorer = PoliticianScorer.__new__(PoliticianScorer)
        scorer.conn = conn
        components_critical = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )
        scorer._score_fund_flow(components_critical, sample_politician)

        # Scenario 2: 3 MEDIUM trails
        cursor.fetchall.return_value = [
            {"risk_tier": "MEDIUM", "count": 3, "contrib": 9}
        ]
        components_medium = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )
        scorer._score_fund_flow(components_medium, sample_politician)

        assert components_critical.score_fund_flow > components_medium.score_fund_flow

    def test_no_trails_scores_zero(self, sample_politician, mock_db_conn):
        """No fund trails = 0 score."""
        from engine.scorer import PoliticianScorer, ScoreComponents

        conn, cursor = mock_db_conn
        cursor.fetchall.return_value = []

        scorer = PoliticianScorer.__new__(PoliticianScorer)
        scorer.conn = conn
        components = ScoreComponents(
            politician_id=sample_politician["id"],
            politician_name=sample_politician["name_normalized"],
        )
        scorer._score_fund_flow(components, sample_politician)

        assert components.score_fund_flow == 0


# ── Total Score Tests ─────────────────────────────────────────────────────────

class TestTotalScoreAndClassification:

    def test_total_score_is_sum_of_components(self):
        """Total score must exactly equal sum of all 6 components."""
        from engine.scorer import ScoreComponents

        c = ScoreComponents(politician_id="test", politician_name="Test")
        c.score_asset_growth = 20
        c.score_tender_linkage = 25
        c.score_fund_flow = 15
        c.score_land_reg = 9
        c.score_rti_contradiction = 5
        c.score_network_depth = 3

        assert c.total_score == 77

    def test_critical_classification_at_75(self):
        """Score ≥75 should be CRITICAL."""
        from engine.scorer import ScoreComponents

        c = ScoreComponents(politician_id="test", politician_name="Test")
        c.score_asset_growth = 25
        c.score_tender_linkage = 25
        c.score_fund_flow = 20
        c.score_land_reg = 5
        c.score_rti_contradiction = 0
        c.score_network_depth = 0
        # Total = 75

        assert c.risk_classification == "CRITICAL"

    def test_high_risk_at_50(self):
        """Score between 50–74 should be HIGH."""
        from engine.scorer import ScoreComponents

        c = ScoreComponents(politician_id="test", politician_name="Test")
        c.score_asset_growth = 15
        c.score_tender_linkage = 20
        c.score_fund_flow = 10
        c.score_land_reg = 5
        c.score_rti_contradiction = 5
        c.score_network_depth = 2
        # Total = 57

        assert c.risk_classification == "HIGH"

    def test_low_risk_below_30(self):
        """Score <30 = LOW RISK."""
        from engine.scorer import ScoreComponents

        c = ScoreComponents(politician_id="test", politician_name="Test")
        c.score_asset_growth = 5
        c.score_tender_linkage = 4
        c.score_fund_flow = 0
        # Total = 9

        assert c.risk_classification == "LOW"


# ── Fund Tracer Tests ─────────────────────────────────────────────────────────

class TestFundTracer:

    def test_risk_tier_critical_at_50_days(self):
        """Lag ≤50 days should be CRITICAL."""
        from engine.fund_tracer import FundTracer

        tracer = FundTracer.__new__(FundTracer)
        assert tracer._compute_risk_tier(45) == "CRITICAL"
        assert tracer._compute_risk_tier(50) == "CRITICAL"

    def test_risk_tier_high_between_51_and_90(self):
        """Lag 51–90 days should be HIGH."""
        from engine.fund_tracer import FundTracer

        tracer = FundTracer.__new__(FundTracer)
        assert tracer._compute_risk_tier(51) == "HIGH"
        assert tracer._compute_risk_tier(90) == "HIGH"

    def test_risk_tier_medium_between_91_and_180(self):
        """Lag 91–180 days should be MEDIUM."""
        from engine.fund_tracer import FundTracer

        tracer = FundTracer.__new__(FundTracer)
        assert tracer._compute_risk_tier(91) == "MEDIUM"
        assert tracer._compute_risk_tier(180) == "MEDIUM"

    def test_amount_match_filter_rejects_low_ratio(self):
        """
        Tender of ₹50Cr against fund release of ₹100Cr = 50% ratio.
        MIN_AMOUNT_MATCH_RATIO is 0.60, so this should be filtered out.
        """
        from engine.fund_tracer import MIN_AMOUNT_MATCH_RATIO

        fund_amount = 100.0
        tender_amount = 50.0
        ratio = tender_amount / fund_amount

        assert ratio < MIN_AMOUNT_MATCH_RATIO

    def test_evidence_summary_contains_key_facts(self):
        """Evidence summary must contain all key identifying information."""
        from engine.fund_tracer import FundTracer

        tracer = FundTracer.__new__(FundTracer)

        release = {
            "scheme_name": "NREGA",
            "amount_cr": 84.0,
            "district": "Pune",
            "state": "Maharashtra",
            "release_date": date(2024, 3, 1),
        }
        tender = {
            "winner_name": "Patil Constructions Pvt Ltd",
            "winner_cin": "L12345MH2010PLC123456",
            "contract_value_cr": 76.0,
            "department": "PWD Maharashtra",
        }
        pol_link = {
            "link_type": "family",
            "confidence": 0.93,
            "politician_name": "Rajendra Patil",
        }

        summary = tracer._build_evidence_summary(release, tender, pol_link, 67, 0.90)

        assert "NREGA" in summary
        assert "84" in summary
        assert "Pune" in summary
        assert "67" in summary
        assert "Patil Constructions" in summary
        assert "Rajendra Patil" in summary
        assert "family" in summary.lower()
        assert "90%" in summary or "0.90" in summary or "90" in summary


# ── Base Scraper Tests ────────────────────────────────────────────────────────

class TestBaseScraper:

    def test_normalize_name_strips_corporate_suffix(self):
        """Corporate suffixes like 'PVT LTD' should be removed for comparison."""
        from scrapers.base_scraper import BaseScraper

        assert BaseScraper.normalize_name("Patil Constructions Pvt Ltd") == "PATIL CONSTRUCTIONS"
        assert BaseScraper.normalize_name("Sharma Infra Limited") == "SHARMA INFRA"
        assert BaseScraper.normalize_name("Tech Solutions LLP") == "TECH SOLUTIONS"

    def test_normalize_name_uppercases(self):
        from scrapers.base_scraper import BaseScraper

        assert BaseScraper.normalize_name("rajendra patil") == "RAJENDRA PATIL"

    def test_extract_pan_valid(self):
        from scrapers.base_scraper import BaseScraper

        text = "My PAN number is ABCDE1234F and I declare..."
        assert BaseScraper.extract_pan(text) == "ABCDE1234F"

    def test_extract_pan_invalid_returns_none(self):
        from scrapers.base_scraper import BaseScraper

        assert BaseScraper.extract_pan("No PAN here") is None
        assert BaseScraper.extract_pan("ABCD12345") is None  # Wrong format

    def test_parse_amount_cr_handles_crore_text(self):
        from scrapers.base_scraper import BaseScraper

        assert BaseScraper.parse_amount_cr("Rs. 45.5 Crores") == 45.5
        assert BaseScraper.parse_amount_cr("₹12.3 Cr") == 12.3

    def test_parse_amount_cr_converts_lakhs(self):
        from scrapers.base_scraper import BaseScraper

        result = BaseScraper.parse_amount_cr("Rs. 250 Lakhs")
        assert abs(result - 2.5) < 0.01  # 250 lakhs = 2.5 crores

    def test_extract_cin_valid(self):
        from scrapers.base_scraper import BaseScraper

        text = "Company CIN: L17110MH1973PLC019786 is registered"
        assert BaseScraper.extract_cin(text) == "L17110MH1973PLC019786"

    def test_url_fingerprint_is_consistent(self):
        from scrapers.base_scraper import BaseScraper

        scraper = BaseScraper.__new__(BaseScraper)
        url = "https://example.com/affidavit/12345"
        assert scraper.url_fingerprint(url) == scraper.url_fingerprint(url)

    def test_url_fingerprint_different_for_different_urls(self):
        from scrapers.base_scraper import BaseScraper

        scraper = BaseScraper.__new__(BaseScraper)
        fp1 = scraper.url_fingerprint("https://example.com/page1")
        fp2 = scraper.url_fingerprint("https://example.com/page2")
        assert fp1 != fp2


# ── Entity Graph Tests ────────────────────────────────────────────────────────

class TestEntityGraph:

    def test_confidence_pan_exact_is_1(self):
        """PAN exact match should have maximum confidence of 1.0."""
        from engine.entity_graph import CONFIDENCE_WEIGHTS

        assert CONFIDENCE_WEIGHTS["pan_exact"] == 1.0

    def test_confidence_fuzzy_lower_than_direct(self):
        """Fuzzy name matches should have lower confidence than PAN matches."""
        from engine.entity_graph import CONFIDENCE_WEIGHTS

        assert CONFIDENCE_WEIGHTS["name_fuzzy_high"] < CONFIDENCE_WEIGHTS["pan_exact"]
        assert CONFIDENCE_WEIGHTS["name_fuzzy_med"] < CONFIDENCE_WEIGHTS["name_fuzzy_high"]

    def test_fuzzy_threshold_configured(self):
        """Fuzzy match threshold must be set between 70 and 95."""
        from engine.entity_graph import FUZZY_MATCH_THRESHOLD

        assert 70 <= FUZZY_MATCH_THRESHOLD <= 95


# ── Run tests ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import subprocess
    subprocess.run(["pytest", __file__, "-v", "--tb=short"])
