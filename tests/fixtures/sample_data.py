"""
tests/fixtures/sample_data.py
==============================
Test data factories for VIGILANT.IN unit tests.
Provides realistic sample records without hitting real databases.
"""

import uuid
from datetime import date, datetime


# ── Politician fixtures ────────────────────────────────────────────────────────

def make_politician(**overrides) -> dict:
    """Factory for a politician record."""
    defaults = {
        "id": str(uuid.uuid4()),
        "ec_affidavit_id": "MH-MLA-2024-TEST-001",
        "name_raw": "Rajendra Bhimrao Patil",
        "name_normalized": "RAJENDRA PATIL",
        "pan": "ABCDE1234F",
        "party": "Test Party",
        "state": "Maharashtra",
        "constituency": "Pune South",
        "election_year": 2024,
        "election_type": "VS",
        "won_election": True,
        "position_held": "MLA",
    }
    defaults.update(overrides)
    return defaults


def make_politician_high_risk(**overrides) -> dict:
    return make_politician(
        name_normalized="RAJENDRA PATIL",
        pan="ABCDE1234F",
        state="Maharashtra",
        **overrides,
    )


def make_politician_low_risk(**overrides) -> dict:
    return make_politician(
        name_normalized="HARSHAD PATEL",
        pan="FGHIJ5678K",
        state="Gujarat",
        **overrides,
    )


# ── Asset fixtures ─────────────────────────────────────────────────────────────

def make_assets_growing(politician_id: str, start_lakh: float = 124.0,
                         end_lakh: float = 897.0) -> list[dict]:
    """Significant unexplained asset growth across two elections."""
    return [
        {
            "politician_id": politician_id,
            "election_year": 2019,
            "total_assets_lakh": start_lakh,
            "declared_annual_income_lakh": 15.0,
            "residential_property_lakh": 40.0,
            "cash_in_hand_lakh": 2.5,
            "bank_deposits_lakh": 30.0,
        },
        {
            "politician_id": politician_id,
            "election_year": 2024,
            "total_assets_lakh": end_lakh,
            "declared_annual_income_lakh": 18.0,
            "residential_property_lakh": 320.0,
            "cash_in_hand_lakh": 8.0,
            "bank_deposits_lakh": 150.0,
        },
    ]


def make_assets_stable(politician_id: str) -> list[dict]:
    """Reasonable asset growth consistent with declared income."""
    return [
        {
            "politician_id": politician_id,
            "election_year": 2019,
            "total_assets_lakh": 50.0,
            "declared_annual_income_lakh": 12.0,
        },
        {
            "politician_id": politician_id,
            "election_year": 2024,
            "total_assets_lakh": 90.0,
            "declared_annual_income_lakh": 14.0,
        },
    ]


# ── Company fixtures ───────────────────────────────────────────────────────────

def make_company(**overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "cin": "L17110MH2010PLC123456",
        "name": "Patil Constructions Private Limited",
        "name_normalized": "PATIL CONSTRUCTIONS",
        "company_type": "Private",
        "status": "Active",
        "registration_date": date(2010, 5, 15),
        "state_of_reg": "Maharashtra",
        "authorized_capital": 50.0,  # crores
        "paid_up_capital": 10.0,
    }
    defaults.update(overrides)
    return defaults


def make_company_shell(**overrides) -> dict:
    """Shell company with minimal capital and suspicious registration."""
    return make_company(
        cin="U74999MH2020PTC345678",
        name="Deccan Agri Holdings Private Limited",
        name_normalized="DECCAN AGRI HOLDINGS",
        authorized_capital=1.0,
        paid_up_capital=0.1,
        registration_date=date(2020, 1, 10),
        **overrides,
    )


def make_director(company_id: str, **overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "company_id": company_id,
        "din": "12345678",
        "pan": "MNOPQ9012R",
        "name_raw": "Meena Rajendra Patil",
        "name_normalized": "MEENA PATIL",
        "role": "director",
        "appointed_date": date(2010, 5, 15),
        "is_active": True,
    }
    defaults.update(overrides)
    return defaults


# ── Fund release fixtures ──────────────────────────────────────────────────────

def make_fund_release(**overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "pfms_ref_id": "PFMS-MH-2024-001234",
        "scheme_code": "MGNREGS",
        "scheme_name": "Mahatma Gandhi National Rural Employment Guarantee Scheme",
        "scheme_category": "NREGA",
        "state": "Maharashtra",
        "district": "Pune",
        "implementing_agency": "District Rural Development Agency - Pune",
        "amount_cr": 84.0,
        "release_date": date(2024, 3, 1),
        "financial_year": "2023-24",
    }
    defaults.update(overrides)
    return defaults


def make_fund_release_series(district: str = "Pune", count: int = 5) -> list[dict]:
    """Create a series of fund releases in the same district."""
    from datetime import timedelta
    base_date = date(2024, 1, 1)
    return [
        make_fund_release(
            id=str(uuid.uuid4()),
            pfms_ref_id=f"PFMS-TEST-{i:04d}",
            district=district,
            amount_cr=round(20.0 + i * 15, 1),
            release_date=base_date + timedelta(days=i * 30),
        )
        for i in range(count)
    ]


# ── Tender fixtures ────────────────────────────────────────────────────────────

def make_tender(**overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "tender_ref_id": "GEM-2024-B-XXXX001",
        "source_portal": "gem",
        "department": "Public Works Department Maharashtra",
        "category": "Construction",
        "state": "Maharashtra",
        "district": "Pune",
        "award_date": date(2024, 5, 7),  # 67 days after default fund release
        "contract_value_cr": 76.0,
        "winner_name": "Patil Constructions Private Limited",
        "winner_cin": "L17110MH2010PLC123456",
        "completion_status": "Ongoing",
    }
    defaults.update(overrides)
    return defaults


def make_correlated_tender(fund_release: dict, lag_days: int = 67,
                            amount_ratio: float = 0.90) -> dict:
    """Create a tender that correlates with a specific fund release."""
    from datetime import timedelta
    award_date = fund_release["release_date"] + timedelta(days=lag_days)
    contract_value = round(fund_release["amount_cr"] * amount_ratio, 1)
    return make_tender(
        id=str(uuid.uuid4()),
        tender_ref_id=f"GEM-CORRELATED-{lag_days}D",
        district=fund_release["district"],
        state=fund_release["state"],
        award_date=award_date,
        contract_value_cr=contract_value,
    )


# ── Entity link fixtures ───────────────────────────────────────────────────────

def make_entity_link(politician_id: str, company_id: str, **overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "politician_id": politician_id,
        "company_id": company_id,
        "link_type": "family",
        "relation_via": "spouse",
        "confidence": 0.93,
        "evidence_sources": ["pan_exact_match"],
        "graph_depth": 1,
    }
    defaults.update(overrides)
    return defaults


def make_entity_link_direct(politician_id: str, company_id: str) -> dict:
    return make_entity_link(
        politician_id, company_id,
        link_type="direct",
        relation_via=None,
        confidence=1.0,
        evidence_sources=["pan_exact_match"],
    )


def make_entity_link_fuzzy(politician_id: str, company_id: str,
                            score: int = 82) -> dict:
    return make_entity_link(
        politician_id, company_id,
        link_type="associate",
        relation_via="possible_associate",
        confidence=score / 100,
        evidence_sources=[f"fuzzy_name_{score}pct"],
        graph_depth=2,
    )


# ── Fund trail fixtures ────────────────────────────────────────────────────────

def make_fund_trail(politician_id: str, fund_id: str, tender_id: str,
                    company_id: str, **overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "politician_id": politician_id,
        "fund_release_id": fund_id,
        "tender_id": tender_id,
        "company_id": company_id,
        "lag_days": 67,
        "amount_match_pct": 90.5,
        "district_match": True,
        "risk_tier": "HIGH",
        "risk_score_contrib": 4,
        "evidence_summary": "Test fund trail — PFMS to GeM in 67 days.",
    }
    defaults.update(overrides)
    return defaults


def make_critical_trail(politician_id: str, fund_id: str, tender_id: str,
                         company_id: str) -> dict:
    return make_fund_trail(
        politician_id, fund_id, tender_id, company_id,
        lag_days=45,
        risk_tier="CRITICAL",
        risk_score_contrib=5,
        evidence_summary="CRITICAL: Fund released, tender awarded to linked company 45 days later.",
    )


# ── RTI flag fixtures ──────────────────────────────────────────────────────────

def make_rti_flag(**overrides) -> dict:
    defaults = {
        "id": str(uuid.uuid4()),
        "rti_application_no": "RTI-MH-2024-001",
        "public_authority": "District Collector Pune",
        "subject": "Details of contractor for road construction work",
        "contractor_name": "Patil Constructions Private Limited",
        "fund_amount_cr": 84.0,
        "contradiction_type": "hidden_contractor",
        "contradiction_detail": "RTI authority withheld contractor list for 2 years.",
        "source_url": "https://rtionline.gov.in/download/RTI-MH-2024-001.pdf",
    }
    defaults.update(overrides)
    return defaults


# ── Complete scenario builders ─────────────────────────────────────────────────

def make_high_risk_scenario() -> dict:
    """
    Complete test scenario: politician with multiple fund trails and high score.
    Returns all related records as a dict for use in integration tests.
    """
    pol = make_politician_high_risk()
    company = make_company()
    company_shell = make_company_shell()
    director = make_director(company["id"])
    fund1 = make_fund_release()
    fund2 = make_fund_release(
        id=str(uuid.uuid4()),
        pfms_ref_id="PFMS-MH-2024-001235",
        scheme_name="Smart City Mission",
        amount_cr=47.0,
        release_date=date(2024, 1, 15),
    )
    tender1 = make_correlated_tender(fund1, lag_days=67)
    tender2 = make_correlated_tender(fund2, lag_days=43, amount_ratio=0.85)
    link = make_entity_link_direct(pol["id"], company["id"])
    trail1 = make_critical_trail(pol["id"], fund1["id"], tender1["id"], company["id"])
    trail2 = make_fund_trail(pol["id"], fund2["id"], tender2["id"], company["id"],
                              lag_days=43, risk_tier="CRITICAL", risk_score_contrib=5)
    rti = make_rti_flag()
    assets = make_assets_growing(pol["id"])

    return {
        "politician": pol,
        "companies": [company, company_shell],
        "directors": [director],
        "fund_releases": [fund1, fund2],
        "tenders": [tender1, tender2],
        "entity_links": [link],
        "fund_trails": [trail1, trail2],
        "rti_flags": [rti],
        "assets": assets,
        # Expected score ranges
        "expected_score_min": 70,
        "expected_classification": "CRITICAL",
    }


def make_low_risk_scenario() -> dict:
    """Complete test scenario for a low-risk politician."""
    pol = make_politician_low_risk()
    assets = make_assets_stable(pol["id"])
    return {
        "politician": pol,
        "companies": [],
        "fund_releases": [],
        "tenders": [],
        "entity_links": [],
        "fund_trails": [],
        "rti_flags": [],
        "assets": assets,
        "expected_score_max": 30,
        "expected_classification": "LOW",
    }
