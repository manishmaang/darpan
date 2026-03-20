"""
api/models.py
=============
Pydantic data models for the VIGILANT.IN API.
"""

from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime


class PoliticianSummary(BaseModel):
    id: str
    name: str
    pan: Optional[str] = None
    party: Optional[str] = None
    state: str
    constituency: str
    election_year: Optional[int] = None
    position_held: Optional[str] = None
    total_score: int = 0
    risk_classification: Optional[str] = None
    score_asset_growth: Optional[int] = 0
    score_tender_linkage: Optional[int] = 0
    score_fund_flow: Optional[int] = 0
    score_land_reg: Optional[int] = 0
    score_rti_contradiction: Optional[int] = 0
    score_network_depth: Optional[int] = 0
    scored_at: Optional[datetime] = None
    latest_assets_lakh: Optional[float] = None
    linked_companies: Optional[int] = 0
    fund_trail_count: Optional[int] = 0

    model_config = {"from_attributes": True}


class AssetYear(BaseModel):
    election_year: int
    total_assets_lakh: Optional[float] = None
    declared_annual_income_lakh: Optional[float] = None
    residential_property_lakh: Optional[float] = None
    agricultural_land_lakh: Optional[float] = None
    cash_in_hand_lakh: Optional[float] = None


class FamilyMember(BaseModel):
    name_normalized: str
    relation: str
    pan: Optional[str] = None


class LinkedCompany(BaseModel):
    cin: str
    name: str
    status: Optional[str] = None
    state_of_reg: Optional[str] = None
    link_type: str
    confidence: float
    relation_via: Optional[str] = None


class RiskScore(BaseModel):
    total_score: int
    risk_classification: str
    score_asset_growth: int
    score_tender_linkage: int
    score_fund_flow: int
    score_land_reg: int
    score_rti_contradiction: int
    score_network_depth: int
    score_reasons: Optional[dict] = None
    raw_metrics: Optional[dict] = None
    scored_at: Optional[datetime] = None


class PoliticianDetail(BaseModel):
    id: str
    name_normalized: str
    pan: Optional[str] = None
    party: Optional[str] = None
    state: str
    constituency: str
    election_year: Optional[int] = None
    position_held: Optional[str] = None
    assets_history: list[AssetYear] = []
    family_members: list[FamilyMember] = []
    linked_companies: list[LinkedCompany] = []
    risk_score: Optional[RiskScore] = None

    model_config = {"from_attributes": True}


class ScoreBreakdown(BaseModel):
    politician_id: str
    politician_name: str
    state: Optional[str] = None
    constituency: Optional[str] = None
    party: Optional[str] = None
    score_asset_growth: int
    score_tender_linkage: int
    score_fund_flow: int
    score_land_reg: int
    score_rti_contradiction: int
    score_network_depth: int
    risk_classification: str
    score_reasons: Optional[dict] = None
    raw_metrics: Optional[dict] = None
    scored_at: Optional[datetime] = None


class FundTrail(BaseModel):
    risk_tier: str
    lag_days: int
    amount_match_pct: Optional[float] = None
    evidence_summary: Optional[str] = None
    scheme_name: str
    fund_amount: float
    release_date: date
    fund_district: str
    winner_name: str
    contract_value_cr: float
    award_date: date
    department: Optional[str] = None
    company_name: str
    cin: Optional[str] = None


class EntityGraphData(BaseModel):
    nodes: list[dict]
    edges: list[dict]


class PlatformStats(BaseModel):
    total_politicians: int
    critical_suspects: int
    high_risk: int
    total_fund_trails: int
    flagged_tender_value_cr: float
    states_covered: int
    last_updated: Optional[str] = None


class SearchResult(BaseModel):
    id: str
    name: str
    party: Optional[str] = None
    state: str
    constituency: str
    score: int
    risk_classification: Optional[str] = None
