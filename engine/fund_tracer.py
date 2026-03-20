"""
engine/fund_tracer.py
=====================
Temporal correlation engine that traces public fund flows to
politician-linked companies.

Core algorithm:
    For each fund release F (from PFMS):
        For each tender T in same district within 90 days:
            If T's winner is in any politician's entity graph:
                Create a fund_trail record with risk tier

This is the mechanistic proof of the corruption cycle:
    Government funds → District → Tender → Linked Company → Politician
"""

import os
import logging
from datetime import date, timedelta
from typing import Optional
from dataclasses import dataclass, field

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Correlation parameters ─────────────────────────────────────────────────────

# Maximum days between fund release and tender award to consider correlated
MAX_LAG_DAYS = 180

# Strong correlation window — within this = highly suspicious
CRITICAL_LAG_DAYS = 50
HIGH_LAG_DAYS = 90
MEDIUM_LAG_DAYS = 180

# Minimum amount match ratio (tender value / fund release value)
# E.g. 0.6 means the tender must be ≥60% of the fund release amount
MIN_AMOUNT_MATCH_RATIO = 0.60

# Minimum fund release size to trace (filter out tiny grants)
MIN_FUND_AMOUNT_CR = 1.0

# Minimum tender value to trace
MIN_TENDER_AMOUNT_CR = 0.5

# Minimum entity link confidence to count as a match
MIN_ENTITY_CONFIDENCE = 0.50


@dataclass
class CorrelatedFlow:
    """Represents a detected fund → tender → politician correlation."""
    politician_id: str
    politician_name: str
    fund_release_id: str
    tender_id: str
    company_id: str
    company_name: str
    company_cin: str
    fund_amount_cr: float
    tender_amount_cr: float
    fund_district: str
    fund_scheme: str
    release_date: date
    award_date: date
    lag_days: int
    amount_match_pct: float
    entity_link_type: str
    entity_confidence: float
    risk_tier: str
    evidence_summary: str
    risk_score_contrib: int = 0

    def __post_init__(self):
        self.risk_score_contrib = self._compute_contribution()
        if not self.risk_tier:
            self.risk_tier = self._compute_risk_tier()

    def _compute_risk_tier(self) -> str:
        if self.lag_days <= CRITICAL_LAG_DAYS:
            return "CRITICAL"
        elif self.lag_days <= HIGH_LAG_DAYS:
            return "HIGH"
        else:
            return "MEDIUM"

    def _compute_contribution(self) -> int:
        """Points contributed to the politician's fund_flow score (0–5 per trail)."""
        base = 5
        if self.risk_tier == "HIGH":
            base = 4
        elif self.risk_tier == "MEDIUM":
            base = 3
        # Reduce if entity confidence is lower
        if self.entity_confidence < 0.70:
            base = max(1, base - 1)
        return base


class FundTracer:
    """
    Connects PFMS fund releases to GeM tender awards via the entity graph.

    Workflow:
    1. Load all politician → company links from entity_links table
    2. Build a lookup: CIN → [politician_ids] for fast matching
    3. For each fund release in the lookback window:
        a. Find tenders in same district within MAX_LAG_DAYS
        b. Check if winner CIN is in politician entity graph
        c. Check amount match ratio
        d. Compute risk tier and evidence summary
        e. Save to fund_trails table
    """

    def __init__(self):
        self.conn = psycopg2.connect(
            os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor
        )

    def run_full_trace(self, lookback_days: int = 365):
        """
        Run correlation analysis over all data within lookback window.
        Typically called daily by Airflow after new data ingestion.
        """
        logger.info(f"Starting fund flow correlation (lookback: {lookback_days} days)...")
        cutoff_date = date.today() - timedelta(days=lookback_days)

        # Build CIN → politicians lookup table
        cin_to_politicians = self._build_cin_politician_map()
        logger.info(f"  Tracking {len(cin_to_politicians)} companies linked to politicians")

        # Load all fund releases in window
        fund_releases = self._load_fund_releases(cutoff_date)
        logger.info(f"  Analyzing {len(fund_releases)} fund releases...")

        trails_found = 0
        trails_saved = 0

        for release in fund_releases:
            if float(release["amount_cr"] or 0) < MIN_FUND_AMOUNT_CR:
                continue

            # Find candidate tenders in same district + time window
            tenders = self._find_candidate_tenders(
                district=release["district"],
                state=release["state"],
                release_date=release["release_date"],
                max_lag=MAX_LAG_DAYS,
            )

            for tender in tenders:
                if float(tender["contract_value_cr"] or 0) < MIN_TENDER_AMOUNT_CR:
                    continue

                # Check if this tender's winner is linked to any politician
                winner_cin = tender.get("winner_cin")
                if not winner_cin or winner_cin not in cin_to_politicians:
                    continue

                linked_politicians = cin_to_politicians[winner_cin]

                for pol_link in linked_politicians:
                    # Compute amount match ratio
                    amount_match = (float(tender["contract_value_cr"]) /
                                    float(release["amount_cr"]))
                    if amount_match < MIN_AMOUNT_MATCH_RATIO:
                        continue  # Amounts don't correlate

                    lag_days = (tender["award_date"] - release["release_date"]).days
                    risk_tier = self._compute_risk_tier(lag_days)

                    # Build evidence summary
                    evidence = self._build_evidence_summary(
                        release, tender, pol_link, lag_days, amount_match
                    )

                    flow = CorrelatedFlow(
                        politician_id=pol_link["politician_id"],
                        politician_name=pol_link["politician_name"],
                        fund_release_id=str(release["id"]),
                        tender_id=str(tender["id"]),
                        company_id=pol_link["company_id"],
                        company_name=pol_link["company_name"],
                        company_cin=winner_cin,
                        fund_amount_cr=float(release["amount_cr"]),
                        tender_amount_cr=float(tender["contract_value_cr"]),
                        fund_district=release["district"],
                        fund_scheme=release["scheme_name"],
                        release_date=release["release_date"],
                        award_date=tender["award_date"],
                        lag_days=lag_days,
                        amount_match_pct=round(amount_match * 100, 1),
                        entity_link_type=pol_link["link_type"],
                        entity_confidence=float(pol_link["confidence"]),
                        risk_tier=risk_tier,
                        evidence_summary=evidence,
                    )

                    trails_found += 1
                    if self._save_trail(flow):
                        trails_saved += 1

        logger.info(f"  Fund trace complete: {trails_found} correlations found, "
                    f"{trails_saved} new trails saved")
        return trails_saved

    # ── Core correlation logic ────────────────────────────────────────────────

    def _build_cin_politician_map(self) -> dict:
        """
        Build a fast lookup: company CIN → list of linked politicians.
        This is the key join between tender winners and politicians.
        """
        cin_map = {}
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT el.politician_id::text, el.company_id::text,
                       el.link_type, el.confidence,
                       p.name_normalized AS politician_name,
                       c.cin, c.name AS company_name
                FROM entity_links el
                JOIN politicians p ON p.id = el.politician_id
                JOIN companies c ON c.id = el.company_id
                WHERE el.confidence >= %s
                ORDER BY el.confidence DESC
            """, (MIN_ENTITY_CONFIDENCE,))

            for row in cur.fetchall():
                cin = row["cin"]
                if cin not in cin_map:
                    cin_map[cin] = []
                cin_map[cin].append(dict(row))

        return cin_map

    def _load_fund_releases(self, since: date) -> list[dict]:
        """Load all fund releases after the cutoff date."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, pfms_ref_id, scheme_name, scheme_category,
                       state, district, amount_cr, release_date
                FROM fund_releases
                WHERE release_date >= %s
                ORDER BY release_date DESC
            """, (since,))
            return [dict(r) for r in cur.fetchall()]

    def _find_candidate_tenders(self, district: str, state: str,
                                  release_date: date, max_lag: int) -> list[dict]:
        """
        Find tenders in the same district within the temporal window.
        The window is [release_date - 7 days, release_date + max_lag days]
        (small negative buffer allows for pre-planned tenders)
        """
        window_start = release_date - timedelta(days=7)
        window_end = release_date + timedelta(days=max_lag)

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, tender_ref_id, department, category, state, district,
                       award_date, contract_value_cr, winner_name, winner_cin
                FROM tenders
                WHERE district ILIKE %s
                  AND state ILIKE %s
                  AND award_date BETWEEN %s AND %s
                  AND winner_cin IS NOT NULL
                  AND contract_value_cr > 0
            """, (f"%{district}%", f"%{state}%", window_start, window_end))
            return [dict(r) for r in cur.fetchall()]

    def _compute_risk_tier(self, lag_days: int) -> str:
        if lag_days <= CRITICAL_LAG_DAYS:
            return "CRITICAL"
        elif lag_days <= HIGH_LAG_DAYS:
            return "HIGH"
        elif lag_days <= MEDIUM_LAG_DAYS:
            return "MEDIUM"
        return "LOW"

    def _build_evidence_summary(self, release: dict, tender: dict,
                                  pol_link: dict, lag_days: int,
                                  amount_match: float) -> str:
        """Build a human-readable evidence summary for this fund trail."""
        return (
            f"PFMS released ₹{release['amount_cr']:.1f}Cr under '{release['scheme_name']}' "
            f"to {release['district']}, {release['state']} on {release['release_date']}. "
            f"Within {lag_days} days, {tender['winner_name']} (CIN: {tender.get('winner_cin', 'N/A')}) "
            f"won a ₹{tender['contract_value_cr']:.1f}Cr tender from {tender.get('department', 'same dept')}. "
            f"This company is {pol_link['link_type']}-linked "
            f"(confidence: {pol_link['confidence']:.0%}) "
            f"to {pol_link['politician_name']}. "
            f"Amount match: {amount_match:.0%}."
        )

    # ── Database operations ───────────────────────────────────────────────────

    def _save_trail(self, flow: CorrelatedFlow) -> bool:
        """Save a correlated fund trail to PostgreSQL. Returns True if new record."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fund_trails (
                        politician_id, fund_release_id, tender_id, company_id,
                        lag_days, amount_match_pct, district_match, risk_tier,
                        risk_score_contrib, evidence_summary
                    ) VALUES (
                        %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                        %s, %s, TRUE, %s, %s, %s
                    )
                    ON CONFLICT (fund_release_id, tender_id) DO UPDATE SET
                        risk_tier = EXCLUDED.risk_tier,
                        evidence_summary = EXCLUDED.evidence_summary,
                        computed_at = NOW()
                    RETURNING (xmax = 0) AS is_new
                """, (
                    flow.politician_id, flow.fund_release_id, flow.tender_id,
                    flow.company_id, flow.lag_days, flow.amount_match_pct,
                    flow.risk_tier, flow.risk_score_contrib, flow.evidence_summary
                ))
                result = cur.fetchone()
                self.conn.commit()
                return result and result["is_new"]
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to save trail: {e}")
            return False

    # ── Report generation ─────────────────────────────────────────────────────

    def get_politician_trails(self, politician_id: str) -> list[dict]:
        """Get all fund trails for a specific politician."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT ft.*, fr.scheme_name, fr.release_date, fr.amount_cr AS fund_amount,
                       t.winner_name, t.contract_value_cr, t.award_date, t.department,
                       c.name AS company_name, c.cin
                FROM fund_trails ft
                JOIN fund_releases fr ON fr.id = ft.fund_release_id
                JOIN tenders t ON t.id = ft.tender_id
                JOIN companies c ON c.id = ft.company_id
                WHERE ft.politician_id = %s::uuid
                ORDER BY ft.risk_tier, ft.lag_days
            """, (politician_id,))
            return [dict(r) for r in cur.fetchall()]

    def get_top_trails(self, limit: int = 50) -> list[dict]:
        """Get the highest-risk fund trails across all politicians."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM v_active_fund_trails
                ORDER BY CASE risk_tier
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    ELSE 4 END, lag_days
                LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fund Flow Tracer")
    parser.add_argument("--lookback-days", type=int, default=365)
    args = parser.parse_args()

    tracer = FundTracer()
    try:
        tracer.run_full_trace(lookback_days=args.lookback_days)
    finally:
        tracer.close()
