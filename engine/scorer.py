"""
engine/scorer.py
================
Multi-factor corruption risk scoring engine.

Computes a transparent, explainable 0–100 risk score for each politician
based on 6 weighted criteria. Every point is traceable to a specific
data record — no black boxes.

Score breakdown:
    1. Asset Growth Anomaly         (0–25 pts)
    2. Tender-to-Relative Linkage   (0–25 pts)
    3. Fund Flow Correlation        (0–20 pts)
    4. Land Registration Spike      (0–15 pts)
    5. RTI Contradiction            (0–10 pts)
    6. Network Depth Score          (0–5 pts)
    ─────────────────────────────────────────
    TOTAL                           (0–100 pts)
"""

import os
import json
import logging
from datetime import date, timedelta
from typing import Optional
from dataclasses import dataclass, asdict

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Score weights ─────────────────────────────────────────────────────────────

MAX_SCORES = {
    "asset_growth": 25,
    "tender_linkage": 25,
    "fund_flow": 20,
    "land_reg": 15,
    "rti_contradiction": 10,
    "network_depth": 5,
}

# Risk classification thresholds
RISK_THRESHOLDS = {
    "CRITICAL": 75,
    "HIGH": 50,
    "WATCH": 30,
    "LOW": 0,
}

# Assumed average annual salary for an MLA/MP (in lakhs)
# Includes salary + allowances + constituency fund (conservative estimate)
ANNUAL_SALARY_LAKH = 12.0  # ~₹12 lakh/year = ₹1 lakh/month total compensation

# Minimum unexplained asset growth % to start scoring
ASSET_GROWTH_THRESHOLD_PCT = 100.0


@dataclass
class ScoreComponents:
    """Holds all scoring component results for one politician."""
    politician_id: str
    politician_name: str

    # Raw metrics (stored for explainability)
    assets_latest_lakh: float = 0.0
    assets_earliest_lakh: float = 0.0
    years_tracked: int = 0
    provable_income_lakh: float = 0.0
    unexplained_growth_lakh: float = 0.0
    unexplained_growth_pct: float = 0.0

    linked_tender_count: int = 0
    linked_tender_value_cr: float = 0.0

    fund_trail_count: int = 0
    critical_trails: int = 0
    high_trails: int = 0

    rera_flagged_count: int = 0

    rti_contradiction_count: int = 0

    max_shell_depth: int = 0

    # Computed scores
    score_asset_growth: int = 0
    score_tender_linkage: int = 0
    score_fund_flow: int = 0
    score_land_reg: int = 0
    score_rti_contradiction: int = 0
    score_network_depth: int = 0

    # Score reasons (human-readable explanations)
    reasons: dict = None

    def __post_init__(self):
        if self.reasons is None:
            self.reasons = {}

    @property
    def total_score(self) -> int:
        return (self.score_asset_growth + self.score_tender_linkage +
                self.score_fund_flow + self.score_land_reg +
                self.score_rti_contradiction + self.score_network_depth)

    @property
    def risk_classification(self) -> str:
        for label, threshold in RISK_THRESHOLDS.items():
            if self.total_score >= threshold:
                return label
        return "LOW"


class PoliticianScorer:
    """
    Computes the 6-factor corruption risk score for each politician.

    Each criterion has:
    1. A formula for computing the raw score
    2. A human-readable explanation of what was found
    3. A reference to the source data that drove the score

    The scorer reads from all PostgreSQL tables and produces
    final scores in the risk_scores table.
    """

    def __init__(self):
        self.conn = psycopg2.connect(
            os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor
        )

    def score_all(self) -> list[ScoreComponents]:
        """Score all politicians in the database."""
        politicians = self._load_all_politicians()
        logger.info(f"Scoring {len(politicians)} politicians...")

        results = []
        for pol in politicians:
            try:
                score = self.score_politician(pol["id"])
                results.append(score)
                self._save_score(score)
                logger.info(
                    f"  {pol['name_normalized']:30s} → {score.total_score:3d} "
                    f"({score.risk_classification})"
                )
            except Exception as e:
                logger.error(f"Failed to score {pol['name_normalized']}: {e}")

        logger.info(f"Scoring complete. {len(results)} politicians scored.")
        return results

    def score_politician(self, politician_id: str) -> ScoreComponents:
        """Compute full score for a single politician."""
        pol = self._load_politician(politician_id)
        components = ScoreComponents(
            politician_id=str(politician_id),
            politician_name=pol.get("name_normalized", ""),
        )

        # Apply each scoring criterion
        self._score_asset_growth(components, pol)
        self._score_tender_linkage(components, pol)
        self._score_fund_flow(components, pol)
        self._score_land_reg(components, pol)
        self._score_rti(components, pol)
        self._score_network_depth(components, pol)

        return components

    # ── Criterion 1: Asset Growth Anomaly ─────────────────────────────────────

    def _score_asset_growth(self, c: ScoreComponents, pol: dict):
        """
        Score based on unexplained asset growth vs provable income.

        Formula:
            provable_income = (annual_salary × years) + declared_business_income
            unexplained_growth = (assets_latest - assets_earliest) - provable_income
            growth_pct = (unexplained_growth / assets_earliest) × 100
            score = min(25, max(0, (growth_pct - 100) / 20))

        Rationale: We give benefit of doubt — growth up to 100% above income
        could be investment returns, inheritance, etc. Beyond 100% is suspicious.
        The score scales linearly: 200% → 5pts, 400% → 15pts, 600% → 25pts.
        """
        assets = self._load_assets(pol["id"])
        if len(assets) < 2:
            c.reasons["asset_growth"] = "Insufficient historical data (need 2+ elections)"
            return

        # Sort by year and take earliest vs latest
        assets.sort(key=lambda x: x["election_year"])
        earliest = assets[0]
        latest = assets[-1]
        years = latest["election_year"] - earliest["election_year"]

        if years == 0:
            return

        earliest_lakh = float(earliest["total_assets_lakh"] or 0)
        latest_lakh = float(latest["total_assets_lakh"] or 0)

        if earliest_lakh <= 0:
            c.reasons["asset_growth"] = "Zero/nil initial assets — cannot compute growth."
            return

        # Provable income over the period
        declared_income_lakh = float(latest.get("declared_annual_income_lakh") or
                                      ANNUAL_SALARY_LAKH) * years
        provable_lakh = declared_income_lakh

        # Unexplained growth
        total_growth = latest_lakh - earliest_lakh
        unexplained = max(0, total_growth - provable_lakh)
        growth_pct = (unexplained / earliest_lakh) * 100

        # Store metrics
        c.assets_earliest_lakh = earliest_lakh
        c.assets_latest_lakh = latest_lakh
        c.years_tracked = years
        c.provable_income_lakh = provable_lakh
        c.unexplained_growth_lakh = unexplained
        c.unexplained_growth_pct = round(growth_pct, 1)

        # Compute score
        if growth_pct <= ASSET_GROWTH_THRESHOLD_PCT:
            c.score_asset_growth = 0
            c.reasons["asset_growth"] = (
                f"Asset growth of {growth_pct:.0f}% is within explainable range. "
                f"Assets: ₹{earliest_lakh:.1f}L → ₹{latest_lakh:.1f}L over {years} years."
            )
        else:
            raw_score = (growth_pct - ASSET_GROWTH_THRESHOLD_PCT) / 20.0
            c.score_asset_growth = min(MAX_SCORES["asset_growth"], int(raw_score))
            c.reasons["asset_growth"] = (
                f"Assets grew from ₹{earliest_lakh:.1f}L to ₹{latest_lakh:.1f}L "
                f"({growth_pct:.0f}% unexplained growth over {years} years). "
                f"Provable income: ₹{provable_lakh:.1f}L. "
                f"Unexplained surplus: ₹{unexplained:.1f}L."
            )

    # ── Criterion 2: Tender-to-Relative Linkage ────────────────────────────────

    def _score_tender_linkage(self, c: ScoreComponents, pol: dict):
        """
        Score based on number of tenders won by politician-linked entities.

        Formula:
            score = min(25, linked_tender_count × 4)

        Why 4 points per tender? Because each verified tender-to-family link
        is a strong signal. A single coincidental tender is possible — but
        4+ tenders to the same network is statistically improbable.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS tender_count,
                       COALESCE(SUM(t.contract_value_cr), 0) AS total_value
                FROM entity_links el
                JOIN companies c ON c.id = el.company_id
                JOIN tenders t ON t.winner_cin = c.cin
                WHERE el.politician_id = %s::uuid
                  AND el.confidence >= 0.70
            """, (pol["id"],))
            row = cur.fetchone()

        tender_count = int(row["tender_count"] or 0)
        total_value = float(row["total_value"] or 0)

        c.linked_tender_count = tender_count
        c.linked_tender_value_cr = total_value

        c.score_tender_linkage = min(MAX_SCORES["tender_linkage"], tender_count * 4)

        if tender_count == 0:
            c.reasons["tender_linkage"] = "No tenders found awarded to linked entities."
        else:
            # Get company names for the explanation
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT c.name, el.link_type, el.confidence,
                                    COUNT(t.id) AS tender_count,
                                    SUM(t.contract_value_cr) AS tender_value
                    FROM entity_links el
                    JOIN companies c ON c.id = el.company_id
                    JOIN tenders t ON t.winner_cin = c.cin
                    WHERE el.politician_id = %s::uuid AND el.confidence >= 0.70
                    GROUP BY c.name, el.link_type, el.confidence
                    ORDER BY tender_value DESC LIMIT 5
                """, (pol["id"],))
                top_companies = cur.fetchall()

            company_desc = "; ".join([
                f"{r['name']} ({r['link_type']}, {int(r['tender_count'])} tenders, "
                f"₹{float(r['tender_value']):.1f}Cr)"
                for r in top_companies
            ])
            c.reasons["tender_linkage"] = (
                f"{tender_count} tenders (total ₹{total_value:.1f}Cr) awarded to linked entities: "
                f"{company_desc}"
            )

    # ── Criterion 3: Fund Flow Correlation ────────────────────────────────────

    def _score_fund_flow(self, c: ScoreComponents, pol: dict):
        """
        Score based on confirmed fund trail correlations.

        Formula:
            correlated_flows = count(fund_trails WHERE politician_id = X)
            score = min(20, correlated_flows × 5)

        CRITICAL trails (lag ≤50d) are worth 2× a MEDIUM trail because
        the shorter the lag, the more deliberate the corruption appears.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT risk_tier, COUNT(*) AS count, SUM(risk_score_contrib) AS contrib
                FROM fund_trails
                WHERE politician_id = %s::uuid
                GROUP BY risk_tier
            """, (pol["id"],))
            trail_rows = cur.fetchall()

        trail_by_tier = {r["risk_tier"]: dict(r) for r in trail_rows}

        critical = int((trail_by_tier.get("CRITICAL") or {}).get("count", 0))
        high = int((trail_by_tier.get("HIGH") or {}).get("count", 0))
        medium = int((trail_by_tier.get("MEDIUM") or {}).get("count", 0))
        total = critical + high + medium

        c.fund_trail_count = total
        c.critical_trails = critical
        c.high_trails = high

        # Weighted count: CRITICAL=2pts, HIGH=1.5pts, MEDIUM=1pt
        weighted = (critical * 2) + (high * 1.5) + (medium * 1.0)
        c.score_fund_flow = min(MAX_SCORES["fund_flow"], int(weighted * 2.5))

        if total == 0:
            c.reasons["fund_flow"] = "No fund-to-tender correlations detected."
        else:
            c.reasons["fund_flow"] = (
                f"{total} fund trail(s) detected: {critical} CRITICAL (≤50d lag), "
                f"{high} HIGH (≤90d lag), {medium} MEDIUM (≤180d lag)."
            )

    # ── Criterion 4: Land Registration Spike ──────────────────────────────────

    def _score_land_reg(self, c: ScoreComponents, pol: dict):
        """
        Score based on RERA property acquisitions near fund releases.

        Formula:
            score = min(15, flagged_properties × 3)
        """
        with self.conn.cursor() as cur:
            # RERA properties registered by linked entities within 180d of fund release
            cur.execute("""
                SELECT COUNT(DISTINCT r.id) AS flagged
                FROM rera_properties r
                JOIN entity_links el ON (
                    r.promoter_cin = (SELECT cin FROM companies WHERE id = el.company_id)
                    OR r.promoter_pan IN (
                        SELECT pan FROM politicians WHERE id = %s::uuid
                        UNION
                        SELECT pan FROM politician_family WHERE politician_id = %s::uuid
                    )
                )
                JOIN fund_releases fr ON (
                    r.state = fr.state
                    AND ABS(r.registration_date - fr.release_date) <= 180
                )
                WHERE el.politician_id = %s::uuid
            """, (pol["id"], pol["id"], pol["id"]))
            flagged = int((cur.fetchone() or {}).get("flagged", 0))

        c.rera_flagged_count = flagged
        c.score_land_reg = min(MAX_SCORES["land_reg"], flagged * 3)

        if flagged == 0:
            c.reasons["land_reg"] = "No suspicious property registrations detected."
        else:
            c.reasons["land_reg"] = (
                f"{flagged} RERA property registration(s) by linked entities "
                f"found within 180 days of fund releases in the same state."
            )

    # ── Criterion 5: RTI Contradictions ───────────────────────────────────────

    def _score_rti(self, c: ScoreComponents, pol: dict):
        """
        Score based on RTI responses that contradict official records.

        Formula:
            score = min(10, contradictions × 2.5)
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM rti_flags rf
                WHERE rf.contractor_name ILIKE ANY(
                    SELECT '%' || c.name || '%'
                    FROM companies c
                    JOIN entity_links el ON el.company_id = c.id
                    WHERE el.politician_id = %s::uuid
                )
            """, (pol["id"],))
            count = int((cur.fetchone() or {}).get("cnt", 0))

        c.rti_contradiction_count = count
        c.score_rti_contradiction = min(MAX_SCORES["rti_contradiction"],
                                         int(count * 2.5))

        if count == 0:
            c.reasons["rti_contradiction"] = "No RTI contradictions found for linked contractors."
        else:
            c.reasons["rti_contradiction"] = (
                f"{count} RTI response(s) reveal contradictions involving "
                f"contractors linked to this politician."
            )

    # ── Criterion 6: Network Depth (Shell Company Score) ──────────────────────

    def _score_network_depth(self, c: ScoreComponents, pol: dict):
        """
        Score based on shell company layering in the entity graph.

        Formula:
            score = shell_layers > 3 ? 5 : shell_layers × 1

        More than 3 layers between politician and final contractor
        is a red flag for deliberate obfuscation.
        """
        with self.conn.cursor() as cur:
            # Check entity_links for deep graph connections
            cur.execute("""
                SELECT COALESCE(MAX(graph_depth), 0) AS max_depth
                FROM entity_links
                WHERE politician_id = %s::uuid
            """, (pol["id"],))
            max_depth = int((cur.fetchone() or {}).get("max_depth", 0))

        c.max_shell_depth = max_depth
        c.score_network_depth = 5 if max_depth > 3 else max_depth

        c.reasons["network_depth"] = (
            f"Maximum shell company depth: {max_depth} layer(s) "
            f"between politician and ultimate company. "
            f"{'Deep obfuscation detected.' if max_depth > 3 else 'Shallow network.'}"
        )

    # ── Database operations ───────────────────────────────────────────────────

    def _save_score(self, c: ScoreComponents):
        """Upsert final score to risk_scores table."""
        raw_metrics = {
            "assets_latest_lakh": c.assets_latest_lakh,
            "assets_earliest_lakh": c.assets_earliest_lakh,
            "unexplained_growth_pct": c.unexplained_growth_pct,
            "linked_tender_count": c.linked_tender_count,
            "linked_tender_value_cr": c.linked_tender_value_cr,
            "fund_trail_count": c.fund_trail_count,
            "critical_trails": c.critical_trails,
            "rera_flagged": c.rera_flagged_count,
            "rti_count": c.rti_contradiction_count,
            "max_shell_depth": c.max_shell_depth,
        }

        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO risk_scores (
                    politician_id, score_asset_growth, score_tender_linkage,
                    score_fund_flow, score_land_reg, score_rti_contradiction,
                    score_network_depth, risk_classification,
                    score_reasons, raw_metrics
                ) VALUES (
                    %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (politician_id) DO UPDATE SET
                    score_asset_growth = EXCLUDED.score_asset_growth,
                    score_tender_linkage = EXCLUDED.score_tender_linkage,
                    score_fund_flow = EXCLUDED.score_fund_flow,
                    score_land_reg = EXCLUDED.score_land_reg,
                    score_rti_contradiction = EXCLUDED.score_rti_contradiction,
                    score_network_depth = EXCLUDED.score_network_depth,
                    risk_classification = EXCLUDED.risk_classification,
                    score_reasons = EXCLUDED.score_reasons,
                    raw_metrics = EXCLUDED.raw_metrics,
                    scored_at = NOW()
            """, (
                c.politician_id,
                c.score_asset_growth, c.score_tender_linkage,
                c.score_fund_flow, c.score_land_reg,
                c.score_rti_contradiction, c.score_network_depth,
                c.risk_classification,
                json.dumps(c.reasons),
                json.dumps(raw_metrics),
            ))
            self.conn.commit()

    def _load_all_politicians(self) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT id, name_normalized FROM politicians ORDER BY state")
            return [dict(r) for r in cur.fetchall()]

    def _load_politician(self, politician_id: str) -> dict:
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM politicians WHERE id = %s", (str(politician_id),))
            return dict(cur.fetchone() or {})

    def _load_assets(self, politician_id: str) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT election_year, total_assets_lakh, declared_annual_income_lakh
                FROM politician_assets WHERE politician_id = %s ORDER BY election_year
            """, (str(politician_id),))
            return [dict(r) for r in cur.fetchall()]

    def close(self):
        self.conn.close()


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Politician Risk Scorer")
    parser.add_argument("--politician-id", help="Score single politician UUID")
    parser.add_argument("--all", action="store_true", default=True,
                        help="Score all politicians (default)")
    args = parser.parse_args()

    scorer = PoliticianScorer()
    try:
        if args.politician_id:
            score = scorer.score_politician(args.politician_id)
            scorer._save_score(score)
            print(f"\n{'='*60}")
            print(f"Politician: {score.politician_name}")
            print(f"Total Score: {score.total_score}/100 ({score.risk_classification})")
            print(f"\nBreakdown:")
            for criterion, max_score in MAX_SCORES.items():
                raw_attr = f"score_{criterion}"
                val = getattr(score, raw_attr, 0)
                print(f"  {criterion:25s}: {val:3d}/{max_score}")
            print(f"\nReasons:")
            for k, v in score.reasons.items():
                print(f"  [{k}] {v}")
        else:
            scorer.score_all()
    finally:
        scorer.close()
