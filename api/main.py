"""
api/main.py
===========
FastAPI application entry point for VIGILANT.IN.
Serves all data to the React spider web frontend.

Endpoints:
    GET /politicians              — List all politicians with risk scores
    GET /politicians/{id}         — Single politician with full details
    GET /politicians/{id}/score   — Score breakdown with reasons
    GET /politicians/{id}/trails  — Fund flow trails
    GET /politicians/{id}/graph   — Entity graph nodes for this politician
    GET /fund-trails              — Top fund trails across all politicians
    GET /stats                    — Platform statistics
    GET /search                   — Search politicians by name/state/party
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from models import (
    PoliticianSummary, PoliticianDetail, ScoreBreakdown,
    FundTrail, EntityGraphData, PlatformStats, SearchResult
)

load_dotenv()
logger = logging.getLogger(__name__)


# ── Database connection pool ──────────────────────────────────────────────────

def get_db():
    """Dependency: yields a PostgreSQL connection per request."""
    conn = psycopg2.connect(
        os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor
    )
    try:
        yield conn
    finally:
        conn.close()


# ── App factory ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("VIGILANT.IN API starting up...")
    yield
    logger.info("VIGILANT.IN API shutting down...")

app = FastAPI(
    title="VIGILANT.IN API",
    description="Political Corruption Intelligence Platform — Public Data Only",
    version="2.4.1",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://vigilant.in"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    return {"status": "ok", "version": "2.4.1"}


# ── Platform stats ────────────────────────────────────────────────────────────

@app.get("/api/stats", response_model=PlatformStats)
async def get_stats(db=Depends(get_db)):
    """Overall platform statistics for the dashboard header."""
    with db.cursor() as cur:
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM politicians) AS total_politicians,
                (SELECT COUNT(*) FROM risk_scores WHERE risk_classification = 'CRITICAL') AS critical_count,
                (SELECT COUNT(*) FROM risk_scores WHERE risk_classification = 'HIGH') AS high_count,
                (SELECT COUNT(*) FROM fund_trails) AS total_trails,
                (SELECT COALESCE(SUM(contract_value_cr), 0) FROM tenders
                 WHERE winner_cin IN (SELECT cin FROM companies
                                       JOIN entity_links el ON el.company_id = companies.id)) AS flagged_tender_value,
                (SELECT COUNT(DISTINCT state) FROM politicians) AS states_covered,
                (SELECT MAX(scored_at) FROM risk_scores) AS last_updated
        """)
        row = dict(cur.fetchone())

    return PlatformStats(
        total_politicians=row["total_politicians"],
        critical_suspects=row["critical_count"],
        high_risk=row["high_count"],
        total_fund_trails=row["total_trails"],
        flagged_tender_value_cr=float(row["flagged_tender_value"] or 0),
        states_covered=row["states_covered"],
        last_updated=str(row["last_updated"] or ""),
    )


# ── Politicians list ──────────────────────────────────────────────────────────

@app.get("/api/politicians", response_model=list[PoliticianSummary])
async def list_politicians(
    state: Optional[str] = Query(None),
    party: Optional[str] = Query(None),
    risk_level: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    sort_by: str = Query("score", regex="^(score|name|state|assets)$"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    db=Depends(get_db),
):
    """
    List politicians with summary risk scores.
    Supports filtering by state, party, risk level.
    """
    filters = []
    params = []

    if state:
        filters.append("p.state ILIKE %s")
        params.append(f"%{state}%")
    if party:
        filters.append("p.party ILIKE %s")
        params.append(f"%{party}%")
    if risk_level:
        filters.append("rs.risk_classification = %s")
        params.append(risk_level.upper())
    if min_score is not None:
        filters.append("rs.total_score >= %s")
        params.append(min_score)

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""
    order_map = {
        "score": "rs.total_score DESC NULLS LAST",
        "name": "p.name_normalized ASC",
        "state": "p.state, p.name_normalized",
        "assets": "pa.total_assets_lakh DESC NULLS LAST",
    }
    order_clause = f"ORDER BY {order_map.get(sort_by, 'rs.total_score DESC')}"

    with db.cursor() as cur:
        cur.execute(f"""
            SELECT
                p.id, p.name_normalized AS name, p.pan, p.party, p.state,
                p.constituency, p.election_year, p.position_held,
                COALESCE(rs.total_score, 0) AS total_score,
                rs.risk_classification,
                rs.score_asset_growth, rs.score_tender_linkage,
                rs.score_fund_flow, rs.score_land_reg,
                rs.score_rti_contradiction, rs.score_network_depth,
                rs.scored_at,
                pa.total_assets_lakh AS latest_assets_lakh,
                (SELECT COUNT(*) FROM entity_links el WHERE el.politician_id = p.id) AS linked_companies,
                (SELECT COUNT(*) FROM fund_trails ft WHERE ft.politician_id = p.id) AS fund_trail_count
            FROM politicians p
            LEFT JOIN risk_scores rs ON rs.politician_id = p.id
            LEFT JOIN LATERAL (
                SELECT total_assets_lakh FROM politician_assets
                WHERE politician_id = p.id ORDER BY election_year DESC LIMIT 1
            ) pa ON TRUE
            {where_clause}
            {order_clause}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        rows = [dict(r) for r in cur.fetchall()]

    return [PoliticianSummary(**r) for r in rows]


# ── Single politician ─────────────────────────────────────────────────────────

@app.get("/api/politicians/{politician_id}", response_model=PoliticianDetail)
async def get_politician(politician_id: str, db=Depends(get_db)):
    """Get complete politician profile including all evidence."""
    with db.cursor() as cur:
        cur.execute("SELECT * FROM politicians WHERE id = %s", (politician_id,))
        pol = cur.fetchone()
        if not pol:
            raise HTTPException(status_code=404, detail="Politician not found")
        pol = dict(pol)

        # Assets history
        cur.execute("""
            SELECT election_year, total_assets_lakh, declared_annual_income_lakh,
                   residential_property_lakh, agricultural_land_lakh, cash_in_hand_lakh
            FROM politician_assets WHERE politician_id = %s ORDER BY election_year
        """, (politician_id,))
        pol["assets_history"] = [dict(r) for r in cur.fetchall()]

        # Family members
        cur.execute("""
            SELECT name_normalized, relation, pan FROM politician_family
            WHERE politician_id = %s
        """, (politician_id,))
        pol["family_members"] = [dict(r) for r in cur.fetchall()]

        # Linked companies
        cur.execute("""
            SELECT c.cin, c.name, c.status, c.state_of_reg,
                   el.link_type, el.confidence, el.relation_via
            FROM entity_links el
            JOIN companies c ON c.id = el.company_id
            WHERE el.politician_id = %s
            ORDER BY el.confidence DESC
        """, (politician_id,))
        pol["linked_companies"] = [dict(r) for r in cur.fetchall()]

        # Risk score
        cur.execute("SELECT * FROM risk_scores WHERE politician_id = %s", (politician_id,))
        score_row = cur.fetchone()
        pol["risk_score"] = dict(score_row) if score_row else None

    return PoliticianDetail(**pol)


# ── Score breakdown ───────────────────────────────────────────────────────────

@app.get("/api/politicians/{politician_id}/score", response_model=ScoreBreakdown)
async def get_score_breakdown(politician_id: str, db=Depends(get_db)):
    """
    Detailed score breakdown with human-readable reasons for each criterion.
    This is what powers the 'click to expand' UI in the spider web dashboard.
    """
    with db.cursor() as cur:
        cur.execute("""
            SELECT rs.*, p.name_normalized AS politician_name,
                   p.state, p.constituency, p.party
            FROM risk_scores rs
            JOIN politicians p ON p.id = rs.politician_id
            WHERE rs.politician_id = %s
        """, (politician_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Score not found. Run scorer.py first.")

    return ScoreBreakdown(**dict(row))


# ── Fund trails ───────────────────────────────────────────────────────────────

@app.get("/api/politicians/{politician_id}/trails")
async def get_politician_trails(politician_id: str, db=Depends(get_db)):
    """Get all fund flow trails for a politician."""
    with db.cursor() as cur:
        cur.execute("""
            SELECT ft.risk_tier, ft.lag_days, ft.amount_match_pct,
                   ft.evidence_summary, ft.computed_at,
                   fr.scheme_name, fr.amount_cr AS fund_amount,
                   fr.release_date, fr.district AS fund_district,
                   t.winner_name, t.contract_value_cr, t.award_date, t.department,
                   c.name AS company_name, c.cin
            FROM fund_trails ft
            JOIN fund_releases fr ON fr.id = ft.fund_release_id
            JOIN tenders t ON t.id = ft.tender_id
            JOIN companies c ON c.id = ft.company_id
            WHERE ft.politician_id = %s
            ORDER BY CASE ft.risk_tier
                WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3 ELSE 4 END, ft.lag_days
        """, (politician_id,))
        return [dict(r) for r in cur.fetchall()]


# ── Top fund trails (global) ──────────────────────────────────────────────────

@app.get("/api/fund-trails")
async def get_top_fund_trails(
    risk_tier: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    db=Depends(get_db),
):
    """Get top fund trails across all politicians."""
    with db.cursor() as cur:
        risk_filter = "AND ft.risk_tier = %s" if risk_tier else ""
        params = [risk_tier, limit] if risk_tier else [limit]
        cur.execute(f"""
            SELECT * FROM v_active_fund_trails
            WHERE TRUE {risk_filter}
            LIMIT %s
        """, params)
        return [dict(r) for r in cur.fetchall()]


# ── Search ────────────────────────────────────────────────────────────────────

@app.get("/api/search")
async def search(
    q: str = Query(..., min_length=2),
    db=Depends(get_db),
):
    """Full-text search for politicians by name, state, party."""
    with db.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.name_normalized AS name, p.party, p.state,
                   p.constituency, COALESCE(rs.total_score, 0) AS score,
                   rs.risk_classification,
                   similarity(p.name_normalized, %s) AS name_sim
            FROM politicians p
            LEFT JOIN risk_scores rs ON rs.politician_id = p.id
            WHERE p.name_normalized ILIKE %s
               OR p.state ILIKE %s
               OR p.party ILIKE %s
            ORDER BY name_sim DESC, score DESC
            LIMIT 20
        """, (q, f"%{q}%", f"%{q}%", f"%{q}%"))
        return [dict(r) for r in cur.fetchall()]


# ── Entity graph data ─────────────────────────────────────────────────────────

@app.get("/api/politicians/{politician_id}/graph")
async def get_entity_graph(politician_id: str, db=Depends(get_db)):
    """
    Returns graph node and edge data for the spider web visualization.
    Includes the politician, all linked companies, fund releases, and tenders.
    """
    nodes = []
    edges = []

    with db.cursor() as cur:
        # Politician node
        cur.execute("SELECT id, name_normalized, state, party FROM politicians WHERE id = %s",
                    (politician_id,))
        pol = cur.fetchone()
        if not pol:
            raise HTTPException(status_code=404, detail="Not found")

        nodes.append({"id": str(pol["id"]), "type": "politician",
                      "label": pol["name_normalized"], "meta": dict(pol)})

        # Linked companies
        cur.execute("""
            SELECT c.id, c.cin, c.name, el.link_type, el.confidence, el.relation_via
            FROM entity_links el JOIN companies c ON c.id = el.company_id
            WHERE el.politician_id = %s
        """, (politician_id,))
        for c in cur.fetchall():
            nodes.append({"id": str(c["id"]), "type": "company",
                          "label": c["name"], "meta": dict(c)})
            edges.append({"source": politician_id, "target": str(c["id"]),
                          "type": c["link_type"], "confidence": float(c["confidence"] or 0)})

        # Fund trails: fund releases + tenders
        cur.execute("""
            SELECT ft.id, fr.id AS fr_id, fr.scheme_name, fr.amount_cr, fr.release_date,
                   t.id AS t_id, t.winner_name, t.contract_value_cr, t.award_date,
                   ft.risk_tier, ft.lag_days
            FROM fund_trails ft
            JOIN fund_releases fr ON fr.id = ft.fund_release_id
            JOIN tenders t ON t.id = ft.tender_id
            WHERE ft.politician_id = %s
        """, (politician_id,))
        for trail in cur.fetchall():
            fr_id = f"fr_{trail['fr_id']}"
            t_id = f"t_{trail['t_id']}"
            nodes.append({"id": fr_id, "type": "fund_release",
                          "label": f"₹{trail['amount_cr']}Cr {trail['scheme_name'][:20]}",
                          "meta": {"date": str(trail["release_date"])}})
            nodes.append({"id": t_id, "type": "tender",
                          "label": f"₹{trail['contract_value_cr']}Cr {trail['winner_name'][:20]}",
                          "meta": {"date": str(trail["award_date"])}})
            edges.append({"source": fr_id, "target": t_id,
                          "type": "funded_by", "lag_days": trail["lag_days"],
                          "risk_tier": trail["risk_tier"]})

    # Deduplicate nodes
    seen = set()
    unique_nodes = []
    for n in nodes:
        if n["id"] not in seen:
            seen.add(n["id"])
            unique_nodes.append(n)

    return {"nodes": unique_nodes, "edges": edges}
