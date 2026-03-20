"""
engine/entity_graph.py
======================
Builds and maintains the Neo4j entity relationship graph.

This is the INTELLIGENCE CORE of VIGILANT.IN.

It connects:
  Politician → (via PAN) → Companies they directly direct
  Politician → (via family PAN) → Companies family members direct
  Politician → (via name fuzzy match) → Companies with related directors
  Companies → Subsidiaries → Sub-subsidiaries (up to depth 4)

The graph is then queried by fund_tracer.py to detect fund flows
from government schemes → tenders won by politician-linked companies.
"""

import os
import logging
import re
from typing import Optional
from datetime import datetime, date

import psycopg2
from psycopg2.extras import RealDictCursor
from neo4j import GraphDatabase
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

# Minimum fuzzy match score to accept a name match (0-100)
# 85 = high confidence; 70 = moderate; below 70 = reject
FUZZY_MATCH_THRESHOLD = 78

# Confidence weights for different match types
CONFIDENCE_WEIGHTS = {
    "pan_exact": 1.00,        # PAN matches exactly → certain
    "din_exact": 0.97,        # DIN matches exactly → near-certain
    "name_spouse": 0.93,      # Spouse name + surname match
    "name_child": 0.90,       # Child name match
    "name_sibling": 0.82,     # Sibling name match
    "name_parent": 0.80,      # Parent name match
    "name_fuzzy_high": 0.72,  # Fuzzy match > 90% similarity
    "name_fuzzy_med": 0.55,   # Fuzzy match 78–90%
    "address_match": 0.40,    # Same registered address
}


class EntityGraphBuilder:
    """
    Reads PostgreSQL data (politicians, companies, directors, family members)
    and constructs the Neo4j entity graph.

    Graph traversal logic:
    1. For each politician, create a :Politician node
    2. For each company where their PAN appears → IS_DIRECTOR/IS_SHAREHOLDER edge
    3. For each family member → FAMILY_OF edge + their company links
    4. Fuzzy-match all director names against family member names
    5. For matched directors → infer IS_DIRECTOR relationship with confidence score
    6. For each company → traverse subsidiaries → SUBSIDIARY_OF edges
    """

    def __init__(self):
        self.pg_conn = psycopg2.connect(
            os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor
        )
        self.neo4j_driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD"))
        )
        self._stats = {
            "politicians_processed": 0,
            "companies_linked": 0,
            "family_links": 0,
            "fuzzy_matches": 0,
            "entity_links_saved": 0,
        }

    # ── Main entry points ─────────────────────────────────────────────────────

    def build_full_graph(self):
        """Full rebuild of the entire entity graph. Run weekly or after major ingestion."""
        logger.info("Starting full entity graph rebuild...")

        politicians = self._load_all_politicians()
        logger.info(f"Processing {len(politicians)} politicians...")

        with self.neo4j_driver.session() as session:
            for pol in politicians:
                try:
                    self._process_politician(session, pol)
                    self._stats["politicians_processed"] += 1
                except Exception as e:
                    logger.error(f"Failed to process politician {pol['name_normalized']}: {e}")

        # After all nodes exist, compute cross-company relationships
        self._compute_shared_director_links()
        # Save entity_links summary back to PostgreSQL for fast API queries
        self._sync_entity_links_to_postgres()

        logger.info(f"Graph build complete: {self._stats}")

    def update_for_politician(self, politician_id: str):
        """Incremental update for a single politician. Called after new scrape data."""
        pol = self._load_politician(politician_id)
        if not pol:
            return
        with self.neo4j_driver.session() as session:
            self._process_politician(session, pol)
        self._sync_entity_links_to_postgres(politician_id=politician_id)

    # ── Processing pipeline ───────────────────────────────────────────────────

    def _process_politician(self, session, pol: dict):
        """Build all graph nodes and edges for one politician."""
        logger.debug(f"  Processing: {pol['name_normalized']}")

        # 1. Create/update Politician node
        self._upsert_politician_node(session, pol)

        # 2. Direct PAN → company links
        if pol.get("pan"):
            direct_companies = self._find_companies_by_pan(pol["pan"])
            for company in direct_companies:
                self._upsert_company_node(session, company)
                self._create_edge(session, "Politician", pol["pan"],
                                   "Company", company["cin"],
                                   "IS_DIRECTOR", {
                                       "confidence": CONFIDENCE_WEIGHTS["pan_exact"],
                                       "match_type": "pan_exact",
                                       "since": str(company.get("appointed_date", "")),
                                   })
                self._stats["companies_linked"] += 1

        # 3. Family member → company links
        family_members = self._load_family(pol["id"])
        for member in family_members:
            self._process_family_member(session, pol, member)

        # 4. Fuzzy name matching — catch undisclosed family links
        self._fuzzy_match_directors(session, pol, family_members)

    def _process_family_member(self, session, pol: dict, member: dict):
        """Create FAMILY_OF edge and find the member's companies."""
        # Create Individual node
        self._upsert_individual_node(session, member)

        # FAMILY_OF edge: Individual → Politician
        rel_confidence = {
            "spouse": 0.95, "child": 0.90, "sibling": 0.82, "parent": 0.80
        }.get(member["relation"], 0.70)

        self._create_family_edge(session, member, pol, rel_confidence)
        self._stats["family_links"] += 1

        # Find companies linked to this family member
        if member.get("pan"):
            companies = self._find_companies_by_pan(member["pan"])
            for company in companies:
                self._upsert_company_node(session, company)
                # Edge: Individual → Company (IS_DIRECTOR)
                self._create_individual_company_edge(
                    session, member, company,
                    confidence=CONFIDENCE_WEIGHTS["pan_exact"]
                )
                self._stats["companies_linked"] += 1

    def _fuzzy_match_directors(self, session, pol: dict, family_members: list):
        """
        Fuzzy-match politician and family names against all directors of
        companies in the politician's state. This catches cases where
        family members aren't declared in the affidavit.
        """
        # Get all directors from companies registered in the same state
        directors = self._load_directors_by_state(pol.get("state", ""))

        # Build name list to match against
        names_to_match = [pol["name_normalized"]] + [
            m["name_normalized"] for m in family_members
        ]

        for director in directors:
            dir_name = director["name_normalized"]
            if not dir_name:
                continue

            best_score = 0
            best_match_name = None
            best_member = None

            for i, match_name in enumerate(names_to_match):
                score = fuzz.token_sort_ratio(dir_name, match_name)
                if score > best_score:
                    best_score = score
                    best_match_name = match_name
                    best_member = family_members[i - 1] if i > 0 else None

            if best_score >= FUZZY_MATCH_THRESHOLD:
                confidence = (
                    CONFIDENCE_WEIGHTS["name_fuzzy_high"] if best_score >= 90
                    else CONFIDENCE_WEIGHTS["name_fuzzy_med"]
                )

                company = self._load_company(director["company_id"])
                if company:
                    self._upsert_company_node(session, company)

                    if best_member:
                        # Family member match
                        self._upsert_individual_node(session, {
                            **director,
                            "name_raw": director["name_raw"],
                            "name_normalized": director["name_normalized"],
                            "relation": best_member["relation"],
                        })
                        self._create_individual_company_edge(
                            session, director, company, confidence=confidence
                        )
                    else:
                        # Direct politician name match
                        self._create_edge(
                            session, "Politician", pol["pan"],
                            "Company", company["cin"],
                            "IS_DIRECTOR", {
                                "confidence": confidence,
                                "match_type": f"fuzzy_{best_score}",
                                "matched_name": dir_name,
                            }
                        )

                    self._stats["fuzzy_matches"] += 1
                    logger.debug(
                        f"    Fuzzy match ({best_score}): {dir_name} ≈ {best_match_name} "
                        f"→ {company.get('name', '')}"
                    )

    # ── Neo4j node/edge operations ────────────────────────────────────────────

    def _upsert_politician_node(self, session, pol: dict):
        session.run("""
            MERGE (p:Politician {pan: $pan})
            ON CREATE SET
                p.pg_id = $pg_id,
                p.name_raw = $name_raw,
                p.name_normalized = $name_normalized,
                p.party = $party,
                p.state = $state,
                p.constituency = $constituency,
                p.election_year = $election_year,
                p.position_held = $position_held,
                p.created_at = datetime()
            ON MATCH SET
                p.name_normalized = $name_normalized,
                p.position_held = $position_held,
                p.updated_at = datetime()
        """, {
            "pan": pol.get("pan") or f"UNKNOWN_{pol['id']}",
            "pg_id": str(pol["id"]),
            "name_raw": pol.get("name_raw", ""),
            "name_normalized": pol.get("name_normalized", ""),
            "party": pol.get("party", ""),
            "state": pol.get("state", ""),
            "constituency": pol.get("constituency", ""),
            "election_year": pol.get("election_year"),
            "position_held": pol.get("position_held", ""),
        })

    def _upsert_company_node(self, session, company: dict):
        session.run("""
            MERGE (c:Company {cin: $cin})
            ON CREATE SET
                c.name = $name,
                c.name_normalized = $name_normalized,
                c.company_type = $company_type,
                c.status = $status,
                c.registration_date = $registration_date,
                c.state_of_reg = $state_of_reg,
                c.authorized_capital = $authorized_capital,
                c.created_at = datetime()
            ON MATCH SET
                c.status = $status,
                c.updated_at = datetime()
        """, {
            "cin": company["cin"],
            "name": company.get("name", ""),
            "name_normalized": company.get("name_normalized", ""),
            "company_type": company.get("company_type", ""),
            "status": company.get("status", ""),
            "registration_date": str(company.get("registration_date") or ""),
            "state_of_reg": company.get("state_of_reg", ""),
            "authorized_capital": float(company.get("authorized_capital") or 0),
        })

    def _upsert_individual_node(self, session, person: dict):
        session.run("""
            MERGE (i:Individual {din: $din})
            ON CREATE SET
                i.name_raw = $name_raw,
                i.name_normalized = $name_normalized,
                i.pan = $pan,
                i.role = $role,
                i.created_at = datetime()
            ON MATCH SET
                i.name_normalized = $name_normalized,
                i.pan = COALESCE($pan, i.pan),
                i.updated_at = datetime()
        """, {
            "din": person.get("din") or f"UNKNOWN_{person['name_normalized'][:20]}",
            "name_raw": person.get("name_raw", ""),
            "name_normalized": person.get("name_normalized", ""),
            "pan": person.get("pan"),
            "role": person.get("role", "director"),
        })

    def _create_edge(self, session, from_label: str, from_key: str,
                     to_label: str, to_key: str, rel_type: str, props: dict):
        """Generic MERGE relationship between two nodes."""
        from_key_field = "pan" if from_label == "Politician" else "cin"
        to_key_field = "cin" if to_label == "Company" else "pan"
        session.run(f"""
            MATCH (a:{from_label} {{{from_key_field}: $from_key}})
            MATCH (b:{to_label} {{{to_key_field}: $to_key}})
            MERGE (a)-[r:{rel_type}]->(b)
            ON CREATE SET r += $props, r.created_at = datetime()
            ON MATCH SET r.confidence = CASE
                WHEN $props.confidence > r.confidence THEN $props.confidence
                ELSE r.confidence END,
                r.updated_at = datetime()
        """, {"from_key": from_key, "to_key": to_key, "props": props})

    def _create_family_edge(self, session, member: dict, pol: dict, confidence: float):
        session.run("""
            MATCH (i:Individual {din: $din})
            MATCH (p:Politician {pan: $pan})
            MERGE (i)-[r:FAMILY_OF]->(p)
            ON CREATE SET r.relation = $relation, r.confidence = $confidence,
                          r.created_at = datetime()
        """, {
            "din": member.get("din") or f"UNKNOWN_{member['name_normalized'][:20]}",
            "pan": pol.get("pan") or f"UNKNOWN_{pol['id']}",
            "relation": member.get("relation", "associate"),
            "confidence": confidence,
        })

    def _create_individual_company_edge(self, session, person: dict,
                                         company: dict, confidence: float):
        session.run("""
            MATCH (i:Individual {din: $din})
            MATCH (c:Company {cin: $cin})
            MERGE (i)-[r:IS_DIRECTOR]->(c)
            ON CREATE SET r.confidence = $confidence, r.created_at = datetime()
            ON MATCH SET r.confidence = CASE
                WHEN $confidence > r.confidence THEN $confidence
                ELSE r.confidence END
        """, {
            "din": person.get("din") or f"UNKNOWN_{person['name_normalized'][:20]}",
            "cin": company["cin"],
            "confidence": confidence,
        })

    # ── Cross-links ───────────────────────────────────────────────────────────

    def _compute_shared_director_links(self):
        """
        Compute SHARES_DIRECTOR_WITH relationships between companies.
        Two companies share a director if the same Individual IS_DIRECTOR of both.
        This detects shell company networks.
        """
        logger.info("Computing shared director links...")
        with self.neo4j_driver.session() as session:
            session.run("""
                MATCH (i:Individual)-[:IS_DIRECTOR]->(c1:Company)
                MATCH (i)-[:IS_DIRECTOR]->(c2:Company)
                WHERE c1 <> c2 AND NOT EXISTS((c1)-[:SHARES_DIRECTOR_WITH]-(c2))
                MERGE (c1)-[:SHARES_DIRECTOR_WITH {via: i.name_normalized}]->(c2)
            """)

    # ── PostgreSQL sync ───────────────────────────────────────────────────────

    def _sync_entity_links_to_postgres(self, politician_id: str = None):
        """
        Sync computed entity links from Neo4j back to PostgreSQL entity_links table.
        This allows fast SQL queries without traversing the graph every time.
        """
        logger.info("Syncing entity links to PostgreSQL...")

        filter_clause = "WHERE p.pg_id = $pid" if politician_id else ""
        params = {"pid": politician_id} if politician_id else {}

        with self.neo4j_driver.session() as session:
            result = session.run(f"""
                MATCH (p:Politician)-[r:IS_DIRECTOR|IS_SHAREHOLDER]->(c:Company)
                {filter_clause}
                RETURN p.pg_id AS politician_id, c.cin AS cin,
                       type(r) AS link_type, r.confidence AS confidence,
                       r.match_type AS match_type
                UNION
                MATCH (p:Politician)<-[:FAMILY_OF]-(i:Individual)-[r:IS_DIRECTOR]->(c:Company)
                {filter_clause}
                RETURN p.pg_id AS politician_id, c.cin AS cin,
                       'family' AS link_type, r.confidence AS confidence,
                       'family_director' AS match_type
            """, **params)

            rows = list(result)

        # Batch upsert to PostgreSQL
        with psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                for row in rows:
                    # Look up company UUID from CIN
                    cur.execute("SELECT id FROM companies WHERE cin = %s", (row["cin"],))
                    company_row = cur.fetchone()
                    if not company_row:
                        continue

                    cur.execute("""
                        INSERT INTO entity_links (politician_id, company_id, link_type,
                                                  confidence, evidence_sources)
                        VALUES (%s::uuid, %s, %s, %s, ARRAY['neo4j_graph'])
                        ON CONFLICT (politician_id, company_id) DO UPDATE SET
                            confidence = GREATEST(EXCLUDED.confidence, entity_links.confidence),
                            link_type = EXCLUDED.link_type
                    """, (row["politician_id"], str(company_row["id"]),
                          row["link_type"] or "direct",
                          float(row["confidence"] or 0.5)))

                conn.commit()
                self._stats["entity_links_saved"] = len(rows)
        logger.info(f"  Synced {len(rows)} entity links to PostgreSQL")

    # ── Data loaders ──────────────────────────────────────────────────────────

    def _load_all_politicians(self) -> list[dict]:
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, name_raw, name_normalized, pan, party, state,
                       constituency, election_year, position_held
                FROM politicians ORDER BY state, name_normalized
            """)
            return [dict(r) for r in cur.fetchall()]

    def _load_politician(self, politician_id: str) -> Optional[dict]:
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT * FROM politicians WHERE id = %s", (politician_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_family(self, politician_id: str) -> list[dict]:
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, name_raw, name_normalized, relation, pan
                FROM politician_family WHERE politician_id = %s
            """, (str(politician_id),))
            return [dict(r) for r in cur.fetchall()]

    def _find_companies_by_pan(self, pan: str) -> list[dict]:
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT c.cin, c.name, c.name_normalized, c.company_type, c.status,
                       c.registration_date, c.state_of_reg, c.authorized_capital,
                       cp.role, cp.appointed_date
                FROM companies c
                JOIN company_persons cp ON cp.company_id = c.id
                WHERE cp.pan = %s
            """, (pan,))
            return [dict(r) for r in cur.fetchall()]

    def _load_directors_by_state(self, state: str) -> list[dict]:
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT cp.id, cp.company_id, cp.din, cp.pan,
                       cp.name_raw, cp.name_normalized, cp.role
                FROM company_persons cp
                JOIN companies c ON c.id = cp.company_id
                WHERE c.state_of_reg = %s AND cp.is_active = TRUE
                LIMIT 10000
            """, (state,))
            return [dict(r) for r in cur.fetchall()]

    def _load_company(self, company_id: str) -> Optional[dict]:
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT * FROM companies WHERE id = %s", (str(company_id),))
            row = cur.fetchone()
            return dict(row) if row else None

    def close(self):
        self.pg_conn.close()
        self.neo4j_driver.close()


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Entity Graph Builder")
    parser.add_argument("--full-rebuild", action="store_true",
                        help="Full graph rebuild (default: incremental)")
    parser.add_argument("--politician-id", help="Process single politician UUID")
    args = parser.parse_args()

    builder = EntityGraphBuilder()
    try:
        if args.politician_id:
            builder.update_for_politician(args.politician_id)
        else:
            builder.build_full_graph()
    finally:
        builder.close()
